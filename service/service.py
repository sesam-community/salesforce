from functools import wraps
from flask import Flask, request, Response, abort
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import os

import json
from simple_salesforce import Salesforce, SalesforceError, SalesforceResourceNotFound
from sesamutils import sesam_logger
from sesamutils.flask import serve

app = Flask(__name__)

logger = sesam_logger("salesforce", app=app)

SF_OBJECTS_CONFIG = json.loads(os.environ.get("SF_OBJECTS_CONFIG","{}"))
VALUESET_LIST = json.loads(os.environ.get("VALUESET_LIST","{}"))
API_VERSION = os.environ.get("API_VERSION","52.0")
DEFAULT_BULK_SWITCH_THRESHOLD = int(os.environ.get("DEFAULT_BULK_SWITCH_THRESHOLD", 0))
salesforce_service_refreshed_at_interval = int(os.environ.get("SALESFORCE_SERVICE_REFRESHED_AT_INTERVAL", 45))
salesforce_service = None
salesforce_service_refreshed_at = None

def datetime_format(dt):
    return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%SZ")

def to_transit_datetime(dt):
    return "~t" + datetime_format(dt)

def get_var(var, scope=None, is_required=False):
    envvar = None
    if (scope is None or scope=="REQUEST") and var in request.args:
        envvar = request.args.get(var)
    elif (scope is None or scope=="ENV") and var.upper() in os.environ:
        envvar = os.environ.get(var.upper())
    if is_required and envvar is None:
        abort(400, "cannot read required '%s' from request params or envvars" % (var.upper()))
    return envvar

class DataAccess:
    def __init__(self):
        self._sobject_fields = {}

    def sesamify(self, entity, datatype=None):
        entity.update({"_id": entity.get("Id")})

        for property, value in entity.items():
            schema = [item for item in self._sobject_fields.get(datatype, []) if item.get("name") == property]
            if value and len(schema) > 0 and "type" in schema[0] and schema[0]["type"] == "datetime":
                if isinstance(value, int):
                    entity[property] = to_transit_datetime(datetime.fromtimestamp(value/1000))
                elif isinstance(value, str):
                    entity[property] = to_transit_datetime(parse(value))

        entity.update({"_updated": "%s" % entity.get("SystemModstamp").replace("~t","")})
        entity.update({"_deleted": entity.get("IsDeleted")})
        return entity


    def get_entities(self, sf, datatype, filters=None, objectkey=None):
        yield '['
        if self._sobject_fields.get(datatype, []) == []:
            try:
                fields = [dict(f) for f in getattr(sf, datatype).describe()["fields"]]
            except SalesforceResourceNotFound as e:
                yield str(e)
                abort(404)
                return
            self._sobject_fields[datatype] = fields
        try:
            yield from self.get_entitiesdata(sf, datatype, filters, objectkey)
            yield ']'
        except SalesforceResourceNotFound as e:
            yield str(e)
            abort(404)

    def get_entitiesdata(self, sf, datatype, filters=None, objectkey=None):
        isFirst = True
        if objectkey:
            obj = getattr(sf, datatype).get(objectkey)
            yield json.dumps(self.sesamify(obj, datatype))
        else:
            select_clause = ",".join([f["name"] for f in self._sobject_fields[datatype]])
            conditions = []
            if filters.get("since"):
                sinceDateTimeStr = parse(filters.get("since")).isoformat()
                conditions.append(f"SystemModstamp>{sinceDateTimeStr}")
            if filters.get("where"):
                conditions.append(filters.get("where"))
            where_clause = "where {}".format(" AND ".join(conditions)) if conditions else ""

            query = f"select {select_clause} from {datatype} {where_clause} order by SystemModstamp"
            logger.debug(f"query:{query}")
            result = sf.query_all_iter(query, include_deleted=True)
            if result:
                for row in result:
                    if not isFirst:
                        yield ',\n'
                    else:
                        isFirst = False
                    yield json.dumps(self.sesamify(row, datatype))
        return

data_access_layer = DataAccess()

def transform(datatype, entities, sf, operation_in="POST", objectkey_in=None):
    def _get_unsesamified_object(entity):
        d = []
        for p in entity.keys():
            if p.startswith("_"):
                d.append(p)
        for p in d:
            del(entity[p])

        return entity

    def _get_object_key(entity, objectkey_in=None):
        '''if 'Id' is specified, use 'Id' as key,
            else pick the first external id field that has a value'''
        key_field = "Id"
        key = None
        if entity.get(key_field):
            key = entity[key_field]
        elif datatype in SF_OBJECTS_CONFIG:
            for k in SF_OBJECTS_CONFIG[datatype]["ordered_key_fields"]:
                if entity.get(k):
                    key_field = k
                    key = f"{key_field}/{entity[key_field]}"
                    break

        key = key or objectkey_in
        if not key:
            abort(500,"cannot figure out the objectkey for %s" % (entity))
        #remove fields starting with '_'
        entity = _get_unsesamified_object(entity)
        if "Id" in entity:
            del(entity["Id"])
        if key_field in entity:
            del(entity[key_field])

        return entity, key

    global ids
    c = None
    listing = []
    if not isinstance(entities, list):
        listing.append(entities)
    else:
        listing = entities

    doBulk = False
    if DEFAULT_BULK_SWITCH_THRESHOLD > 0:
        bulk_switch_threshold = DEFAULT_BULK_SWITCH_THRESHOLD
        if datatype in SF_OBJECTS_CONFIG:
            bulk_switch_threshold = SF_OBJECTS_CONFIG[datatype].get("bulk_switch_threshold", DEFAULT_BULK_SWITCH_THRESHOLD)
        doBulk = bulk_switch_threshold < len(listing)

    if doBulk:
        deleteListPerExternalId = {}
        upsertListPerExternalId = {}
        singleDeleteListPerExternalId = {}
        for k in SF_OBJECTS_CONFIG[datatype]["ordered_key_fields"] + ["Id"]:
            deleteListPerExternalId[k] = []
            upsertListPerExternalId[k] = []
            singleDeleteListPerExternalId[k] = []
        for e in listing:
            externalIdField = "Id"
            for k in SF_OBJECTS_CONFIG[datatype]["ordered_key_fields"]:
                if e.get(k):
                    externalIdField = k
            if operation_in == "DELETE" or e.get("_deleted", False):
                if e.get("Id"):
                    deleteListPerExternalId[externalIdField].append(_get_unsesamified_object(e))
                else:
                    singleDeleteListPerExternalId[externalIdField].append(f"{e[externalIdField]}")
            else:
                upsertListPerExternalId[externalIdField].append(_get_unsesamified_object(e))

        for k,v in ({"deleteListPerExternalId":deleteListPerExternalId,
                    "upsertListPerExternalId":upsertListPerExternalId,
                    "singleDeleteListPerExternalId": singleDeleteListPerExternalId}).items():
            for vk in v.keys():
                logger.debug(f"length of {k}({vk}) = {str(len(v.get(vk)))}")
        for externalId in deleteListPerExternalId.keys():
            if deleteListPerExternalId.get(externalId):
                bulkResult1 = getattr(sf.bulk, datatype).delete(deleteListPerExternalId.get(externalId), batch_size=10000, use_serial=True)

        for externalId in upsertListPerExternalId.keys():
            if upsertListPerExternalId.get(externalId):
                bulkResult2 = getattr(sf.bulk, datatype).upsert(upsertListPerExternalId.get(externalId), externalId, batch_size=10000, use_serial=True)

        for externalId in singleDeleteListPerExternalId.keys():
            if singleDeleteListPerExternalId.get(externalId):
                for objectkey in singleDeleteListPerExternalId.get(externalId):
                    logger.debug(f"deleting {externalId}/{objectkey}")
                    try:
                        getattr(sf, datatype).delete(f"{externalId}/{objectkey}")
                    except SalesforceResourceNotFound as err:
                        None
                    except Exception as err:
                        logger.debug(f"{datatype}/{externalId}/{objectkey} received exception of type {type(err).__name__}")
    else:
        for e in listing:
            operation = "DELETE" if operation_in == "DELETE" or e.get("_deleted", False) else operation_in
            object, objectkey = _get_object_key(e, objectkey_in)

            logger.debug(f"performing {operation} on {datatype}/{objectkey}")
            if operation == "DELETE":
                try:
                    getattr(sf, datatype).delete(objectkey)
                except SalesforceResourceNotFound as err:
                        None
                except Exception as err:
                    logger.debug(f"{datatype}/{objectkey} received exception of type {type(err).__name__}")
            else:
                getattr(sf, datatype).upsert(objectkey, object)

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth and (
            not(get_var("USERNAME", "ENV") and get_var("PASSWORD", "ENV") and get_var("SECURITY_TOKEN", "ENV"))
            and not get_var("LOGIN_CONFIG", "ENV")):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


def _refresh_sf():
    if request.authorization:
        auth = request.authorization
    elif get_var("LOGIN_CONFIG", "ENV"):
        login_config = json.loads(get_var("LOGIN_CONFIG", "ENV"))
        auth =  {
            "username": login_config["SECURITY_TOKEN"] + "\\" + login_config["USERNAME"],
            "password": login_config["PASSWORD"]
            }
    else:
        auth =  {
            "username": get_var("SECURITY_TOKEN", "ENV") + "\\" + get_var("USERNAME", "ENV"),
            "password": get_var("PASSWORD", "ENV")
            }
    token, username = auth['username'].split("\\", 1)
    password = auth['password']

    instance = get_var('instance') or "prod"
    if instance == "sandbox":
        sf = Salesforce(username, password, token, domain='test', version=API_VERSION)
    else:
        sf = Salesforce(username, password, token, version=API_VERSION)
    return sf

def get_sf():
    global salesforce_service
    global salesforce_service_refreshed_at
    do_relogin = not salesforce_service
    if salesforce_service_refreshed_at:
        delta = datetime.now(timezone.utc) - salesforce_service_refreshed_at
        do_relogin = do_relogin or delta.seconds//60 >= salesforce_service_refreshed_at_interval
    if do_relogin:
        salesforce_service = _refresh_sf()
        salesforce_service_refreshed_at = datetime.now(timezone.utc)
    logger.debug(f"do_relogin={do_relogin}, salesforce_service_refreshed_at={salesforce_service_refreshed_at}")
    return salesforce_service


def get_path_for_valueset(req):
    path_prefix_for_alias = "/ValueSet/SesamAlias/"
    if req.path.startswith(path_prefix_for_alias):
        alias = request.path.replace(path_prefix_for_alias,"")
        path = VALUESET_LIST.get(alias)
        if not path:
            abort(500, "cannot map alias to SF id")
        return path
    else:
        return request.path.replace("/ValueSet", "")

@app.route('/ValueSet', methods=["GET"], endpoint="get_valueset_all")
@app.route('/ValueSet/', methods=["GET"], endpoint="get_valueset_all/")
@app.route('/ValueSet/CustomField/<sf_id_or_alias>', methods=["GET"], endpoint="get_custom_valueset_by_id")
@app.route('/ValueSet/GlobalValueSet/<sf_id_or_alias>', methods=["GET"], endpoint="get_global_valueset_by_id")
@app.route('/ValueSet/SesamAlias/<sf_id_or_alias>', methods=["GET"], endpoint="get_valueset_by_alias")
@requires_auth
def valueset_execute(sf_id_or_alias=None):
    try:
        sf = get_sf()

        path = get_path_for_valueset(request)
        do_refine = request.args.get("do_refine", "1").lower() not in ["0", "false", "no"]

        if request.endpoint.startswith("get_valueset_all"):
            input_list = VALUESET_LIST.values()
        else:
            input_list = [path]
        output_list = []
        for vs in input_list:
            tooling_api_response = sf.toolingexecute(
                f"sobjects{vs}",
                method=request.method)
            response_data = {"path":vs,
                "_id": vs}
            if do_refine:
                metadata = tooling_api_response.get("Metadata",{})
                if request.endpoint == "get_global_valueset_by_id" or vs.startswith("/GlobalValueSet/"):
                    response_data["data"] = metadata.get("customValue")
                elif request.endpoint == "get_custom_valueset_by_id" or vs.startswith("/CustomField/"):
                    response_data["data"] = metadata.get("valueSet",{}).get("valueSetDefinition",{}).get("value",[])
            else:
                response_data["data"] = tooling_api_response
            output_list.append(response_data)
        return Response(json.dumps(output_list), mimetype='application/json', status=200)

    except SalesforceError as err:
        logger.exception(err)
        return Response(json.dumps({"resource_name": err.resource_name, "content": err.content, "url": err.url}),
            mimetype='application/json',
            status=err.status)
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)

@app.route('/ValueSet', methods=["POST"], endpoint="valueset_by_path_field")
@app.route('/ValueSet/', methods=["POST"], endpoint="valueset_by_path_field/")
@app.route('/ValueSet/CustomField/<sf_id_or_alias>', methods=["POST"], endpoint="custom_valueset_by_id")
@app.route('/ValueSet/GlobalValueSet/<sf_id_or_alias>', methods=["POST"], endpoint="global_valueset_by_id")
@app.route('/ValueSet/SesamAlias/<sf_id_or_alias>', methods=["POST"], endpoint="valueset_by_alias")
@requires_auth
def valueset_execute_non_get(sf_id_or_alias=None):
    READONLY_FIELDS = ["Id",
        "DeveloperName",
        "MasterLabel",
        "Description",
        "NamespacePrefix",
        "ManageableState",
        "CreatedDate",
        "CreatedById",
        "LastModifiedDate",
        "LastModifiedById"]
    def _updatable(d):
        for key in list(set(READONLY_FIELDS) & set(d.keys())):
            del d[key]
        return d

    try:
        sf = get_sf()

        do_read_path_from_data = request.endpoint.startswith("valueset_by_path_field")
        path = None if do_read_path_from_data else get_path_for_valueset(request)

        data = request.get_json()
        data = data if isinstance(data, list) else [data]

        for vs in data:
            if do_read_path_from_data:
                path = vs["path"]

            pre_patch_data = sf.toolingexecute(
                f"sobjects{path}",
                method="GET")

            patch_data = pre_patch_data
            if request.endpoint == "global_valueset_by_id" or path.startswith("/GlobalValueSet/"):
                patch_data["Metadata"]["customValue"] = vs["data"]
            elif request.endpoint == "custom_valueset_by_id" or path.startswith("/CustomField/"):
                patch_data_temp = {}
                patch_data_temp["Metadata"] = pre_patch_data["Metadata"]
                patch_data_temp["Metadata"]["valueSet"]["valueSetDefinition"]["value"] = vs["data"]
                if not patch_data_temp["Metadata"]["valueSet"]["valueSettings"]:
                    patch_data_temp["Metadata"]["valueSet"]["valueSettings"] = []
                patch_data = patch_data_temp
            tooling_api_response = sf.toolingexecute(
                f"sobjects{path}",
                method="PATCH",
                data=_updatable(patch_data))
        return Response("", mimetype='application/json', status=200)

    except SalesforceError as err:
        logger.exception(err)
        return Response(json.dumps({"resource_name": err.resource_name, "content": err.content, "url": err.url}),
            mimetype='application/json',
            status=err.status)
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)


@app.route('/sf/tooling/<path:path>', methods=["GET", "POST", "DELETE", "PATCH", "PUT"], endpoint="tooling_execute")
@requires_auth
def tooling_execute(path):
    try:
        sf = get_sf()
        data = request.get_json()
        response_json = sf.toolingexecute(
            path,
            method=request.method,
            data=data)
        if request.method == "GET":
            return Response(json.dumps(data_access_layer.sesamify(response_json)), mimetype='application/json')
        else:
            return Response(json.dumps(response_json), mimetype='application/json')
    except SalesforceError as err:
        logger.exception(err)
        return Response(json.dumps({"resource_name": err.resource_name, "content": err.content, "url": err.url}),
            mimetype='application/json',
            status=err.status)
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)

@app.route('/sf/rest/<path:path>', methods=['GET', 'POST', 'DELETE', 'PATCH'], endpoint="restful")
@requires_auth
def restful(path=None):
    try:
        sf = get_sf()
        response_data = sf.restful(path, request.args, request.method, json=request.get_json())
        return Response(json.dumps(response_data), mimetype='application/json')
    except SalesforceError as err:
        return Response(json.dumps({"resource_name": err.resource_name, "content": err.content, "url": err.url}),
            mimetype='application/json',
            status=err.status)
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)


@app.route('/<datatype>', methods=['GET'], endpoint="get_all")
@app.route('/<datatype>/<objectkey>', methods=['GET'], endpoint="get_by_id")
@app.route('/<datatype>/<objectkey>', methods=['GET'], endpoint="get_by_id")
@app.route('/<datatype>/<ext_id_field>/<ext_id>', methods=['GET'], endpoint="get_by_ext_id")
@requires_auth
def get_entities(datatype, objectkey=None, ext_id_field=None, ext_id=None):
    try:
        sf = get_sf()
        filters = {k: v for k, v in request.args.items() if k in["since","where"]}
        if request.endpoint == "get_by_ext_id":
            objectkey = f"{ext_id_field}/{ext_id}"
        entities = data_access_layer.get_entities(sf, datatype, filters, objectkey)
        return Response(response=entities, mimetype='application/json')
    except SalesforceError as err:
        return Response(json.dumps({"resource_name": err.resource_name, "content": err.content, "url": err.url}),
            mimetype='application/json',
            status=err.status)
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)

@app.route('/<datatype>', methods=["POST", "PUT", "PATCH", "DELETE"], endpoint = "crud_all")
@app.route('/<datatype>/<objectkey>', methods=['POST', "PUT", "PATCH", "DELETE"], endpoint="crud_by_id")
@app.route('/<datatype>/<ext_id_field>/<ext_id>', methods=["POST", "PUT", "PATCH", "DELETE"], endpoint="crud_by_ext_id")
@requires_auth
def receiver(datatype, objectkey=None, ext_id_field=None, ext_id=None):
    try:
        entities = request.get_json()
        sf = get_sf()
        if request.endpoint == "crud_by_ext_id":
            objectkey = f"{ext_id_field}/{ext_id}"
        if getattr(sf, datatype):
            transform(datatype, entities, sf, operation_in=request.method, objectkey_in=objectkey)
        return Response("", mimetype='application/json')
    except SalesforceError as err:
        logger.exception(err)
        return Response(json.dumps({"resource_name": err.resource_name, "content": err.content, "url": err.url}),
            mimetype='application/json',
            status=err.status)
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)

if __name__ == '__main__':
    PORT = int(get_var('PORT', "ENV") or 5000)
    username = json.loads(get_var("LOGIN_CONFIG", "ENV")).get("USERNAME") if get_var("LOGIN_CONFIG", "ENV") else get_var("USERNAME", "ENV")
    logger.info(f"Starting opp with USERNAME={username}")

    if get_var("WEBFRAMEWORK", "ENV") == "FLASK":
        app.run(debug=True, host='0.0.0.0', port=PORT)
    else:
        serve(app, port=PORT)

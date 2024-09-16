
# salesforce
Sesam-Salesforce connector that can be used to:
  * get/delete/upsert objects
  * get/delete/upsert valuesets(a.k.a. picklist)
  * perform tooling API operations

[![SesamCommunity CI&CD](https://github.com/sesam-community/salesforce/actions/workflows/sesam-community-ci-cd.yml/badge.svg)](https://github.com/sesam-community/salesforce/actions/workflows/sesam-community-ci-cd.yml)
## ENV VARIABLES

<table><tr><th>CONFIG_NAME        </th><th> DESCRIPTION           </th><th> IS_REQUIRED  </th><th>DEFAULT_VALUE</th></tr>
<tr><td> SF_OBJECTS_CONFIG </td><td> dict for object level customizations. see schemas section for description. </td><td> no </td><td> n/a </td></tr>
<tr><td> LOGIN_CONFIG </td><td> a dict either with keys<br> - <em>INSTANCE, CLIENT_ID, CLIENT_SECRET</em> <br>
Fex  
	
```json
{
  "CLIENT_ID": "<MY_CLIENT_ID>",
  "CLIENT_SECRET": "<MY_CLIENT_SECRET>",
  "INSTANCE": "myinstance.sandbox.my.salesforce.com"
}
```
, or <br><br> - _USERNAME, PASSWORD, SECURITY_TOKEN, and optionally DOMAIN_(defaults to 'Login' if not set),<br>
Fex
			
```json 
{
  "USERNAME": "myusename",
  "PASSWORD": "mypassword",
  "SECURITY_TOKEN": "mysecuritytoken",
  "DOMAIN":"myinstance.sandbox.my"
}
```

</td><td> yes </td><td> n/a </td><tr>

<tr><td> WEBFRAMEWORK </td><td> set to 'FLASK' to use flask, otherwise it will run on cherrypy </td><td> no </td><td> n/a </td></tr>
<tr><td> LOG_LEVEL </td><td> LOG_LEVEL. one of [CRITICAL\|ERROR\|WARNING\|INFO\|DEBUG] </td><td> no </td><td> 'INFO' </td></tr>
<tr><td> VALUESET_LIST </td><td> a dict where keys are the aliases to be used in sesam and values are the paths to the corresponding valueset. Used when fetching all valusets and for patching. 
	<br>Fex
	
```json
{
  "my_global_vs": "GlobalValueSet/0Nt5I0000008cw7SAA", 
  "my_custom_vs": "CustomField/00N5I000004yDnkUAE"
}
```
 
 </td><td> no </td><td> n/a </td></tr>
<tr><td> DEFAULT_BULK_SWITCH_THRESHOLD </td><td> Integer. Threshold value on the number of incoming entities to swith to bulk-api instead of rest-api.Disabled if not set. </td><td> no </td><td> None </td></tr>
</table>

## ENDPOINTS

 1. `/<datatype>`, methods=["GET", "POST", "PUT", "PATCH", "DELETE"]

    By default _Id_ is used to match target object. If _Id_ is not available to Sesam, the _SF_OBJECTS_CONFIG_ envvar can be configured for alternative match keys.

    * "GET": returns all data(upserted and deleted) of type _datatype_. Response is streamed, thus the response will give 200 status but malformed body when error is encountered._Id_ and _SystemModstamp_ is set as _\_id_ and _\_updated_, respectively.
    * "POST", "PUT", "PATCH": upserts objects or deletes if _\_deleted_ is true. Accepts dict or list of dicts.
    * "DELETE": deletes incoming objects.

    #### query params
    * `since`: Optional. Data updated after _since_ value will be delivered. CAnnot be older then 30 days ago due to Salesforce REST API limitations.
    * `where`: Optional. Applicable to GET method condition that will be appended to SOQL select query.
    * `extra_attributes`: Optional. CSV of extra attributes to fetch. Fex, 'Createdby.name'. N.B. Transit encoding of datetime fields is not supported on these attributes.
    * `do_create_if_key_is_empty`: Optional. Applicable to POST/PUT/PATCH requests. Allows creation of object when the objectkey cannot be determined.
    
___

 2. `/<datatype>/<ext_id_field>/<ext_id>`, methods=["GET", "POST", "PUT", "PATCH", "DELETE"]

    Same as point 1, but here the the the objectkey(externalkey here) can additionally be read from the url.

___

3. `/<datatype>/<objectkey>`, methods=["GET", "POST", "PUT", "PATCH", "DELETE"]

    Same as point 2, but here the the the objectkey(genuine objectkey/Id) can additionally be read from the url.
___
 4. `/ValueSet`, methods=["GET","POST"]

    * "GET": returns all valuesets that are specified in _VALUESET\_LIST_ envvar
    * "POST": Upserts values to valuesets. See _ValueSet_ below in schemas section for description of payload.

    Note that a value is disabled via _isActive_ flag in the json.

    #### query params
     * `do_refine`: Optional. If equals to one of _"0", "false", "no"_ case-insensitively, the original payload will be returned in _data_ field of the response.
       Otherwise, only the valueset section.
___
 5. `/ValueSet/`, methods=["GET", "POST"]

    Same as 4. (Sesam required the trailing slach for some reason.)
___
 6. `/ValueSet/CustomField/<sf_id>`, methods=["GET", "POST"]

	  Same as 4, but for single valueset that is customfield.
___
 7. `/ValueSet/GlobalValueSet/<sf_id>`, methods=["GET", "POST"]

    Same as 6, but for single valueset that is global valueset.
___
 8. `/ValueSet/SesamAlias/<alias_in_VALUSET_LIST_envvar>`, methods=["GET", "POST"]

    Same as 6, but for single valueset that is global valueset.
___
 9. `/sf/tooling/<path:path>`, methods=["GET", "POST", "DELETE", "PATCH", "PUT"]

    This is endpoint that makes available the [Salesforce tooling API](https://developer.salesforce.com/docs/atlas.en-us.api_tooling.meta/api_tooling/intro_api_tooling.htm).

    #### query params     
     * `preserve_as_list`: Optional.  Applicable to /sf/tooling/<path:path> requests. If sent as _true_ or _1_ and the payload is a list, the first element in the list will be sent forward, othwersie payload is sent forward as it comes in. 
______
 10. `/services/restful/<path:path>` or `/sf/rest/<path:path>`, methods=["GET", "POST", "DELETE", "PATCH", "PUT"]

    This is endpoint that makes available any REST call to Salesforce RestAPI. If the original endpoint is `services/data/v{{version}}/mypath1/mypath2/mypath3`, the path in this endpoint should be `/mypath1/mypath2/mypath3`. Query params are passed over.
    
    #### query params     
     * `preserve_as_list`: Optional.  Applicable to /sf/tooling/<path:path> requests. If sent as _true_ or _1_ and the payload is a list, the first element in the list will be sent forward, othwersie payload is sent forward as it comes in. 
______
 11. `/services/apexrest/<path:path>`, methods=["GET", "POST", "DELETE", "PATCH", "PUT"]

    This is endpoint that makes available the [apexrest webservices](https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_rest.htm). If the original endpoint is `services/apexrest/mypath1/mypath2/mypath3`, the path in this endpoint should be `/mypath1/mypath2/mypath3`. Query params are passed over.
    
    #### query params     
     * `preserve_as_list`: Optional.  Applicable to /sf/tooling/<path:path> requests. If sent as _true_ or _1_ and the payload is a list, the first element in the list will be sent forward, othwersie payload is sent forward as it comes in. 
___

## Schema Examples

 * SF_OBJECTS_CONFIG is a dict where keysa are sobject names that to be customized. Value is a dict for different customizations available:
    * _ordered_key_fields_: a ordered list of strings. Effective when setting _\_id_ value and _Id_ is not available. The first field that reveals a non-null value will be used to ser _\_id_.
```
{
        "aadgroup__c": {
            "ordered_key_fields": [
                "sesam_ext_id__c",
                "some_ext_id__c"
            ]
        },
        "Product2":{
            "ordered_key_fields": [
                "sesam_ext_id__c",
                "some_ext_id__c"
            ]
        }
    }
```
 * VALUSET_LIST:
 ```
    {
        "alias1": "/GlobalValueSet/0Nt5I0000008cw7SAA",
        "alias2": "/CustomField/00N5I000004yDnkUAE"
    }
 ```

 * ValueSet:
```
[
	{
		"data": [
			{
				"color": null,
				"default": false,
				"description": null,
				"isActive": true,
				"label": "mylabel",
				"urls": null,
				"valueName": "myvalue"
			}
		]
	}
]
```

Example configs:

### system:
```
{
  "_id": "salesforce",
  "type": "system:microservice",
  "metadata": {
    "tags": ["salesforce"]
  },
  "connect_timeout": 60,
  "docker": {
    "environment": {
      "DEFAULT_BULK_SWITCH_THRESHOLD": 999,
      "INSTANCE": "sandbox",
      "LOGIN_CONFIG": "$SECRET(salesforce_login_config)",
      "LOG_LEVEL": "DEBUG",
      "SF_OBJECTS_CONFIG": {
        "Account": {
          "ordered_key_fields": ["myExternalIdFieldForAccount1", "myExternalIdFieldForAccount2", "myExternalIdFieldForAccount3"]
        },
        "Case": {
          "ordered_key_fields": ["myExternalIdFieldFroCaseObject1"]
        }
      }
    },
    "image": "sesamcommunity/salesforce:2.0.0",
    "memory": 8192,
    "port": 5000
  },
  "read_timeout": 7200
}
```

### Input pipe
```
...
...
...

      "source": {
        "type": "json",
        "system": "salesforce",
        "is_chronological": false,
        "is_since_comparable": true,
        "supports_since": true,
        "url": "/account?extra_attributes=CreatedBy.name&where=IsPersonAccount=false and PersonEmail='myuser@email.com'"
      }
...
...
...
```

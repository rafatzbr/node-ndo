NDO4H - Node Data Objects for Hana
============
Promisify & Objectify as camelCase hana queries

#Usage

## Initialization

* Make sure you imported and initialized \@sap/hdbext library as described in https://www.npmjs.com/package/@sap/hdbext

* Import the library
```javascript
const {ndo4h} = require('ndo4h');
```

* Initialize it, passing the @sap/hdbext library
```javascript
const db = ndo4h(req.db);
```

## Executing queries

```javascript
db.execute(query, parameters);
```

where 

* query is a string
* parameters is an array

and it returns an array of objects with all field names as properties in camelCase, if they were in snake_case in the database

### Examples
* Queries without parameters

```javascript
const users = await db.execute("SELECT * FROM USERS");
```

Example return:
```json
[
    {
        "id": 1,
        "firstName": 'Rafael',
        "lastName": 'Zanetti',
        "email": 'something@else.com'
    }
]
```

* Query With parameters

```javascript
const user = await db.execute("SELECT * FROM USERS WHERE EMAIL = ?", [req.params.email]);
```



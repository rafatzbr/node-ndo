ar4h
============

This module provides a very simple Active Record implementation in NodeJS for SAP HANA DB

The [change log](CHANGELOG.md) describes notable changes in this package.

## Usage

```js
const db = require('ar4h').ar4h();

const get = () => {
    db.find('users', ['name', 'email'], {user: 'test'})
        .then((user) => {
            console.log(user);
        });
}
```
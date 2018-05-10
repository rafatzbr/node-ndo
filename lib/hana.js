'use strict';

var hdb = require("@sap/hdbext");
var xsenv = require("@sap/xsenv");

module.exports = class HANA {
    constructor (conn) {
        this.conn = conn;
    }

    execute (sql, values, done) {
        var op = sql.split(' ').shift().toUpperCase();
        this.conn.prepare(sql, (err, pstmt) => {
            if (err) return reject(err);

            pstmt.exec(values, (err, rs) => done(err, rs));
        });
    }

    getLastId(table, done) {
        if (table === undefined) table = this.table;

        const sql = `SELECT TO_INTEGER(CURRENT_IDENTITY_VALUE()) as "id" FROM ${table}`;
        this.conn.exec(sql, (err, rs) => done(err, rs));
    }

     /**
     * Commit a transaction
     */
    commit () {
        this.conn.commit();
    }

    /**
     * Rollback a transaction
     */
    rollback () {
        this.conn.rollback();
    }
}
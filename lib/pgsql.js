'use strict';

var pg = require('pg');

module.exports = class PGSQL {
    constructor (conn) {
        this.conn = conn;
    }

    execute (sql, values, done) {
        var pos = sql.indexOf('?'),
            indx = 1;
        while (pos >= 0) {
            sql = sql.substring(0, pos - 1) + '$' + indx + sql.substring(pos + 1, sql.length);
            pos = sql.indexOf('?');
            indx++;
        }

        this.conn.query(sql, values, (err, rs) => done(err, rs.rows));
    }

    getLastId(table, done) {
        this.conn.query(`SELECT currval(pg_get_serial_sequence('${table}','id'))`, (err, rs) => done(err, rs));
    }

     /**
     * Commit a transaction
     */
    commit (cb) {
        if (cb === undefined) {
            cb = () => {};
        }

        this.conn.query("COMMIT", () => cb);
    }

    /**
     * Rollback a transaction
     */
    rollback (bc) {
        if (cb === undefined) {
            cb = () => {};
        }
        
        this.conn.query("COMMIT", () => cb);
    }
}
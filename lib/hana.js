'use strict';

module.exports = class HANA {
    constructor (conn) {
        this.conn = conn;
    }

    execute (sql, values, done) {
        var op = sql.split(' ').shift().toUpperCase();
        this.conn.prepare(sql, (err, pstmt) => {
            if (err) return done(err);

            pstmt.exec(values, (err, rs) => done(err, rs));
        });
    }

    getLastId(table, done) {
        if (table === undefined) table = this.table;

        const sql = `SELECT TO_BIGINT(CURRENT_IDENTITY_VALUE()) as "id" FROM dummy`;
        this.conn.exec(sql, (err, rs) => {
            if (err) {
                err.sql = sql;
                return done(err);
            }
            
            done(null, rs);
        });
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
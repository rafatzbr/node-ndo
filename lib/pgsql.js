'use strict';

module.exports = class PGSQL {
    constructor (conn) {
        this.conn = conn;
    }

    execute (sql, values, done) {
        let pos = sql.indexOf('?'),
            indx = 1;
        while (pos >= 0) {
            sql = sql.substring(0, pos - 1) + '$' + indx + sql.substring(pos + 1, sql.length);
            pos = sql.indexOf('?');
            indx++;
        }

        if (sql.toUpperCase().indexOf('ORDER BY') >= 0) {
            pos = sql.toLowerCase().indexOf('order by') + 8;
            let order = sql.substring(pos);
            let fields = order.split(',').join(' NULLS FIRST, ');
            sql = sql.substring(0, pos) + fields + ' NULLS FIRST';
        }
        
        this.conn.query(sql, values, (err, rs) => {
            if (err) return done(err);
            return done(null, rs.rows);
        });
    }

    getLastId(table, done) {
        this.conn.query(`SELECT currval(pg_get_serial_sequence('${table}','id'))`, (err, rs) => {
            if (err) return done(err);
            done(null, rs.rows);
        });
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
        
        this.conn.query("ROLLBACK", () => cb);
    }
}
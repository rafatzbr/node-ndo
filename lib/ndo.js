'use strict';
const _ = require('lodash');
const HANA = require('./hana');

/**
 * Node Data Objects
 */
class NDO {
    /**
     * Initialize some internal variables
     */
    constructor (db) {
        this.db = db;
        this.values = [];
        this.allFields = false;
        this.table = '';
    }

    /**
     * Commit a transaction
     */
    commit () {
        this.db.commit();
    }

    /**
     * Rollback a transaction
     */
    rollback () {
        this.db.rollback();
    }

    /**
     * Run a query against table
     * @returns {Promise}
     * @param {String} table Name of the table to search. You can use tableName|Alias
     * @param {Mixed} fields Array with the name of the fields or * for all fields. You can use tableAlias.fieldName
     * @param {Object} where Array of object conditions in the format {field: value} or an object with the connectors, like and: {}
     * @param {Object} joins Object or array of joins in the format {table, alias, where: []}
     * @param {Array} order List of orderby objects in format {field, direction}
     * @param {Integer} limit Number of tuples to return
     * @param {Array} groupBy List of fields to group by
     */
    find (table, fields, where, joins, order, limit, groupBy, offset) {
        this.values = [];
        this.allFields = false;
        var sql = this._buildSelect(table, fields, where, joins, order, limit, groupBy, offset);

        return this.execute(sql);
    }

    /**
     * Shortcut to find, to return only one row
     * @param {String} table Name of the table to search. You can use tableName|Alias
     * @param {Array} fields Array with the name of the fields or * for all fields. You can use tableAlias.fieldName
     * @param {Object} where Array of object conditions in the format {field: value} or an object with the connectors, like {and: {}}
     */
    async findOne (table, fields, where, joins, order, groupBy) {
        this.values = [];
        this.allFields = false;
        
        var rs = await this.find(table, fields, where, joins, order, 1, groupBy);
        if (rs.length > 0) {
            return rs.pop();
        }

        return [];
    }

    /**
     * Insert values into table
     * @param {String} table Name of the table to insert. 
     * @param {Mixed} values Array of value objects, in the format {field: value}. In case you want to use a SQL function, use {field: {function: functionName}}
     */
    insert (table, values) {
        this.values = [];
        if (table === undefined || !table) {
            throw new Error('Invalid table');
        }
        this.table = table;

        var sql = this._buildInsert(table, values);
        
        return this.execute(sql);
    }

    /**
     * Insert values into table
     * @param {String} table Name of the table to update. 
     * @param {Mixed} values Array of value objects, in the format {field: value}. In case you want to use a SQL function, use {field: {function: functionName}}
     * @param {Mixed} where Array of object conditions in the format {field: value} or an object with the connectors, like {and: {}}
     */
    update (table, values, where) {
        this.values = [];

        var sql = this._buildUpdate(table, values, where);

        return this.execute(sql);
    }

    /**
     * Insert values into table
     * @param {String} table Name of the table to delete. 
     * @param {Mixed} where Array of object conditions in the format {field: value} or an object with the connectors, like {and: {}}
     */
    delete (table, where) {
        this.values = [];
        var sql = this._buildDelete(table, where);
        return this.execute(sql);
    }

    /**
     * Execute a SQL String
     * @param {String} sql SQL String to be executed
     * @param {Array} values Array of values in case of prepared statements
     */
    execute (sql, values) {
        if (values !== undefined) {
            this.values = values;
        }
        
        if (process.env.NDO_TRACE_LEVEL === 'DEBUG') {
            console.log(sql, this.values);
        }

        var op = sql.split(' ').shift().toUpperCase(),
            rows;
        return new Promise((resolve, reject) => {
            this.db.execute(sql, this.values, (err, rs) => {
                if (err) {
                    console.error(sql, this.values);
                    return reject(this._error(err))
                };
                switch(op) {
                    case 'SELECT':
                        rows = [];
                        for (let i = 0; i < rs.length; i++) {
                            var row = {};
                            for (var field in rs[i]) {    
                                let cField = _.camelCase(field);
                                row[cField] = rs[i][field];
                                // if (row[cField] && typeof row[cField] === 'object') {
                                //     row[cField] = rs[i][field].toString();
                                // }
                            }
                            rows.push(row);
                        }

                        this.values = [];
                        resolve(rows);
                        break;
                    case 'INSERT':
                        if (!this.table) {
                            this.table = sql.split(' ')[2].toUpperCase();
                        }
                        this.db.getLastId(this.table, (err, rs) => {
                            if (err) {
                                console.error('Error in getLastId', err);
                                return reject(this._error(err));
                            }
                            let row = rs;
                            if (rs.length && rs.length > 0) row = rs.pop();

                            this.values = [];
                            resolve(row);
                        });
                        
                        break;
                    default: 
                        this.values = [];
                        resolve(rs);
                        break;
                }
            });
        });
    }

    /**
     * Build a SELECT String SQL
     * 
     * @returns {String}
     * @param {String} table Name of the table to search. You can use tableName|Alias
     * @param {Mixed} fields Array with the name of the fields or * for all fields. You can use tableAlias.fieldName
     * @param {Object} where Array of object conditions in the format {field: value} or an object with the connectors, like and: {}
     * @param {Object} joins Object or array of joins in the format {table, alias, where: []}
     * @param {Array} order List of orderby objects in format {field, direction}
     * @param {Integer} limit Number of tuples to return
     * @param {Array} groupBy List of fields to group by
     * @param {Integer} offset Starting point of the query
     */
    _buildSelect (table, fields, where, joins, order, limit, groupBy, offset) {
        var values = [],
            tableAlias = table;

        var sql = `SELECT `;

        var fieldsWithAlias = [];
        if (typeof fields === 'object') {
            for (let i = 0; i < fields.length; i++) {
                let field = fields[i],
                    fieldAlias = _.camelCase(field);
                
                if (field.indexOf('.') >= 0) {
                    fieldAlias = field.split('.').pop();
                }
                
                if (field.indexOf('|') >= 0) {
                    let parts = field.split('|');
                    field = parts[0];
                    fieldAlias = parts[1]
                }
                fieldsWithAlias.push(`${field} as "${fieldAlias}"`);
            }

            sql += `${fieldsWithAlias.join(', ')} `;
        } else if (fields === '*') {
            this.allFields = true;
            sql += '* ';
        }

        if (table.indexOf('|') >= 0) {
            let parts = table.split('|');
            table = parts[0];
            tableAlias = parts[1];
        } 
        sql += `FROM ${table} as ${tableAlias} `;
        
        if (joins !== undefined && Object.keys(joins).length > 0) {
            var joinArr = [];
            if (joins.length !== undefined) {
                sql += this._buildJoins(joins, 'INNER') + ' ';
            }
            for (var joinType in joins) {
                sql += this._buildJoins(joins[joinType], joinType) + ' ';
            }
        }

        if (where && Object.keys(where).length > 0) {
            sql += `WHERE `;
            if (where.length !== undefined) {
                sql += this._buildWhere(where, 'AND');
            } else {
                for (var andOr in where) {
                    sql += this._buildWhere(where[andOr], andOr);
                }
            }
        }

        if (groupBy && groupBy.length > 0) {
            sql += `GROUP BY ${groupBy.join(', ')} `;            
        }

        if (order && Object.keys(order).length > 0) {
            let ordering = [];
            if (order.length !== undefined) {
                for (let i = 0; i < order.length; i++) {
                    ordering.push(`${order[i]} ASC`);
                }
            } else {
                for (var field in order) {
                    ordering.push(`${field} ${order[field]}`);
                }
            }

            sql += `ORDER BY ${ordering.join(', ')} `;
        }

        if (limit) {
            sql += `LIMIT ${limit} `;
        }

        if (offset !== undefined) {
            sql += `OFFSET ${offset} `;
        }

        return sql;
    }

    /**
     * Build a WHERE SQL String
     * @returns {String}
     * @param {Object} where Array of object conditions in the format {field: value} or an object with the connectors, like and: {}
     * @param {String} andOr 
     */
    _buildWhere (where, andOr) {
        var arrWhere = [];
        if (andOr === undefined) andOr = 'AND';
        if (where.length !== undefined) {
            for (let i = 0; i < where.length; i++) {
                if (where[i].field !== undefined) {
                    let strWhere = _.snakeCase(where[i].field);
                    if (where[i].field.indexOf('.') > 0) {
                        strWhere = where[i].field;
                    }
                    if (typeof where[i].value !== 'object') {
                        strWhere += (where[i].operator !== undefined ? ` ${where[i].operator} ` : '= ? ');
                        if (where[i].value !== undefined) {
                            this.values.push(where[i].value);
                        } else if (where[i].subquery !== undefined) {
                            strWhere += where[i].subquery + ' ';
                        }
                    } else {
                        strWhere += ` IN (${where[i].value.join(',')}) `;
                    }
                    arrWhere.push(strWhere);
                } else {
                    for (var field in where[i]) {
                        if (field.indexOf('.') < 0) {
                            arrWhere.push(`${_.snakeCase(field)} = ?`);
                        } else {
                            arrWhere.push(`${field} = ?`);
                        }
                        this.values.push(where[i][field]);
                    }
                }
            }
        } else {
            for (var field in where) {
                arrWhere.push(`${_.snakeCase(field)} = ?`);
                this.values.push(where[field]);
            }
        }

        return '(' + arrWhere.join(` ${andOr} `) + ') ';
    }

    /**
     * Build a JOIN SQL String based on joins
     * @returns {String}
     * @param {Object} joins Object or array of joins in the format {table, alias, on: ['t1.field = t2.field']}
     * @param {*} type Type of JOIN (INNER, FULL, LEFT)
     */
    _buildJoins (joins, type) {
        var ret = [];
        for (let i = 0; i < joins.length; i++) {
            if (typeof joins[i].on === 'string') {
                joins[i].on = [joins[i].on];
            }

            let join = {on: joins[i].on};
            if (joins[i].table.indexOf('|') >= 0) {
                let parts = joins[i].table.split('|');
                join.table = parts[0];
                join.alias = parts[1];
            } else {
                join.table = join.alias = joins[i].table;
            }

            ret.push(`${type} JOIN ${join.table} AS ${join.alias} ON ${join.on.join(' AND ')}`);
        }
        
        return ret.join("\n");
    }

    /**
     * Build an INSERT SQL String
     * @returns {String}
     * @param {String} table Name of the table to INSERT
     * @param {Mixed} values Array of value objects, in the format {field: value}. In case you want to use a SQL function, use {field: {function: functionName}}
     */
    _buildInsert (table, values) {
        var fixedValues = [], fields = [], ret = {values: [], fixedValues: []};
        var sql = '';

        if (Object.keys(values).length > 0) {
            sql = `INSERT INTO ${table} (`;
            
            if (values.length !== undefined) {
                for (let i = 0; i < values.length; i++) {
                    if (i === 0) {
                        _.forEach(values[i], (value, key) => {
                            fields.push(_.snakeCase(key));
                        });
                    }
                    let aux = this._buildInsertValues(values[i]);
                    ret.values.push(aux.values);
                    if (aux.fixedValues.length > 0) {
                        ret.fixedValues.push(aux.fixedValues);
                    }
                }
            } else {
                _.forEach(values, (value, key) => {
                    fields.push(_.snakeCase(key).toUpperCase());
                });

                ret = this._buildInsertValues(values);
            }
            
            this.values = ret.values;
            fixedValues = ret.fixedValues;

            sql += fields.join(', ') + ') VALUES (';
            if (this.values.length > 0) {
                sql += _.trimEnd(_.repeat('?, ', (fields.length - fixedValues.length)), ', ');
            }

            if (fixedValues.length > 0) {
                if (this.values.length > 0) sql += ', ';

                sql += fixedValues.join(', ');
            }

            sql += ')';
        }

        return sql;
    }

    /**
     * Create the (fields) VALUES (?) part of an INSERT SQL
     * @returns {String}
     * @param {Array} mainValues 
     */
    _buildInsertValues (mainValues) {
        var values = [], fixedValues = [];
        for (var field in mainValues) {
            if (typeof mainValues[field] === 'object' && mainValues[field]) {
                // Fixed values
                if (mainValues[field].function !== undefined) {
                    fixedValues.push(mainValues[field].function + 
                                    '(' + (mainValues[field].value !== undefined ? mainValues[field].value : '') + 
                    ')');
                }
            } else {
                values.push(mainValues[field]);
            }
        }

        return {values, fixedValues};
    }

    /**
     * Build an UPDATE SQL String
     * @returns {String} 
     * @param {String} table Name of the table to INSERT
     * @param {Mixed} values Array of value objects, in the format {field: value}. In case you want to use a SQL function, use {field: {function: functionName}}
     * @param {Object} where Array of object conditions in the format {field: value} or an object with the connectors, like and: {}
     */
    _buildUpdate (table, values, where) {
        var fields = [], 
            sql = '';
            // console.log(values);
        if (_.keys(values).length > 0) {
            sql = `UPDATE ${table} SET `;
            for(var field in values) {
                if (typeof values[field] === 'object' && values[field]) {
                    // Fixed values
                    if (values[field].function !== undefined) {
                        fields.push(_.snakeCase(field).toUpperCase() + ' = ' + values[field].function + 
                                        '(' + (values[field].value !== undefined ? values[field].value : '') + 
                        ')');
                    }
                } else {
                    fields.push(_.snakeCase(field).toUpperCase()  + ' = ?');
                    this.values.push(values[field]);
                }
            }

            sql += fields.join(', ') + ' ';
            if (where) {
                sql += 'WHERE ' + this._buildWhere(where);
            }
            
        }
        return sql;
    }

    /**
     * Build a DELETE SQL String
     * @returns {String} 
     * @param {String} table Name of the table to INSERT
     * @param {Object} where Array of object conditions in the format {field: value} or an object with the connectors, like and: {}
     */
    _buildDelete (table, where) {
        var sql = '';
        if (_.keys(where).length > 0) {
            sql = `DELETE FROM ${table} WHERE ` + this._buildWhere(where);
        }

        return sql;
    }

    /**
     * Query table for the last inserted ID
     * @param {String} table Table to query
     * @param {*} done Callback function
     */
    _getLastId (table, done) {
        if (table === undefined) table = this.table;

        const sql = `SELECT TO_BIGINT(CURRENT_IDENTITY_VALUE()) as "id" FROM ${table}`;
        this.conn.exec(sql, (err, rs) => done(err, rs));
    }

    /**
     * Create a standard error object from msg
     * @returns {Object}
     * @param {String} err Error message
     * @param {String} statusCode HTTP Status code of the error
     */
    _error (err, statusCode) {
        if (typeof err === String) {
            err = new Error(err);
        } else {
            err = new Error(err.message);
        }
        
        err.statusCode = statusCode || 500;

        return err;
    }
}

exports.ndo = (conn) => {
    let db = new HANA(conn);
    return new NDO(db);
};
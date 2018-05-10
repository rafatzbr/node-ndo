'use strict';

const { Pool, Client } = require('pg');

exports.middleware = function middleware(pgService) {
  var pool = new Pool(pgService);

  return function db(req, res, next) {
    pool.connect((err, client, release) => {
      if (err) {
        err.status = 500;
        return next(err);
      }
      
      if (pgService.schema === undefined) {
          schema = 'public';
      }

      var releaseClient = true;
      client.on('end', () => {
        releaseClient = false;
        delete req.db;
      });

      function cleanup() {
        console.log('Cleanup triggered');
        delete req.db;
        if (releaseClient) {
          releaseClient = false;
          release(true);
        }
      }

      var end = res.end;
      res.end = function () {
        cleanup();
        res.end = end;
        res.end.apply(this, arguments);
      };

      client.query(`SET search_path TO ${pgService.schema}`, (err, result) => {
        if (err) {
            err.status = 500;
            return next(err);
        rs}
        req.db = client;
        next();
      });
    });
  };
};

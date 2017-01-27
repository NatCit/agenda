/** @package

    database.js

*/

var PostgresSQL = require('pg'),
    logger = require('winston');

var pool = null,
    Database = self = {},
    Utils = {};

Database.init = function(table){
  if(table){
    self._table = table;
  }
}

Database.initPoolConn = function( config,cb ){

  if(config){
    pool = new PostgresSQL.Pool(config.postgres);

    pool.on('error', function (err, client) {
      // if an error is encountered by a client while it sits idle in the pool
      // the pool itself will emit an error event with both the error and
      // the client which emitted the original error
      // this is a rare occurrence but can happen if there is a network partition
      // between your application and the database, the database restarts, etc.
      // and so you might want to handle it and at least log it out
      logger.error('idle client error', err.message, err.stack);
    })

  }else{
    logger.error("[ initPoolConn ] Config not found!");
  }

  logger.info("[ initPoolConn ] - Init Success!");
  if(cb){
    cb( { "ok" : 1 } );
  }
};

//Get pool client
Database.poolConn = function( cb ){

  // to run a query we can acquire a client from the pool,
  // run a query on the client, and then return the client to the pool
  pool.connect(function(err, clientPG, done) {
    // Handle pool connection errors
    if(err){
      done();
      logger.error(err);
      return {success: false, data: err};
    }

    if(cb){
      return cb(clientPG, done);
    }
  });

  return this;
}

Database.deleteMany = function( query, cb ){

  self.poolConn(function(clientPG, done){

    var params = {};

    if(query){
      var querySQL = 'DELETE FROM '+ self._table +' WHERE ';
      if (query.name){
        querySQL += 'name = $1'
        params.values = [ query.name ];
      } else if (query.id) {
        querySQL += 'id = $1'
        params.values = [ query.id ];
      } else if(query.data.card_id) {
        //TODO:FIX: Implementar tratamento genérico para query de deleção
        var jsonData = 'card_id":'+ parseInt(query.data.card_id);
        querySQL += "data LIKE '%"+ jsonData +"%';"
        params.values = null;
      } else {
        logger.error("[ deleteMany ] Delete query must have parameters");
        return false;
      }
      params.querySQL = querySQL;
    }else{
      logger.error("[ deleteMany ] Delete query must have parameters");
    }

    logger.debug("[Agenda][deleteMany] querySQL: ", params.querySQL);
    logger.debug("[Agenda][deleteMany] values: ", params.values);

    var queryOnEnd = function(err, result){
        done();
        if(err){
          logger.error("[ deleteMany ] Error on database connect : ", err);
          return;
        }
        if(cb){
          cb( err, result );
        }
    }
    clientPG.query( { text : params.querySQL.toString() , values : params.values }, queryOnEnd );

  });
}

Database.insertOne = function( props, cb ){

  self.poolConn(function(clientPG, done){

    var keys = Object.keys(props),
        colValues = keys.map(function(item, idx) {return '$' + (idx+1);}),
        colNames = keys.join('","');

    var values = self.Utils.setQueryValues(props);

    //InsertOne
    var querySQL = 'INSERT INTO '+ self._table
                  +' ( "'+ colNames +'" )'
                  +' VALUES ( '+ colValues.toString() +' ) RETURNING *'

    logger.debug("[ insertOne ] Query SQL: ", querySQL);
    logger.debug("[ insertOne ] Values: ", values);
    logger.debug("[ insertOne ] Col Names: ", colValues.toString());

    var queryOnEnd = function(err, data){
        done();
        if(err){
          logger.error("[ insertOne ] : ", err);
          return;
        }
        //Tratando dados paro agenda
        var value = data.rows[0];
        var result = {};
        result.ops = value;

        if(cb){
          cb( err, result );
        }
    }
    clientPG.query( { text : querySQL , values : values }, queryOnEnd);

  });
};

Database.findAndLockNextJob = function( params, cb ){

  self.poolConn(function(clientPG,done){
    var queryOnEnd = function(err, data){
        done();
        if(err){
          logger.error("[ findAndLockNextJob ] : ", err);
          return;
        }

        logger.debug("[ findAndLockNextJob ] - Success | data = ", JSON.stringify(data.rows[0]));

        //Tratando dados paro agenda
        var value = data.rows[0];
        var result = {};
        result.value = value;

        if(cb){
          cb( err, result );
        }
    }
    clientPG.query( { text : params.querySQL , values : params.values }, queryOnEnd );

  });
};

Database.find = function( params, cb ){

    self.poolConn(function(clientPG,done){
        var queryOnEnd = function(err, data){
            done();
            if(err){
                logger.error("[ find ] : ", err);
                return;
            }

            logger.debug("[ find ] - Success | data = ", JSON.stringify(data.rows[0]));

            //Tratando dados paro agenda
            var value = data.rows[0];
            var result = {};
            result.value = value;

            if(cb){
                cb( err, result );
            }
        }
        clientPG.query( { text : params.querySQL , values : params.values }, queryOnEnd );

    });
};

Database.lockOnTheFly = function( params, cb ){

  self.poolConn(function(clientPG,done){
    var queryOnEnd = function(err, data){
        done();
        if(err){
          logger.error("[ lockOnTheFly ] : ", err);
          return;
        }

        logger.debug("[ lockOnTheFly ] - Success | data.rows = ", data.rows[0]);
        //Tratando dados paro agenda
        var value = data.rows[0];
        var result = {};
        result.value = value;

        if(cb){
          cb( err, result );
        }
    }
    clientPG.query( { text : params.querySQL.toString() , values : params.values }, queryOnEnd );

  });
};

Database.findAndModify = function( query, sort, update, options, props, cb ){

  self.poolConn(function(clientPG,done){

    var props = {};
    if(update.$set){
        props = update.$set;
        if(update.$setOnInsert){
            props.nextRunAt = update.$setOnInsert.nextRunAt;
        }
    }else if(update.unique){
        props = update.$setOnInsert;
    }

    var keys = Object.keys(props),
        colValues = keys.map(function(item, idx) {return '$' + (idx+1);});

    var queryConditions = null;
    if( query.type && query.name ){
        query.type = JSON.stringify(query.type).replace(/"/g, "'");
        query.name = JSON.stringify(query.name).replace(/"/g, "'");
        queryConditions = '"name"='+query.name+' AND "type"='+query.type;
    }else if(query.id){
        queryConditions = '"id"='+query.id;
    }

    var colNames = self.Utils.setQueryColNames(keys);
    var values = self.Utils.setQueryValues(props);

    logger.debug("[ findAndModify ] Query values: ", values);

    if(update){
      //Upsert: true
      if(options.upsert){
        var querySQL = 'WITH upsert AS (UPDATE ' + self._table + ' SET ( '+ colNames +' ) = ( '+ colValues +' ) WHERE '+ queryConditions +'  RETURNING *)'
                     + 'INSERT INTO '+ self._table +' ( '+ colNames +' ) SELECT '+ colValues +' WHERE NOT EXISTS (SELECT * FROM upsert) RETURNING *;';
      }
    }

    logger.debug("[ findAndModify ] Query SQL ["+query.name+"] : ", querySQL.toString());

    var params = {};
    params.querySQL = querySQL;
    params.values = values;

    var queryOnEnd = function(err, data){
        done();
        if(err){
          logger.error("[ findAndModify ][ "+query.name+" ] : ", err);
          return;
        }

        logger.debug("[ findAndModify ] - Success | rows = ", data.rows[0]);

        //Tratando dados paro agenda
        var value = data.rows[0];
        var result = {};
        result.value = value;

        if(cb){
          cb( err, result );
        }
    }
    clientPG.query( { text : params.querySQL.toString(), values : params.values}, queryOnEnd );

  });
};

Utils.setQueryValues = function( props, cb ){
  var values = [];
  for (var key in props) {
    //Map date types
    if(key === 'lastRunAt' || key === 'lastFinishedAt' || key === 'nextRunAt' || key === 'failedAt' || key === 'lockedAt' || key === 'repeatTimezone'){
        if(props[key]){
          values.push(JSON.stringify(props[key]).replace(/"/g, "'"));
        }else{
          props[key] = null;
          values.push(props[key]);
        }
    }else if(key === 'data'){
        values.push(props[key]);
    }else {
        values.push(props[key]);
    }
  }
  if(cb){
    return cb(values);
  }else{
    return values;
  }
};

Utils.setQueryColNames = function( colNames, cb ){
  var length = colNames.length;
  var reduceAction = function(total,value, index){
    value = JSON.stringify(value);
    if(index == 1){
      return JSON.stringify(total) +','+ value;
    }else{
      return total +','+ value;
    }
  }
  var result = colNames.reduce(reduceAction);
  if(cb){
    return cb(result);
  }else{
    return result;
  }
}

Database.Utils = Utils;

module.exports = Database;

var path = require('path');
var Agenda = require( path.join('..', 'index.js') );

//TODO:Fix: Adicionar informações num arquivo de config
var config = {};
    config.postgres = {
      user : process.env.POSTGRESDB_USER || 'postgres',
      database : 'specialist_admin',
      password : "",
      host : process.env.POSTGRESDB_HOST || 'localhost',
      port : process.env.POSTGRESDB_PORT || '5432',
      max : process.env.POSTGRESDB_MAX_POOL_CLIENT || '5', // max number of clients in the pool
      idleTimeoutMillis : process.env.POSTGRESDB_IDLE_TIMEOUT || '60000' // how long a client is allowed to remain idle before being closed
    };

var postgresConnStr = 'postgres://'+ config.postgres.user +'@' + config.postgres.host + ':' + config.postgres.port + '/' + config.database;

var configAgenda = {
      postgres : config.postgres,
      defaultLockLifetime : 604800000,
      db: {
          address: postgresConnStr,
          collection : 'agenda_test'
          }
    };

var agenda = new Agenda(configAgenda);

agenda.define('CARD_TYPE_SCHEDULE', {priority: 'high', concurrency: 3}, function(job, done) {

  var now = new Date();
  var data = job.attrs.data;
  console.log("["+ now +"] " + ">>> JOB TYPE [ SCHEDULE ] RUN! ", data);

  done();
});

agenda.define('1', {priority: 'high'}, function(job, done) {

  var now = new Date();
  var data = job.attrs.data;
  console.log("["+ now +"] " + ">>> JOB TYPE [ NOW ] RUN!", data);

  done();
});
agenda.define('2', {priority: 'high'}, function(job, done) {

    var now = new Date();
    var data = job.attrs.data;
    console.log("["+ now +"] " + ">>> JOB TYPE [ NOW ] RUN!", data);

    done();
});
agenda.define('3', {priority: 'high'}, function(job, done) {

    var now = new Date();
    var data = job.attrs.data;
    console.log("["+ now +"] " + ">>> JOB TYPE [ NOW ] RUN!", data);

    done();
});

agenda.define('CARD_TYPE_EVERY_A', {priority: 'high'}, function(job, done) {

    var now = new Date();
    var data = job.attrs.data;
    console.log("["+ now +"] " + ">>> JOB TYPE [ EVERY ] RUN! ", data);

    done();
});

agenda.define('CARD_TYPE_EVERY_B', {priority: 'high'}, function(job, done) {

    var now = new Date();
    var data = job.attrs.data;
    console.log("["+ now +"] " + ">>> JOB TYPE [ EVERY ] RUN! ", data);

    done();
});

agenda.define('CARD_TYPE_EVERY_C', {priority: 'high'}, function(job, done) {

    var now = new Date();
    var data = job.attrs.data;
    console.log("["+ now +"] " + ">>> JOB TYPE [ EVERY ] RUN! ", data);

    done();
});

agenda.on('ready', function() {

    //SCHEDULE
    //agenda.schedule('in 10 seconds', 'CARD_TYPE_SCHEDULE', {card_id: '01'});

    //NOW
    agenda.now('1' ,{ card_id: '666'});
    agenda.now('2' ,{ card_id: '333'});
    agenda.now('3' ,{ card_id: '999'});

    //EVERY
    //agenda.every('15 seconds', 'CARD_TYPE_EVERY_A', { card_id : 'A1'});
    //agenda.every('15 seconds', 'CARD_TYPE_EVERY_B', { card_id : 'B2'});
    //agenda.every('15 seconds', 'CARD_TYPE_EVERY_C', { card_id : 'C3'});

    agenda.start();

});

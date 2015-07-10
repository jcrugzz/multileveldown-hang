var Forkee = require('forkee');
var async = require('async');
var once = require('one-time');
var LevelBatch = require('level-batch-stream');
var BatchStream = require('batch-stream');
var multilevel = require('multileveldown');
var sublevel = require('subleveldown');
var net = require('net');
var uuid = require('uuid');
var faker = require('faker');
var monotonic = require('monotonic-timestamp');

function generateBatchWithId(id, len) {
  return Array.apply(null, new Array(len))
    .map(function () {
      return {
        type: 'put',
        key: monotonic(),
        value: {
          id: id,
          name: faker.name.findName(),
          email: faker.internet.email(),
          descripton: faker.lorem.paragraphs()
        }
      }
    });
}

var actions = {
  read: read,
  write: write
};

var db = multilevel.client();
var sublevels = {};
var ended = false;
var total = 0;
var all = 0;
var socket;

var fork = new Forkee()
  .on('request', function (data, callback) {
    var fn = once(function () {
      console.log('final');
      var args = Array.prototype.slice.call(arguments);
      ended = true;
      socket.destroy();
      db.close(function (err) {
        console.log('db closed');
        if (err) { return callback(err); }
        callback.apply(null, args);
      });
    });

    connect(data.port || 3000, function () {
      console.log('connected?');
      all = data.ids.length;
      actions[data.action](data, fn);
    });

  });

function connect(port, callback) {

  socket = net.connect(port);

  socket.on('connect', function () {
    if (callback) callback();
  });

  socket.on('error', function () {
    socket.destroy();
  });

  socket.on('close', function () {
    if (!ended) {
      console.log('socket closed');
      setTimeout(conect.bind(null, port), 1000);
    }
  });

  socket
    .pipe(db.connect())
    .pipe(socket);

}

function read(data, callback) {
  var ids = data.ids;
  console.log('begin read');
  sublevels = ids.reduce(function (acc, id) {
    acc[id] = sublevel(db, id, { valueEncoding: 'json' });
    return acc;
  }, {});

  stripe(ids, callback);
}

function stripe(ids, callback) {

  //
  // We are done with recursion
  //
  if (!ids.length) { return callback(); }

  var newIds = [];
  async.each(
    ids,
    function stripeOne(id, next) {
      var db = sublevels[id];
      //
      // Read the first value from the database
      //
      console.log('before read');
      peek(db, function (err, data) {
        if (err) { return next(err); }
        console.log('after read');
        //
        // If we returned undefined, we are at the end of the stack
        //
        if (!data) {
          return next();
        }

        // Do something with the value
        newIds.push(id);

        //
        // Delete the value to "pop it off the stack"
        //
        console.log('before delete');
        resilDelete(db, data.key, function (err) {
          if (err) { return next(err); }
          console.log('after delete');
          next();
        });
      });
    },
    function (err) {
      if (err) { return callback(err); }
      // There can be no error here. We handle them upstream
      stripe(newIds, callback);
    });
}

//
// Pipe a bunch of generated fake data to the multilevel instance based on the
// IDs passed down
//
function write(data, callback) {
  var ids = data.ids;

  console.log('begin write');
  sublevels = ids.reduce(function (acc, id) {
    acc[id] = sublevel(db, id, { valueEncoding: 'json' });
    return acc;
  }, {});

  async.each(ids, function (id, next) {
    var db = sublevels[id];
    db.batch(generateBatchWithId(id, 100), next);
  }, callback);
}


/**
 * A delete that doesn't fail because it retries recursively using setImmediate
 * so it won't starve i/o
 */
function resilDelete(d, key, cb) {
  d.batch([{ type: 'del', key: key }], function (err) {
    if (err) {
      console.error('Error deleting key %s', key);
      if (!err.notFound) {
        return void setImmediate(resilDelete, d, key, cb);
      }
    }
    cb();
  });
}

/*
 * Return the first element in a database
 */
function peek(d, cb) {
  var data;
  var fn = once(cb);
  d.createReadStream({ limit: 1 }).on('data', function (d) {
    data = d;
  }).on('error', fn).on('end', function () {
    fn(null, data);
  });
}


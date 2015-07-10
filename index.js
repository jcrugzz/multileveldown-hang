var EE = require('events').EventEmitter;
var net = require('net');
var once = require('one-time');
var Fork = require('fork');
var level = require('level-hyper');
var async = require('async');
var multilevel = require('multileveldown');
var uuid = require('uuid');

var child = require.resolve('./child');

function generateIds(len) {
  return Array.apply(null, new Array(len))
    .map(function () {
      return uuid();
    })
}

function Repro(options) {
  options = options || {};

  this.task = options.task || 'write';

  this.number = options.number || 3;
  this.iteration = 0;

  this.path = options.path || 'test.db';
  this.port = options.port || 3000;

  this._level = level(this.path, { valueEncoding: 'json' });

  this._net = net.createServer(function (socket) {

    socket.on('error', function(err) {
      socket.destroy();
    });

    socket.pipe(multilevel.server(this._level)).pipe(socket);

  }.bind(this)).listen(this.port, this._onConnect.bind(this));
}

Repro.prototype = new EE();
Repro.prototype.constructor = Repro;

Repro.prototype._onConnect = function (err) {
  var self = this;
  if (err) return this.emit('error', err);
  this.sequence();
};

Repro.prototype.sequence = function () {
  var self = this;
  var action = 'series';
  async[action](
    this.functions(),
    function (err) {
      if (err) { return this.emit('error', err); }
      self.finish();
    });

};

Repro.prototype.functions = function () {
  var self = this;
  return Array.apply(null, new Array(this.number)).map(function (_, idx) {
    return self.once.bind(self, idx);
  });
};

Repro.prototype.once = function (idx, cb) {
  var self = this;
  async.waterfall([
    this.load.bind(this, idx),
    this.spawn.bind(this)
  ], function (err) {
    if (err) { return cb(err); }
    return self.task == 'read'
      ? self.cleanup(idx, cb)
      : cb();
  });
};

Repro.prototype.finish = function () {
  this.emit('finish');
};

Repro.prototype.cleanup = function (idx, cb) {
  var self = this;
  this._level.del('ids' + idx, cb);
};

Repro.prototype.load = function (idx, cb) {
  var self = this;
  if (this.task === 'write') {
    var ids = generateIds(1000);
    return this.store(ids, idx, cb);
  }

  this._level.get('ids' + idx, function (err, ids) {
    if (err) { return cb(err); }
    cb(null, ids);
  });

};

Repro.prototype.store = function (ids, idx, cb) {
  var self = this;
  this._level.put('ids' + idx, ids, function (err) {
    if (err) { return cb(err); }
    cb(null, ids);
  });
}

Repro.prototype.spawn = function (ids, callback) {
  var fn = once(callback);
  return new Fork({
    path: child,
    execArgv: ['--max_old_space_size=4096']
  })
    .fork({ action: this.task, ids: ids }, fn);
};

var argv = process.argv.slice(2);

var repro = new Repro({ task: argv[0] })
  .on('error', function (err) {
    console.error(err);
    process.exit(1);
  })
  .on('finish', function () {
    console.log('%s case finished', repro.task);
    process.exit(0);
  });


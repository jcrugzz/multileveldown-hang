var EE = require('events').EventEmitter;
var net = require('net');
var once = require('one-time');
var Fork = require('fork');
var level = require('level-hyper');
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

  this.path = options.path || 'test.db';
  this.port = options.port || 3000;
  this.ids = generateIds(100);

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

  this.spawn(this.task, function (err) {
    if (err) return self.emit('error', err);
    this.emit('finish');
  });

};

Repro.prototype.spawn = function (type, callback) {
  callback = once(callback);
  return new Fork(child)
    .fork({ action: type, ids: this.ids }, callback);
};

var argv = process.argv.slice(2);

var repro = new Repro({ task: argv[0] })
  .on('error', function (err) {
    console.error(err);
    process.exit(1);
  })
  .on('finish', function () {
    console.log('Test case finished');
    process.exit(0);
  });


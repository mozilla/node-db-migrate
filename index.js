var assert = require('assert');
var path = require('path');

var driver = require('./lib/driver');
var Migrator = require('./lib/migrator');
var log = require('./lib/log');

exports.dataType = require('./lib/data_type');
exports.config = require('./lib/config');

exports.connect = function(config, callback) {
  driver.connect(config, function(err, db) {
    if (err) { callback(err); return; }
    callback(null, new Migrator(db, config['migrations-dir']));
  });
};

exports.createMigration = function(title, migrationsDir, callback) {
  var migration = new Migration(title, migrationsDir, new Date());
  migration.write(function(err) {
    if (err) { callback(err); return; }
    callback(null, migration);
  });
};

function onComplete(migrator, cb, err) {
  migrator.driver.close();
  assert.ifError(err);
  log.info('Done');
  if("function" === typeof cb) {
    cb.call();
  }
}

exports.up = function(config, migrationsDir, destination, count, cb) {
  count = (undefined === count) ? Number.MAX_VALUE : count;
  exports.connect(config, function(err, migrator) {
    assert.ifError(err);
    migrator.migrationsDir = path.resolve(migrationsDir);
    migrator.driver.createMigrationsTable(function(err) {
      assert.ifError(err);
      migrator.up({destination: destination, count: count}, onComplete.bind(this, migrator, cb));
    });
  });
};

exports.down = function(config, migrationsDir, destination, count) {
  count = (undefined === count) ? Number.MAX_VALUE : count;
  exports.connect(config, function(err, migrator) {
    assert.ifError(err);
    migrator.migrationsDir = path.resolve(migrationsDir);
    migrator.driver.createMigrationsTable(function(err) {
      assert.ifError(err);
      migrator.down({destination: destination, count: count}, onComplete.bind(this, migrator));
    });
  });
};
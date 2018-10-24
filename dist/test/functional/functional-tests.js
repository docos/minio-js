/*
 * Minio Javascript Library for Amazon S3 Compatible Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var os = require('os');
var stream = require('stream');
var crypto = require('crypto');
var async = require('async');
var _ = require('lodash');
var fs = require('fs');
var http = require('http');
var https = require('https');
var url = require('url');
var chai = require('chai');
var assert = chai.assert;
var superagent = require('superagent');
var uuid = require("uuid");
var step = require("mocha-steps").step;
var minio = undefined;

try {
  minio = require('../../../dist/main/minio');
} catch (err) {
  minio = require('minio');
}

require('source-map-support').install();

describe('functional tests', function () {
  this.timeout(30 * 60 * 1000);
  var playConfig = {};
  // If credentials aren't given, default to play.minio.io.
  if (process.env['SERVER_ENDPOINT']) {
    var res = process.env['SERVER_ENDPOINT'].split(":");
    playConfig.endPoint = res[0];
    playConfig.port = parseInt(res[1]);
  } else {
    playConfig.endPoint = 'play.minio.io';
    playConfig.port = 9000;
  }
  playConfig.accessKey = process.env['ACCESS_KEY'] || 'Q3AM3UQ867SPQQA43P2F';
  playConfig.secretKey = process.env['SECRET_KEY'] || 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG';

  // If the user provides ENABLE_HTTPS, 1 = secure, anything else = unsecure.
  // Otherwise default useSSL as true.
  if (process.env['ENABLE_HTTPS'] !== undefined) {
    playConfig.useSSL = process.env['ENABLE_HTTPS'] == '1';
  } else {
    playConfig.useSSL = true;
  }

  // dataDir is falsy if we need to generate data on the fly. Otherwise, it will be
  // a directory with files to read from, i.e. /mint/data.
  var dataDir = process.env['MINT_DATA_DIR'];

  var client = new minio.Client(playConfig);
  var usEastConfig = playConfig;
  usEastConfig.region = 'us-east-1';
  var clientUsEastRegion = new minio.Client(usEastConfig);

  var bucketName = "minio-js-test-" + uuid.v4();
  var objectName = uuid.v4();

  var _1byteObjectName = 'datafile-1-b';
  var _1byte = dataDir ? fs.readFileSync(dataDir + '/' + _1byteObjectName) : new Buffer(1).fill(0);

  var _100kbObjectName = 'datafile-100-kB';
  var _100kb = dataDir ? fs.readFileSync(dataDir + '/' + _100kbObjectName) : new Buffer(100 * 1024).fill(0);
  var _100kbObjectNameCopy = _100kbObjectName + '-copy';

  var _100kbObjectBufferName = _100kbObjectName + '.buffer';
  var _MultiPath100kbObjectBufferName = 'path/to/' + _100kbObjectName + '.buffer';
  var _100kbmd5 = crypto.createHash('md5').update(_100kb).digest('hex');
  var _100kb1kboffsetmd5 = crypto.createHash('md5').update(_100kb.slice(1024)).digest('hex');

  var _6mbObjectName = 'datafile-6-MB';
  var _6mb = dataDir ? fs.readFileSync(dataDir + '/' + _6mbObjectName) : new Buffer(6 * 1024 * 1024).fill(0);
  var _6mbmd5 = crypto.createHash('md5').update(_6mb).digest('hex');
  var _6mbObjectNameCopy = _6mbObjectName + '-copy';

  var _5mbObjectName = 'datafile-5-MB';
  var _5mb = dataDir ? fs.readFileSync(dataDir + '/' + _5mbObjectName) : new Buffer(5 * 1024 * 1024).fill(0);
  var _5mbmd5 = crypto.createHash('md5').update(_5mb).digest('hex');

  var metaData = {
    'Content-Type': 'text/html',
    'Content-Language': 123,
    'X-Amz-Meta-Testing': 1234,
    'randomstuff': 5678
  };

  var tmpDir = os.tmpdir();

  var traceStream;

  // FUNCTIONAL_TEST_TRACE env variable contains the path to which trace
  // will be logged. Set it to /dev/stdout log to the stdout.
  if (process.env['FUNCTIONAL_TEST_TRACE']) {
    var filePath = process.env['FUNCTIONAL_TEST_TRACE'];
    // This is necessary for windows.
    if (filePath === 'process.stdout') {
      traceStream = process.stdout;
    } else {
      traceStream = fs.createWriteStream(filePath, { flags: 'a' });
    }
    traceStream.write('====================================\n');
    client.traceOn(traceStream);
  }

  before(function (done) {
    return client.makeBucket(bucketName, '', done);
  });
  after(function (done) {
    return client.removeBucket(bucketName, done);
  });

  if (traceStream) {
    after(function () {
      client.traceOff();
      if (filePath !== 'process.stdout') {
        traceStream.end();
      }
    });
  }

  describe('makeBucket with period and region', function () {
    if (playConfig.endPoint === 's3.amazonaws.com') {
      step('makeBucket(bucketName, region, cb)_region:eu-central-1_', function (done) {
        return client.makeBucket(bucketName + '.sec.period', 'eu-central-1', done);
      });
      step('removeBucket(bucketName, cb)__', function (done) {
        return client.removeBucket(bucketName + '.sec.period', done);
      });
    }
  });

  describe('listBuckets', function () {
    step('listBuckets(cb)__', function (done) {
      client.listBuckets(function (e, buckets) {
        if (e) return done(e);
        if (_.find(buckets, { name: bucketName })) return done();
        done(new Error('bucket not found'));
      });
    });
    step('listBuckets()__', function (done) {
      client.listBuckets().then(function (buckets) {
        if (!_.find(buckets, { name: bucketName })) return done(new Error('bucket not found'));
      }).then(function () {
        return done();
      })['catch'](done);
    });
  });

  describe('makeBucket with region', function () {
    step('makeBucket(bucketName, region, cb)_bucketName:' + bucketName + '-region, region:us-east-2_', function (done) {
      try {
        clientUsEastRegion.makeBucket(bucketName + '-region', 'us-east-2', assert.fail);
      } catch (e) {
        done();
      }
    });
    step('makeBucket(bucketName, region, cb)_bucketName:' + bucketName + '-region, region:us-east-1_', function (done) {
      clientUsEastRegion.makeBucket(bucketName + '-region', 'us-east-1', done);
    });
    step('removeBucket(bucketName, cb)_bucketName:' + bucketName + '-region_', function (done) {
      clientUsEastRegion.removeBucket(bucketName + '-region', done);
    });
    step('makeBucket(bucketName, region)_bucketName:' + bucketName + '-region, region:us-east-1_', function (done) {
      clientUsEastRegion.makeBucket(bucketName + '-region', 'us-east-1', function (e) {
        if (e) {
          // Some object storage servers like Azure, might not delete a bucket rightaway
          // Add a sleep of 40 seconds and retry
          setTimeout(function () {
            clientUsEastRegion.makeBucket(bucketName + '-region', 'us-east-1', done);
          }, 40 * 1000);
        } else done();
      });
    });
    step('removeBucket(bucketName)_bucketName:' + bucketName + '-region_', function (done) {
      clientUsEastRegion.removeBucket(bucketName + '-region').then(function () {
        return done();
      })['catch'](done);
    });
  });

  describe('bucketExists', function () {
    step('bucketExists(bucketName, cb)_bucketName:' + bucketName + '_', function (done) {
      return client.bucketExists(bucketName, done);
    });
    step('bucketExists(bucketName, cb)_bucketName:' + bucketName + 'random_', function (done) {
      client.bucketExists(bucketName + 'random', function (e, exists) {
        if (e === null && !exists) return done();
        done(new Error());
      });
    });
    step('bucketExists(bucketName)_bucketName:' + bucketName + '_', function (done) {
      client.bucketExists(bucketName).then(function () {
        return done();
      })['catch'](done);
    });
  });

  describe('removeBucket', function () {
    step('removeBucket(bucketName, cb)_bucketName:' + bucketName + 'random_', function (done) {
      client.removeBucket(bucketName + 'random', function (e) {
        if (e.code === 'NoSuchBucket') return done();
        done(new Error());
      });
    });
    step('makeBucket(bucketName, region)_bucketName:' + bucketName + '-region-1, region:us-east-1_', function (done) {
      client.makeBucket(bucketName + '-region-1', 'us-east-1').then(function () {
        return client.removeBucket(bucketName + '-region-1');
      }).then(function () {
        return done();
      })['catch'](done);
    });
  });
  describe('tests for putObject getObject removeObject with multipath', function () {
    step('putObject(bucketName, objectName, stream)_bucketName:' + bucketName + ', objectName:' + _MultiPath100kbObjectBufferName + ', stream:100Kib_', function (done) {
      client.putObject(bucketName, _MultiPath100kbObjectBufferName, _100kb).then(function () {
        return done();
      })['catch'](done);
    });

    step('getObject(bucketName, objectName, callback)_bucketName:' + bucketName + ', objectName:' + _MultiPath100kbObjectBufferName + '_', function (done) {
      var hash = crypto.createHash('md5');
      client.getObject(bucketName, _MultiPath100kbObjectBufferName, function (e, stream) {
        if (e) return done(e);
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('error', done);
        stream.on('end', function () {
          if (hash.digest('hex') === _100kbmd5) return done();
          done(new Error('content mismatch'));
        });
      });
    });

    step('removeObject(bucketName, objectName)_bucketName:' + bucketName + ', objectName:' + _MultiPath100kbObjectBufferName + '_', function (done) {
      client.removeObject(bucketName, _MultiPath100kbObjectBufferName).then(function () {
        return done();
      })['catch'](done);
    });
  });
  describe('tests for putObject copyObject getObject getPartialObject statObject removeObject', function () {

    var tmpFileUpload = tmpDir + '/' + _100kbObjectName;
    step('fPutObject(bucketName, objectName, filePath, metaData, callback)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + ', filePath: ' + tmpFileUpload + '_', function (done) {
      fs.writeFileSync(tmpFileUpload, _100kb);
      client.fPutObject(bucketName, _100kbObjectName, tmpFileUpload, done);
    });

    step('putObject(bucketName, objectName, stream, size, metaData, callback)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + ', stream:100kb, size:' + _100kb.length + ', metaData:' + metaData + '_', function (done) {
      var stream = readableStream(_100kb);
      client.putObject(bucketName, _100kbObjectName, stream, _100kb.length, metaData, done);
    });

    step('putObject(bucketName, objectName, stream, size, metaData, callback)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + ', stream:100kb, size:' + _100kb.length + '_', function (done) {
      var stream = readableStream(_100kb);
      client.putObject(bucketName, _100kbObjectName, stream, _100kb.length, done);
    });

    step('getObject(bucketName, objectName, callback)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + '_', function (done) {
      var hash = crypto.createHash('md5');
      client.getObject(bucketName, _100kbObjectName, function (e, stream) {
        if (e) return done(e);
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('error', done);
        stream.on('end', function () {
          if (hash.digest('hex') === _100kbmd5) return done();
          done(new Error('content mismatch'));
        });
      });
    });

    step('putObject(bucketName, objectName, stream, callback)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + ', stream:100kb_', function (done) {
      client.putObject(bucketName, _100kbObjectBufferName, _100kb, '', done);
    });

    step('getObject(bucketName, objectName, callback)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + '_', function (done) {
      var hash = crypto.createHash('md5');
      client.getObject(bucketName, _100kbObjectBufferName, function (e, stream) {
        if (e) return done(e);
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('error', done);
        stream.on('end', function () {
          if (hash.digest('hex') === _100kbmd5) return done();
          done(new Error('content mismatch'));
        });
      });
    });

    step('putObject(bucketName, objectName, stream, metaData)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + ', stream:100kb_, metaData:{}', function (done) {
      client.putObject(bucketName, _100kbObjectBufferName, _100kb, {}).then(function () {
        return done();
      })['catch'](done);
    });

    step('getPartialObject(bucketName, objectName, offset, length, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + ', offset:0, length=1024_', function (done) {
      client.getPartialObject(bucketName, _100kbObjectBufferName, 0, 1024).then(function (stream) {
        stream.on('data', function () {});
        stream.on('end', done);
      })['catch'](done);
    });

    step('getPartialObject(bucketName, objectName, offset, length, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + ', offset:1024, length=1024_', function (done) {
      var expectedHash = crypto.createHash('md5').update(_100kb.slice(1024, 2048)).digest('hex');
      var hash = crypto.createHash('md5');
      client.getPartialObject(bucketName, _100kbObjectBufferName, 1024, 1024).then(function (stream) {
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('end', function () {
          if (hash.digest('hex') === expectedHash) return done();
          done(new Error('content mismatch'));
        });
      })['catch'](done);
    });

    step('getPartialObject(bucketName, objectName, offset, length, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + ', offset:1024', function (done) {
      var hash = crypto.createHash('md5');
      client.getPartialObject(bucketName, _100kbObjectBufferName, 1024).then(function (stream) {
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('end', function () {
          if (hash.digest('hex') === _100kb1kboffsetmd5) return done();
          done(new Error('content mismatch'));
        });
      })['catch'](done);
    });

    step('getObject(bucketName, objectName)_bucketName:' + bucketName + ', objectName:' + _100kbObjectBufferName + '_', function (done) {
      client.getObject(bucketName, _100kbObjectBufferName).then(function (stream) {
        stream.on('data', function () {});
        stream.on('end', done);
      })['catch'](done);
    });

    step('putObject(bucketName, objectName, stream, cb)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + '_', function (done) {
      var stream = readableStream(_6mb);
      client.putObject(bucketName, _6mbObjectName, stream, done);
    });

    step('getObject(bucketName, objectName, cb)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + '_', function (done) {
      var hash = crypto.createHash('md5');
      client.getObject(bucketName, _6mbObjectName, function (e, stream) {
        if (e) return done(e);
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('error', done);
        stream.on('end', function () {
          if (hash.digest('hex') === _6mbmd5) return done();
          done(new Error('content mismatch'));
        });
      });
    });

    step('getObject(bucketName, objectName, cb)_bucketName:' + bucketName + ' non-existent object', function (done) {
      client.getObject(bucketName, 'an-object-that-does-not-exist', function (e, stream) {
        if (stream) return done(new Error("on errors the stream object should not exist"));
        if (!e) return done(new Error("expected an error object"));
        if (e.code !== 'NoSuchKey') return done(new Error("expected NoSuchKey error"));
        done();
      });
    });

    step('getPartialObject(bucketName, objectName, offset, length, cb)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', offset:0, length:100*1024_', function (done) {
      var hash = crypto.createHash('md5');
      var expectedHash = crypto.createHash('md5').update(_6mb.slice(0, 100 * 1024)).digest('hex');
      client.getPartialObject(bucketName, _6mbObjectName, 0, 100 * 1024, function (e, stream) {
        if (e) return done(e);
        stream.on('data', function (data) {
          return hash.update(data);
        });
        stream.on('error', done);
        stream.on('end', function () {
          if (hash.digest('hex') === expectedHash) return done();
          done(new Error('content mismatch'));
        });
      });
    });

    step('copyObject(bucketName, objectName, srcObject, cb)_bucketName:' + bucketName + ', objectName:' + _6mbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _6mbObjectName + '_', function (done) {
      client.copyObject(bucketName, _6mbObjectNameCopy, "/" + bucketName + "/" + _6mbObjectName, function (e) {
        if (e) return done(e);
        done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject)_bucketName:' + bucketName + ', objectName:' + _6mbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _6mbObjectName + '_', function (done) {
      client.copyObject(bucketName, _6mbObjectNameCopy, "/" + bucketName + "/" + _6mbObjectName).then(function () {
        return done();
      })['catch'](done);
    });

    step('statObject(bucketName, objectName, cb)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + '_', function (done) {
      client.statObject(bucketName, _6mbObjectName, function (e, stat) {
        if (e) return done(e);
        if (stat.size !== _6mb.length) return done(new Error('size mismatch'));
        done();
      });
    });

    step('statObject(bucketName, objectName)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + '_', function (done) {
      client.statObject(bucketName, _6mbObjectName).then(function (stat) {
        if (stat.size !== _6mb.length) return done(new Error('size mismatch'));
      }).then(function () {
        return done();
      })['catch'](done);
    });

    step('removeObject(bucketName, objectName)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + '_', function (done) {
      client.removeObject(bucketName, _100kbObjectName).then(function () {
        async.map([_100kbObjectBufferName, _6mbObjectName, _6mbObjectNameCopy], function (objectName, cb) {
          return client.removeObject(bucketName, objectName, cb);
        }, done);
      })['catch'](done);
    });
  });

  describe('tests for copyObject statObject', function () {
    var etag;
    var modifiedDate;
    step('putObject(bucketName, objectName, stream, metaData, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + ', stream: 100kb, metaData:' + metaData + '_', function (done) {
      client.putObject(bucketName, _100kbObjectName, _100kb, metaData, done);
    });

    step('copyObject(bucketName, objectName, srcObject, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + '_', function (done) {
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, function (e) {
        if (e) return done(e);
        done();
      });
    });

    step('statObject(bucketName, objectName, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectName + '_', function (done) {
      client.statObject(bucketName, _100kbObjectName, function (e, stat) {
        if (e) return done(e);
        if (stat.size !== _100kb.length) return done(new Error('size mismatch'));
        if (Object.keys(stat.metaData).length !== Object.keys(metaData).length) return done(new Error('content-type mismatch'));
        assert.equal(stat.metaData['content-type'], metaData['Content-Type']);
        assert.equal(stat.metaData['Testing'], metaData['Testing']);
        assert.equal(stat.metaData['randomstuff'], metaData['randomstuff']);
        etag = stat.etag;
        modifiedDate = stat.modifiedDate;
        done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject, conditions, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + ', conditions:ExceptIncorrectEtag_', function (done) {
      var conds = new minio.CopyConditions();
      conds.setMatchETagExcept('TestEtag');
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, conds, function (e) {
        if (e) return done(e);
        done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject, conditions, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + ', conditions:ExceptCorrectEtag_', function (done) {
      var conds = new minio.CopyConditions();
      conds.setMatchETagExcept(etag);
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, conds).then(function () {
        done(new Error("CopyObject should have failed."));
      })['catch'](function () {
        return done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject, conditions, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + ', conditions:MatchCorrectEtag_', function (done) {
      var conds = new minio.CopyConditions();
      conds.setMatchETag(etag);
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, conds, function (e) {
        if (e) return done(e);
        done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject, conditions, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + ', conditions:MatchIncorrectEtag_', function (done) {
      var conds = new minio.CopyConditions();
      conds.setMatchETag('TestETag');
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, conds).then(function () {
        done(new Error("CopyObject should have failed."));
      })['catch'](function () {
        return done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject, conditions, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + ', conditions:Unmodified since ' + modifiedDate, function (done) {
      var conds = new minio.CopyConditions();
      conds.setUnmodified(new Date(modifiedDate));
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, conds, function (e) {
        if (e) return done(e);
        done();
      });
    });

    step('copyObject(bucketName, objectName, srcObject, conditions, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + ', srcObject:/' + bucketName + '/' + _100kbObjectName + ', conditions:Unmodified since 2010-03-26T12:00:00Z_', function (done) {
      var conds = new minio.CopyConditions();
      conds.setUnmodified(new Date("2010-03-26T12:00:00Z"));
      client.copyObject(bucketName, _100kbObjectNameCopy, "/" + bucketName + "/" + _100kbObjectName, conds).then(function () {
        done(new Error("CopyObject should have failed."));
      })['catch'](function () {
        return done();
      });
    });

    step('statObject(bucketName, objectName, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + '_', function (done) {
      client.statObject(bucketName, _100kbObjectNameCopy, function (e, stat) {
        if (e) return done(e);
        if (stat.size !== _100kb.length) return done(new Error('size mismatch'));
        done();
      });
    });

    step('removeObject(bucketName, objectName, cb)_bucketName:' + bucketName + ', objectName:' + _100kbObjectNameCopy + '_', function (done) {
      async.map([_100kbObjectName, _100kbObjectNameCopy], function (objectName, cb) {
        return client.removeObject(bucketName, objectName, cb);
      }, done);
    });
  });

  describe('listIncompleteUploads removeIncompleteUpload', function () {
    step('initiateNewMultipartUpload(bucketName, objectName, metaData, cb)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', metaData:' + metaData, function (done) {
      client.initiateNewMultipartUpload(bucketName, _6mbObjectName, metaData, done);
    });
    step('listIncompleteUploads(bucketName, prefix, recursive)_bucketName:' + bucketName + ', prefix:' + _6mbObjectName + ', recursive: true_', function (done) {
      // Minio's ListIncompleteUploads returns an empty list, so skip this on non-AWS.
      // See: https://github.com/minio/minio/commit/75c43bfb6c4a2ace
      if (!client.host.includes('s3.amazonaws.com')) {
        this.skip();
      }

      var found = false;
      client.listIncompleteUploads(bucketName, _6mbObjectName, true).on('error', function (e) {
        return done(e);
      }).on('data', function (data) {
        if (data.key === _6mbObjectName) found = true;
      }).on('end', function () {
        if (found) return done();
        done(new Error(_6mbObjectName + ' not found during listIncompleteUploads'));
      });
    });
    step('listIncompleteUploads(bucketName, prefix, recursive)_bucketName:' + bucketName + ', recursive: true_', function (done) {
      // Minio's ListIncompleteUploads returns an empty list, so skip this on non-AWS.
      // See: https://github.com/minio/minio/commit/75c43bfb6c4a2ace
      if (!client.host.includes('s3.amazonaws.com')) {
        this.skip();
      }

      var found = false;
      client.listIncompleteUploads(bucketName, "", true).on('error', function (e) {
        return done(e);
      }).on('data', function (data) {
        if (data.key === _6mbObjectName) found = true;
      }).on('end', function () {
        if (found) return done();
        done(new Error(_6mbObjectName + ' not found during listIncompleteUploads'));
      });
    });
    step('removeIncompleteUploads(bucketName, prefix)_bucketName:' + bucketName + ', prefix:' + _6mbObjectName + '_', function (done) {
      client.removeIncompleteUpload(bucketName, _6mbObjectName).then(done)['catch'](done);
    });
  });

  describe('fPutObject fGetObject', function () {
    var tmpFileUpload = tmpDir + '/' + _6mbObjectName;
    var tmpFileDownload = tmpDir + '/' + _6mbObjectName + '.download';

    step('fPutObject(bucketName, objectName, filePath, callback)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', filePath:' + tmpFileUpload + '_', function (done) {
      fs.writeFileSync(tmpFileUpload, _6mb);
      client.fPutObject(bucketName, _6mbObjectName, tmpFileUpload, done);
    });

    step('fPutObject(bucketName, objectName, filePath, metaData, callback)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', filePath:' + tmpFileUpload + ', metaData: ' + metaData + '_', function (done) {
      return client.fPutObject(bucketName, _6mbObjectName, tmpFileUpload, metaData, done);
    });
    step('fGetObject(bucketName, objectName, filePath, callback)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', filePath:' + tmpFileDownload + '_', function (done) {
      client.fGetObject(bucketName, _6mbObjectName, tmpFileDownload).then(function () {
        var md5sum = crypto.createHash('md5').update(fs.readFileSync(tmpFileDownload)).digest('hex');
        if (md5sum === _6mbmd5) return done();
        return done(new Error('md5sum mismatch'));
      })['catch'](done);
    });

    step('removeObject(bucketName, objectName, filePath, callback)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + '_', function (done) {
      fs.unlinkSync(tmpFileDownload);
      client.removeObject(bucketName, _6mbObjectName).then(function () {
        return done();
      })['catch'](done);
    });

    step('fPutObject(bucketName, objectName, filePath, metaData)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', filePath:' + tmpFileUpload + '_', function (done) {
      client.fPutObject(bucketName, _6mbObjectName, tmpFileUpload).then(function () {
        return done();
      })['catch'](done);
    });

    step('fGetObject(bucketName, objectName, filePath)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + ', filePath:' + tmpFileDownload + '_', function (done) {
      client.fGetObject(bucketName, _6mbObjectName, tmpFileDownload).then(function () {
        return done();
      })['catch'](done);
    });

    step('removeObject(bucketName, objectName, filePath, callback)_bucketName:' + bucketName + ', objectName:' + _6mbObjectName + '_', function (done) {
      fs.unlinkSync(tmpFileUpload);
      fs.unlinkSync(tmpFileDownload);
      client.removeObject(bucketName, _6mbObjectName, done);
    });
  });
  describe('fGetObject-resume', function () {
    var localFile = tmpDir + '/' + _5mbObjectName;
    step('putObject(bucketName, objectName, stream, metaData, cb)_bucketName:' + bucketName + ', objectName:' + _5mbObjectName + ', stream:5mb_', function (done) {
      var stream = readableStream(_5mb);
      client.putObject(bucketName, _5mbObjectName, stream, _5mb.length, {}, done);
    });
    step('fGetObject(bucketName, objectName, filePath, callback)_bucketName:' + bucketName + ', objectName:' + _5mbObjectName + ', filePath:' + localFile, function (done) {
      var tmpFile = tmpDir + '/' + _5mbObjectName + '.' + _5mbmd5 + '.part.minio';
      // create a partial file
      fs.writeFileSync(tmpFile, _100kb);
      client.fGetObject(bucketName, _5mbObjectName, localFile).then(function () {
        var md5sum = crypto.createHash('md5').update(fs.readFileSync(localFile)).digest('hex');
        if (md5sum === _5mbmd5) return done();
        return done(new Error('md5sum mismatch'));
      })['catch'](done);
    });
    step('removeObject(bucketName, objectName, callback)_bucketName:' + bucketName + ', objectName:' + _5mbObjectName + '_', function (done) {
      fs.unlinkSync(localFile);
      client.removeObject(bucketName, _5mbObjectName, done);
    });
  });

  describe('bucket policy', function () {
    var policy = '{"Version":"2012-10-17","Statement":[{"Action":["s3:GetBucketLocation","s3:ListBucket"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::' + bucketName + '"],"Sid":""},{"Action":["s3:GetObject"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::' + bucketName + '/*"],"Sid":""}]}';

    step('setBucketPolicy(bucketName, bucketPolicy, cb)_bucketName:' + bucketName + ', bucketPolicy:' + policy + '_', function (done) {
      client.setBucketPolicy(bucketName, policy, function (err) {
        if (err && err.code == 'NotImplemented') return done();
        if (err) return done(err);
        done();
      });
    });

    step('getBucketPolicy(bucketName, cb)_bucketName:' + bucketName + '_', function (done) {
      client.getBucketPolicy(bucketName, function (err, response) {
        if (err && err.code == 'NotImplemented') return done();
        if (err) return done(err);
        if (!response) {
          return done(new Error('policy is empty'));
        }
        done();
      });
    });
  });

  describe('presigned operations', function () {
    step('presignedPutObject(bucketName, objectName, expires, cb)_bucketName:' + bucketName + ', objectName:' + _1byteObjectName + ', expires: 1000_', function (done) {
      client.presignedPutObject(bucketName, _1byteObjectName, 1000, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'PUT';
        options.headers = {
          'content-length': _1byte.length
        };
        if (options.protocol === 'https:') transport = https;
        var request = transport.request(options, function (response) {
          if (response.statusCode !== 200) return done(new Error('error on put : ' + response.statusCode));
          response.on('error', function (e) {
            return done(e);
          });
          response.on('end', function () {
            return done();
          });
          response.on('data', function () {});
        });
        request.on('error', function (e) {
          return done(e);
        });
        request.write(_1byte);
        request.end();
      });
    });

    step('presignedPutObject(bucketName, objectName, expires)_bucketName:' + bucketName + ', objectName:' + _1byteObjectName + ', expires:-123_', function (done) {
      // negative values should trigger an error
      client.presignedPutObject(bucketName, _1byteObjectName, -123).then(function () {
        done(new Error('negative values should trigger an error'));
      })['catch'](function () {
        return done();
      });
    });

    step('presignedPutObject(bucketName, objectName)_bucketName:' + bucketName + ', objectName:' + _1byteObjectName + '_', function (done) {
      // Putting the same object should not cause any error
      client.presignedPutObject(bucketName, _1byteObjectName).then(function () {
        return done();
      })['catch'](done);
    });

    step('presignedGetObject(bucketName, objectName, expires, cb)_bucketName:' + bucketName + ', objectName:' + _1byteObjectName + ', expires:1000_', function (done) {
      client.presignedGetObject(bucketName, _1byteObjectName, 1000, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'GET';
        if (options.protocol === 'https:') transport = https;
        var request = transport.request(options, function (response) {
          if (response.statusCode !== 200) return done(new Error('error on put : ' + response.statusCode));
          var error = null;
          response.on('error', function (e) {
            return done(e);
          });
          response.on('end', function () {
            return done(error);
          });
          response.on('data', function (data) {
            if (data.toString() !== _1byte.toString()) {
              error = new Error('content mismatch');
            }
          });
        });
        request.on('error', function (e) {
          return done(e);
        });
        request.end();
      });
    });

    step('presignedUrl(httpMethod, bucketName, objectName, expires, cb)_httpMethod:GET, bucketName:' + bucketName + ', objectName:' + _1byteObjectName + ', expires:1000_', function (done) {
      client.presignedUrl('GET', bucketName, _1byteObjectName, 1000, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'GET';
        if (options.protocol === 'https:') transport = https;
        var request = transport.request(options, function (response) {
          if (response.statusCode !== 200) return done(new Error('error on put : ' + response.statusCode));
          var error = null;
          response.on('error', function (e) {
            return done(e);
          });
          response.on('end', function () {
            return done(error);
          });
          response.on('data', function (data) {
            if (data.toString() !== _1byte.toString()) {
              error = new Error('content mismatch');
            }
          });
        });
        request.on('error', function (e) {
          return done(e);
        });
        request.end();
      });
    });

    step('presignedGetObject(bucketName, objectName, cb)_bucketName:' + bucketName + ', objectName:' + _1byteObjectName + '_', function (done) {
      client.presignedGetObject(bucketName, _1byteObjectName, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'GET';
        if (options.protocol === 'https:') transport = https;
        var request = transport.request(options, function (response) {
          if (response.statusCode !== 200) return done(new Error('error on put : ' + response.statusCode));
          var error = null;
          response.on('error', function (e) {
            return done(e);
          });
          response.on('end', function () {
            return done(error);
          });
          response.on('data', function (data) {
            if (data.toString() !== _1byte.toString()) {
              error = new Error('content mismatch');
            }
          });
        });
        request.on('error', function (e) {
          return done(e);
        });
        request.end();
      });
    });

    step('presignedGetObject(bucketName, objectName, expires)_bucketName:' + bucketName + ', objectName:this.does.not.exist, expires:2938_', function (done) {
      client.presignedGetObject(bucketName, 'this.does.not.exist', 2938).then(assert.fail)['catch'](function () {
        return done();
      });
    });

    step('presignedGetObject(bucketName, objectName, expires, respHeaders, cb)_bucketName:' + bucketName + ', objectName:' + _1byteObjectName + ', expires:1000_', function (done) {
      var respHeaders = {
        'response-content-type': 'text/html',
        'response-content-language': 'en',
        'response-expires': 'Sun, 07 Jun 2020 16:07:58 GMT',
        'response-cache-control': 'No-cache',
        'response-content-disposition': 'attachment; filename=testing.txt',
        'response-content-encoding': 'gzip'
      };
      client.presignedGetObject(bucketName, _1byteObjectName, 1000, respHeaders, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'GET';
        if (options.protocol === 'https:') transport = https;
        var request = transport.request(options, function (response) {
          if (response.statusCode !== 200) return done(new Error('error on get : ' + response.statusCode));
          if (respHeaders['response-content-type'] != response.headers['content-type']) {
            return done(new Error('content-type header mismatch'));
          }
          if (respHeaders['response-content-language'] != response.headers['content-language']) {
            return done(new Error('content-language header mismatch'));
          }
          if (respHeaders['response-expires'] != response.headers['expires']) {
            return done(new Error('expires header mismatch'));
          }
          if (respHeaders['response-cache-control'] != response.headers['cache-control']) {
            return done(new Error('cache-control header mismatch'));
          }
          if (respHeaders['response-content-disposition'] != response.headers['content-disposition']) {
            return done(new Error('content-disposition header mismatch'));
          }
          if (respHeaders['response-content-encoding'] != response.headers['content-encoding']) {
            return done(new Error('content-encoding header mismatch'));
          }
          response.on('data', function () {});
          done();
        });
        request.on('error', function (e) {
          return done(e);
        });
        request.end();
      });
    });

    step('presignedPostPolicy(postPolicy, cb)_postPolicy:expiresin10days_', function (done) {
      var policy = client.newPostPolicy();
      policy.setKey(_1byteObjectName);
      policy.setBucket(bucketName);
      var expires = new Date();
      expires.setSeconds(24 * 60 * 60 * 10);
      policy.setExpires(expires);

      client.presignedPostPolicy(policy, function (e, data) {
        if (e) return done(e);
        var req = superagent.post(data.postURL);
        _.each(data.formData, function (value, key) {
          return req.field(key, value);
        });
        req.attach('file', new Buffer([_1byte]), 'test');
        req.end(function (e) {
          if (e) return done(e);
          done();
        });
        req.on('error', function (e) {
          return done(e);
        });
      });
    });

    step('presignedPostPolicy(postPolicy)_postPolicy: null_', function (done) {
      client.presignedPostPolicy(null).then(function () {
        done(new Error('null policy should fail'));
      })['catch'](function () {
        return done();
      });
    });

    step('presignedUrl(httpMethod, bucketName, objectName, expires, reqParams, cb)_httpMethod:GET, bucketName:' + bucketName + ', expires:1000_', function (done) {
      client.presignedUrl('GET', bucketName, '', 1000, { 'prefix': 'data', 'max-keys': 1000 }, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'GET';
        options.headers = {};
        var str = '';
        if (options.protocol === 'https:') transport = https;
        var callback = function callback(response) {
          if (response.statusCode !== 200) return done(new Error('error on put : ' + response.statusCode));
          response.on('error', function (e) {
            return done(e);
          });
          response.on('end', function () {
            if (!str.match('<Key>' + _1byteObjectName + '</Key>')) {
              return done(new Error('Listed object does not match the object in the bucket!'));
            }
            done();
          });
          response.on('data', function (chunk) {
            str += chunk;
          });
        };
        var request = transport.request(options, callback);
        request.end();
      });
    });

    step('presignedUrl(httpMethod, bucketName, objectName, expires, cb)_httpMethod:DELETE, bucketName:' + bucketName + ', objectName:' + _1byteObjectName + ', expires:1000_', function (done) {
      client.presignedUrl('DELETE', bucketName, _1byteObjectName, 1000, function (e, presignedUrl) {
        if (e) return done(e);
        var transport = http;
        var options = _.pick(url.parse(presignedUrl), ['hostname', 'port', 'path', 'protocol']);
        options.method = 'DELETE';
        options.headers = {};
        if (options.protocol === 'https:') transport = https;
        var request = transport.request(options, function (response) {
          if (response.statusCode !== 204) return done(new Error('error on put : ' + response.statusCode));
          response.on('error', function (e) {
            return done(e);
          });
          response.on('end', function () {
            return done();
          });
          response.on('data', function () {});
        });
        request.on('error', function (e) {
          return done(e);
        });
        request.end();
      });
    });
  });

  describe('listObjects', function () {
    var listObjectPrefix = 'miniojsPrefix';
    var listObjectsNum = 10;
    var objArray = [];
    var listArray = [];
    var listPrefixArray = [];

    step('putObject(bucketName, objectName, stream, size, metaData, callback)_bucketName:' + bucketName + ', stream:1b, size:1_Create ' + listObjectsNum + ' objects', function (done) {
      _.times(listObjectsNum, function (i) {
        return objArray.push(listObjectPrefix + '.' + i);
      });
      objArray = objArray.sort();
      async.mapLimit(objArray, 20, function (objectName, cb) {
        return client.putObject(bucketName, objectName, readableStream(_1byte), _1byte.length, {}, cb);
      }, done);
    });

    step('listObjects(bucketName, prefix, recursive)_bucketName:' + bucketName + ', prefix: miniojsprefix, recursive:true_', function (done) {
      client.listObjects(bucketName, listObjectPrefix, true).on('error', done).on('end', function () {
        if (_.isEqual(objArray, listPrefixArray)) return done();
        return done(new Error('listObjects lists ' + listPrefixArray.length + ' objects, expected ' + listObjectsNum));
      }).on('data', function (data) {
        listPrefixArray.push(data.name);
      });
    });

    step('listObjects(bucketName, prefix, recursive)_recursive:true_', function (done) {
      try {
        client.listObjects("", "", true).on('end', function () {
          return done(new Error('listObjects should throw exception when empty bucketname is passed'));
        });
      } catch (e) {
        if (e.name == 'InvalidBucketNameError') {
          done();
        } else {
          done(e);
        }
      }
    });

    step('listObjects(bucketName, prefix, recursive)_bucketName:' + bucketName + ', recursive:false_', function (done) {
      client.listObjects(bucketName, '', false).on('error', done).on('end', function () {
        if (_.isEqual(objArray, listArray)) return done();
        return done(new Error('listObjects lists ' + listArray.length + ' objects, expected ' + listObjectsNum));
      }).on('data', function (data) {
        listArray.push(data.name);
      });
    });

    step('listObjectsV2(bucketName, prefix, recursive)_bucketName:' + bucketName + ', recursive:true_', function (done) {
      listArray = [];
      client.listObjectsV2(bucketName, '', true).on('error', done).on('end', function () {
        if (_.isEqual(objArray, listArray)) return done();
        return done(new Error('listObjects lists ' + listArray.length + ' objects, expected ' + listObjectsNum));
      }).on('data', function (data) {
        listArray.push(data.name);
      });
    });

    step('removeObject(bucketName, objectName, callback)_bucketName:' + bucketName + '_Remove ' + listObjectsNum + ' objects', function (done) {
      async.mapLimit(listArray, 20, function (objectName, cb) {
        return client.removeObject(bucketName, objectName, cb);
      }, done);
    });
  });

  function readableStream(data) {
    var s = new stream.Readable();
    s._read = function () {};
    s.push(data);
    s.push(null);
    return s;
  }

  describe('removeObjects', function () {
    var listObjectPrefix = 'miniojsPrefix';
    var listObjectsNum = 10;
    var objArray = [];
    var objectsList = [];

    step('putObject(bucketName, objectName, stream, size, contentType, callback)_bucketName:' + bucketName + ', stream:1b, size:1_Create ' + listObjectsNum + ' objects', function (done) {
      _.times(listObjectsNum, function (i) {
        return objArray.push(listObjectPrefix + '.' + i);
      });
      objArray = objArray.sort();
      async.mapLimit(objArray, 20, function (objectName, cb) {
        return client.putObject(bucketName, objectName, readableStream(_1byte), _1byte.length, '', cb);
      }, done);
    });

    step('listObjects(bucketName, prefix, recursive)_bucketName:' + bucketName + ', recursive:false_', function (done) {
      client.listObjects(bucketName, listObjectPrefix, false).on('error', done).on('end', function () {
        try {
          client.removeObjects(bucketName, '', function (e) {
            if (e) {
              done();
            }
          });
        } catch (e) {
          if (e.name === "InvalidArgumentError") {
            done();
          }
        }
      }).on('data', function (data) {
        objectsList.push(data.name);
      });
    });

    objectsList = [];

    step('listObjects(bucketName, prefix, recursive)_bucketName:' + bucketName + ', recursive:false_', function (done) {
      client.listObjects(bucketName, listObjectPrefix, false).on('error', done).on('end', function () {
        client.removeObjects(bucketName, objectsList, function (e) {
          if (e) {
            done(e);
          }
          done();
        });
      }).on('data', function (data) {
        objectsList.push(data.name);
      });
    });
  });

  describe('bucket notifications', function () {
    describe('#listenBucketNotification', function () {
      before(function () {
        // listenBucketNotification only works on Minio, so skip if
        // the host is Amazon.
        if (client.host.includes('s3.amazonaws.com')) {
          this.skip();
        }
      });

      step('listenBucketNotification(bucketName, prefix, suffix, events)_bucketName:' + bucketName + ', prefix:photos/, suffix:.jpg, events:bad_', function (done) {
        var poller = client.listenBucketNotification(bucketName, 'photos/', '.jpg', ['bad']);
        poller.on('error', function (error) {
          if (error.code != 'NotImplemented') {
            assert.match(error.message, /A specified event is not supported for notifications./);
            assert.equal(error.code, 'InvalidArgument');
          }
          done();
        });
      });
      step('listenBucketNotification(bucketName, prefix, suffix, events)_bucketName:' + bucketName + ', events: s3:ObjectCreated:*_', function (done) {
        var poller = client.listenBucketNotification(bucketName, '', '', ['s3:ObjectCreated:*']);
        var records = 0;
        var pollerError = null;
        poller.on('notification', function (record) {
          records++;

          assert.equal(record.eventName, 's3:ObjectCreated:Put');
          assert.equal(record.s3.bucket.name, bucketName);
          assert.equal(record.s3.object.key, objectName);
        });
        poller.on('error', function (error) {
          pollerError = error;
        });
        setTimeout(function () {
          // Give it some time for the notification to be setup.
          if (pollerError) {
            if (pollerError.code != 'NotImplemented') {
              done(pollerError);
            } else {
              done();
            }
            return;
          }
          client.putObject(bucketName, objectName, 'stringdata', function (err) {
            if (err) return done(err);
            setTimeout(function () {
              // Give it some time to get the notification.
              poller.stop();
              client.removeObject(bucketName, objectName, function (err) {
                if (err) return done(err);
                if (!records) return done(new Error('notification not received'));
                done();
              });
            }, 10 * 1000);
          });
        }, 10 * 1000);
      });

      // This test is very similar to that above, except it does not include
      // Minio.ObjectCreatedAll in the config. Thus, no events should be emitted.
      step('listenBucketNotification(bucketName, prefix, suffix, events)_bucketName:' + bucketName + ', events:s3:ObjectRemoved:*', function (done) {
        var poller = client.listenBucketNotification(bucketName, '', '', ['s3:ObjectRemoved:*']);
        poller.on('notification', assert.fail);
        poller.on('error', function (error) {
          if (error.code != 'NotImplemented') {
            done(error);
          }
        });

        client.putObject(bucketName, objectName, 'stringdata', function (err) {
          if (err) return done(err);
          // It polls every five seconds, so wait for two-ish polls, then end.
          setTimeout(function () {
            poller.stop();
            poller.removeAllListeners('notification');
            // clean up object now
            client.removeObject(bucketName, objectName, done);
          }, 11 * 1000);
        });
      });
    });
  });
});
//# sourceMappingURL=functional-tests.js.map

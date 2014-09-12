/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

var http = require('http');
var fs = require('fs');
var os = require('os');
var url = require('url');
var stream = require('./stream');

var log_version = "v1";
var config = {};
if (process.argv.length > 2) {
  // Attempt to read server config from the first argument
  try {
    config = require(process.argv[2]);
  } catch(e) {
    // TODO: catch malformed JSON separately.
    console.log(e);
    config = {};
  }
} else {
  config.motd = "Telemetry Collector (default configuration)";
}

// Make sure we can store the maxiumum lengths in the expected number of bytes:
var max_data_length = check_max(config.max_data_length || 200 * 1024, 4, "max_data_length");
var max_path_length = check_max(config.max_path_length || 10 * 1024, 2, "max_path_length");

// Even a full IPv6 address shouldn't be longer than this...
var max_ip_length = check_max(config.max_ip_length || 100, 1, "max_ip_length");

// NOTE: This is for logging actual telemetry submissions
var log_path = config.log_path || "./";
var log_base = config.log_base || "telemetry.log";

// TODO: URL Validation to ensure we're receiving dimensions
// ^/submit/telemetry/id/reason/appName/appUpdateChannel/appVersion/appBuildID$
// See http://mxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/TelemetryPing.jsm#783
var url_prefix = config.url_prefix || "/submit/telemetry/";
var url_prefix_len = url_prefix.length;
var include_ip = false;
if (config.include_request_ip) {
  log_version = "v2";
  include_ip = true;
}

var max_log_size = config.max_log_size || 500 * 1024 * 1024; // 500MB
var max_log_age_ms = config.max_log_age_ms || 5 * 60 * 1000; // 5 minutes in milliseconds

var log_file = unique_name(log_base);
var log_time = new Date().getTime();
var log_size = 0;

// We keep track of "last touched" and then rotate after current logs have
// been untouched for max_log_age_ms.
var timer = setInterval(function(){ rotate_time(); }, max_log_age_ms);

// NOTE: This is for logging request metadata (for monitoring and stats)
var request_log_file = config.stats_log_file || "/var/log/telemetry/telemetry-server.log";

var record_stream = null;
var out_stream = null;

// Ensure that the given value can be stored in the given number of bytes.
function check_max(value, num_bytes, label) {
  var max = Math.pow(2, num_bytes * 8) - 1;
  if (value > max) {
    console.log("Max supported value for '" + label + "' is " + max +
      " (you specified " + value + "). Using " + max + ".");
    return max;
  }
  return value;
}

function finish(code, request, response, msg, start_time, bytes_stored) {
  var duration = process.hrtime(start_time);
  var duration_ms = duration[0] * 1000 + duration[1] / 1000000;
  response.writeHead(code, {'Content-Type': 'text/plain'});
  response.end(msg);
  stat = {
    "url": request.url,
    "duration_ms": duration_ms,
    "code": code,
    "size": bytes_stored,
    "level": "info",
    "message": msg,
    "timestamp": new Date()
  };
  log_message = JSON.stringify(stat);
  // Don't want to do this synchronously, but it seems the most foolproof way.
  // The async version appears to leak FDs (resulting in EMFILE errors after a
  // while)
  // NOTE: if this is changed to use a persistent fd or stream, we need to
  //       listen for SIGHUP and close the log file so it can be rotated by
  //       logrotate
  fs.appendFileSync(request_log_file, log_message + "\n");
}

// Get the IP Address of the client. If we're receiving forwarded requests from
// a load balancer, use the appropriate header value instead.
function get_client_ip(request) {
  var client_ip = null;
  if (request.headers['x-forwarded-for']) {
    client_ip = request.headers['x-forwarded-for'];
  } else {
    client_ip = request.connection.remoteAddress;
  }
  return client_ip;
}

// We don't want to do this calculation within rotate() because it is also
// called when the log reaches the max size and we don't need to check both
// conditions (time and size) every time.
function rotate_time() {
  // Don't bother rotating empty log files (by time).
  if (log_size == 0) {
    return;
  }
  last_modified_age = new Date().getTime() - log_time;
  if (last_modified_age > max_log_age_ms) {
    console.log("Time to rotate " + log_file + " unmodified for " + last_modified_age + "ms");
    rotate();
  }
}

function get_output_stream() {
  // TODO: if output_type == S3, send to S3. else:
  return fs.createWriteStream(log_file);
}

function flush_log() {
  if (record_stream) {
    console.log("Flushing " + log_file);
    record_stream.end();
    //'', 'utf8', function(){
      if (log_size == 0) {
        console.log("Deleting empty file");
        // No data was written. Delete the file.
        fs.unlink(log_file, function (err) {
          if (err) {
            console.log('Error deleting empty file: ' + err);
            // TODO: throw err?
          } else {
            console.log('Deleted empty file instead of rotating: ' + log_file);
          }
        });
      } else {
        console.log("Rotating non-empty file after " + log_size + " bytes");
        // Some data was written. Rename the file.
        fs.rename(log_file, log_file + ".finished", function (err) {
          if (err) {
            console.log("Error rotating " + log_file + " (" + log_size + "): " + err);
          }
        });
      }
    //});
  }
}

function rotate() {
  console.log(new Date().toISOString() + ": Rotating " + log_file + " after " + log_size + " bytes");
  flush_log();

  // Start a new file whether the flush succeeded or not.
  log_file = unique_name(log_base);
  log_size = 0;

  // Set up fresh streams.
  record_stream = stream.RecordStream(log_version);
  out_stream = get_output_stream();
  console.log("piping output to " + log_file);
  record_stream.pipe(out_stream);
}

function unique_name(name) {
  // Could use UUID, but pid + timestamp should suffice and give more useful info.
  return log_path + "/" + name + "." + log_version + "." + os.hostname() + "." + process.pid + "." + new Date().getTime();
}

function getRequest(request, response, process_time, callback) {
  if (request.method != 'GET') {
    return finish(405, request, response, "Wrong request type", process_time, 0);
  }
  if (request.url == '/status') {
    callback();
    return;
  }
  return finish(404, request, response, "Not Found", process_time, 0);
}

function postRequest(request, response, process_time, callback) {
  var request_time = new Date().getTime();
  var data_length = parseInt(request.headers["content-length"]);
  if (!data_length) {
    return finish(411, request, response, "Missing content length", process_time, 0);
  }
  if (data_length > max_data_length) {
    // Note, the standard way to deal with "request too large" is to return
    // a HTTP Status 413, but we do not want clients to re-send large requests.
    return finish(202, request, response, "Request too large (" + data_length + " bytes). Limit is " + max_data_length + " bytes. Server will discard submission.", process_time, 0);
  }
  if (request.method != 'POST') {
    return finish(405, request, response, "Wrong request type", process_time, 0);
  }

  // Parse the url to strip off any query params.
  var url_parts = url.parse(request.url);
  var url_path = url_parts.pathname;
  // Make sure that url_path starts with the expected prefix, then chop that
  // off before storing.
  if (url_path.slice(0, url_prefix_len) != url_prefix) {
    return finish(404, request, response, "Not Found", process_time, 0);
  } else {
    // Strip off the un-interesting part of the path.
    url_path = url_path.slice(url_prefix_len);
  }
  var path_length = Buffer.byteLength(url_path);
  if (path_length > max_path_length) {
    // Similar to the content-length above, we would normally return 414, but
    // we don't want clients to retry these either.
    return finish(202, request, response, "Path too long (" + path_length + " bytes). Limit is " + max_path_length + " bytes", process_time, 0);
  }

  var client_ip = '';
  var client_ip_length = 0;
  if (include_ip) {
    client_ip = get_client_ip(request);
    client_ip_length = Buffer.byteLength(client_ip);
    if (client_ip_length > max_ip_length) {
      console.log("Received an excessively long ip address: " + client_ip_length + " > " + max_ip_length);
      client_ip = "0.0.0.0";
    }
  }

  // TODO: return the record length so we know when to rotate.
  var record_length = record_stream.writePreamble(client_ip, url_path, data_length, request_time, function(){
    console.log("Process " + process.pid + " wrote a preamble");
  });

  request.on('data', function(data) {
    // Write the data as it comes in:
    record_stream.write(data);
  });

  request.on('end', function() {
    console.log("Process " + process.pid + " finished writing a request.");
    log_size += record_length;
    log_time = request_time;
    // If length of outputfile is > max_log_size, start writing a new one.
    if (log_size > max_log_size) {
      rotate();
    }
    callback(record_length);
  });
}

function run_server(port) {
  rotate();
  var server = http.createServer(function(request, response) {
    var start_time = process.hrtime();
    if (request.method == "POST") {
      postRequest(request, response, start_time, function(bytes_written) {
        finish(200, request, response, 'OK', start_time, bytes_written);
      });
    } else {
      getRequest(request, response, start_time, function() {
        finish(200, request, response, 'OK', start_time, 0);
      });
    }
  });

  server.on('error', function(e){
    // Stop (with a message) if a server is already running.
    if (e.code == 'EADDRINUSE') {
      console.log("ERROR: There is already a server running on port " + port);
      cluster.worker.kill();
    } else {
      // Something unexpected. Pass it along.
      throw e;
    }
  });

  server.listen(port);
  console.log("Process " + process.pid + " Listening on port " + port);
}

var cluster = require('cluster');
var numCPUs = os.cpus().length;

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  if (config.motd) {
    console.log(config.motd);
  }

  cluster.on('exit', function(worker, code, signal) {
    if (worker.suicide) {
      // Worker killed itself on purpose. Don't fork another.
      console.log('Worker ' + worker.process.pid + ' suicided with code ' + code);
      process.exit(1);
    } else {
      // Start another one:
      console.log('Worker ' + worker.process.pid + ' died. Respawning.');
      cluster.fork();
    }
    // TODO: See how long the child actually stayed alive. We don't want to
    //       fork continuously, so if the child processes are dying right away
    //       we should abort the master (and have the server respawned
    //       externally).
  });

  cluster.on('disconnect', function(worker){
    console.log('Worker ' + worker.process.pid + ' disconnected');
  });
} else {
  // Finalize current log files on exit.
  process.on('exit', function() {
    console.log("Received exit message in pid " + process.pid);
    console.log("Finalizing log file:" + log_file);
    flush_log();
  });

  // Catch signals that break the main loop. Since they don't exit directly,
  // on('exit') will also be called.
  process.on('SIGTERM', function() {
    console.log("Received SIGTERM in pid " + process.pid);
  });
  process.on('SIGINT', function() {
    console.log("Received SIGINT in pid " + process.pid);
  });

  run_server(config.port || 8080);
}

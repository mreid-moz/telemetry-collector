var util = require('util');
var Transform = require('stream').Transform;
util.inherits(RecordStream, Transform);

// version must be one of:
//    "v1" - do not include client IPs
//    "v2" - include client IPs
function RecordStream(version, options) {
  if (!(this instanceof RecordStream))
    return new RecordStream(options);

  Transform.call(this, options);
  this._MAX_IP_LENGTH = 0xff;
  this._version = version;
  this._record = null;
  this._record_ptr = 0;
  this._record_length = 0;
  this._include_ip = (this._version === "v2");
}

// clientIPLength is ignored if this is a "v1" record.
RecordStream.prototype.writePreamble = function(client_ip, path, data_length, request_time, done) {
  var preamble_length = 15; // 1 sep + 2 path + 4 data + 8 timestamp
  var client_ip_length = 0;
  var path_length = Buffer.byteLength(path);
  if (this._include_ip) {
    preamble_length += 1; // length of client_ip
    client_ip_length = Buffer.byteLength(client_ip);
    if (client_ip_length > this._MAX_IP_LENGTH) {
      console.log("Received an excessively long ip address: " +
                  client_ip_length + " > " + this._MAX_IP_LENGTH);
      client_ip = "0.0.0.0";
      client_ip_length = Buffer.byteLength(client_ip);
    }
  }

  this._record_length = preamble_length +
                        client_ip_length +
                        path_length +
                        data_length;

  this._record = new Buffer(this._record_length);

  // Write the preamble so we can read the pieces back out:
  // 1 byte record separator 0x1e (so we can find our spot if we encounter a
  //   corrupted record)
  // [v2 only] 1 byte uint to indicate client ip address length
  // 2 byte uint to indicate path length
  // 4 byte uint to indicate data length
  // 8 byte uint to indicate request timestamp (epoch) split into two 4-byte
  //   writes
  this._record_ptr = 0;
  this._record.writeUInt8(0x1e, this._record_ptr);
  this._record_ptr += 1;
  if (this._include_ip) {
    this._record.writeUInt8(client_ip_length, this._record_ptr);
    this._record_ptr += 1;
  }
  this._record.writeUInt16LE(path_length, this._record_ptr);
  this._record_ptr += 2;
  this._record.writeUInt32LE(data_length, this._record_ptr);
  this._record_ptr += 4;

  // Blast the lack of 64 bit int support :(
  // Standard bitwise operations treat numbers as 32-bit integers, so we have
  // to do it the ugly way.  Note that Javascript can represent exact ints
  // up to 2^53 so timestamps are safe for approximately a bazillion years.
  // This produces the equivalent of a single little-endian 64-bit value (and
  // can be read back out that way by other code).
  this._record.writeUInt32LE(request_time % 0x100000000, this._record_ptr);
  this._record_ptr += 4;
  this._record.writeUInt32LE(Math.floor(request_time / 0x100000000), this._record_ptr);
  this._record_ptr += 4;

  if (this._record_ptr != preamble_length) {
    // TODO: assert
    console.log("ERROR: We should have written " + preamble_length +
                "preamble bytes, but we actually wrote " + this._record_ptr);
  }

  if (this._include_ip) {
    // Write the client ip address if need be:
    this._record.write(client_ip, this._record_ptr);
    this._record_ptr += client_ip_length;
  }
  // Now write the path:
  this._record.write(path, this._record_ptr);
  this._record_ptr += path_length;

  // Done writing preamble.
  done();

  return this._record_length;
}

RecordStream.prototype._transform = function(chunk, encoding, done) {
  if (chunk.length > 0) {
    if (!this._record) {
      this.emit('error', new Error('Cannot write record data before preamble'));
      return;
    }

    chunk.copy(this._record, this._record_ptr);
    this._record_ptr += chunk.length;

    if (this._record_ptr == this._record_length) {
      // Record is complete.
      this.push(this._record);
      this._record = null;
    } else if (this._record_ptr > this._record_length) {
      // wtf have you done?
      this.emit('error', new Error('You wrote too much to this record: ptr = ' + this._record_ptr + ", len = " + this._record_length));
      this._record = null;
      return;
    }
  }
  done();
};

module.exports.RecordStream = RecordStream;

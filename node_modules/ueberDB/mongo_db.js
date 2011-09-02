/* vim: set ts=2: */

var mongodb = require( 'mongodb' );
var async = require( 'async' );

exports.database = function( settings ) {
	this.connection = this.db = this.collection = null;
  
  if( !settings || !settings.host || !settings.port || !settings.db ) {
    settings = {
			'host'				: 'localhost',
			'port'				:	27017,
			'db'					: 'etherpad',
			'collection'	: 'etherpad'
		};
  }
  
  this.settings = settings;
  
  // taken from sqlite settings
  this.settings.cache = 0;
  this.settings.writeInterval = 0;
  this.settings.json = true;
}

exports.database.prototype.init = function( callback ) {
  var self = this;

	var serverSpec = new mongodb.Server( self.settings.host, self.settings.port, { 'auto_reconnect' : true } );

	var connection = new mongodb.Db( self.settings.db, serverSpec );

	// XXX: this could probably be cleaned up with async.waterfall

	connection.open( function( err, db ) {
		if( err ) {
			console.error( err );

			throw new Error( 'Database connection failed!' );
		} else {
			self.connection	= connection;
			self.db					= db;

			db.createCollection( self.settings.collection, function( err, collection ) {
				if( err ) {
					console.error( err );

					throw new Error( 'Failed to create collection!' );
				} else {
					self.collection = collection;

					self.collection.createIndex( 'key', { 'unique' : true }, function( err ) {
						if( err ) {
							console.error( err );

							throw new Error( 'Failed to create unique key index!' );
						} else {
							callback();
						}
					});
				}
			});
		}
	});
}

exports.database.prototype.get = function( key, callback ) {
	this.collection.findOne( { 'key' : key }, function( err, record ) {
		callback( err, record ? record.value : null );
	});
}

exports.database.prototype.set = function( key, value, callback ) {
	var record = {
		'key'	 	: key,
		'value'	: value
	};

	this.collection.update( { 'key' : key }, record, { 'upsert' : true, 'safe' : true }, function( err ) {
		callback( err );
	});
}

exports.database.prototype.remove = function( key, callback ) {
	this.collection.findAndModify( { 'key' : key }, [ [ '_id', 'asc' ] ], {}, { 'remove' : true }, function( err ) {
		callback( err );
	});
}

exports.database.prototype.doBulk = function( bulk, callback ) {
	var self = this;

	var inserts = [];
	var deletes = [];

	async.forEach(
		bulk,
		function( query, callback ) {
			switch( query.type ) {
				case 'set':
					inserts.push( {
						'key'		: query.key,
						'value'	: query.value
					});
				break;

				case 'remove':
					deletes.push( query.key );
				break;
			}

			callback();
		},

		function( err ) {
			if( inserts.length > 0 ) {
				self.collection.insert( inserts );
			}

			if( deletes.length > 0 ) {
				self.collection.findAndModify( { 'key' : { '$in' : deletes } }, [ [ '_id', 'asc' ] ], {}, { 'remove' : true } );	
			}

			callback();
		}
	);
}

exports.database.prototype.close = function( callback ) {
  this.db.close();
	this.connection.close();

  callback( null );
}

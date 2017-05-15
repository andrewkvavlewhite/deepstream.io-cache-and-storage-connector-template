'use strict'

const events = require( 'events' )
const util = require( 'util' )
const pckg = require( '../package.json' )
// const dataTransform = require( './transform-data' )
const neo4j = require('neo4j-driver').v1
var flatten = require('flat')
var unflatten = require('flat').unflatten
const _ = require('underscore')

/**
 * A template that can be forked to create cache or storage connectors
 * for [deepstream](http://deepstream.io)
 *
 * Cache connectors are classes that connect deepstream to an in-memory cache, e.g. Redis, Memcached,
 * IronCache or Amazon's elastic cache
 *
 * Storage connectors are classes that connect deepstream to a database, e.g. MongoDB, CouchDB, Cassandra or
 * Amazon's DynamoDB. They can also be used with relational databases, but deepstream's data-structures (blocks
 * of JSON, identified by a key) lends itself very well to object/document based databases.
 *
 * Whats this class used for?
 *
 * Both cache and storage connectors expose the same interface and offer similar functionality,
 * yet their role is a little bit different.
 *
 * Deepstream servers don't hold any data themselves. This allows the individual servers to remain
 * stateless and to go down / fail over without causing any data-loss, but it also allows for
 * the data to be distributed across multiple nodes.
 *
 * Whenever deepstream has to store something, its written to the cache in a blocking fashion, but written to
 * storage in a non blocking way. (Well, its NodeJS, so it's not really 'blocking', but the next callback for
 * this particular update won't be processed until the cache operation is complete)
 *
 * Similarly, whenever an entry needs to be retrieved, deepstream looks for it in the cache first and in storage
 * second. This means that the cache needs to be very fast - and fortunately most caches are. Both Redis and Memcached
 * have proven to be able to return queries within the same millisecond.
 *
 * So why have this distinction between cache and storage at all? Because they complement each other quite well:
 *
 * - Caches need to make a relatively small amount of data accessible at very high speeds. They achieve that by storing
 *   the data in memory, rather than on disk (although some, e.g. Redis, write to disk as well). This means that
 *   all data is lost when the process exists. Caches also usually don't offer support for elaborate querying.
 *
 * - Databases (storage) offer long-term storage of larger amounts of data and allow for more elaborate ways of querying.
 *   (full-text search, SQL etc.)
 *
 * Some considerations when implementing a cache/storage connector
 *
 * - this.isReady starts as false. Once the connection to the cache / storage is established, emit a 'ready' event and set
 *   it to true
 *
 * - Whenever a generic error occurs (e.g. an error that's not directly related to a get, set or delete operation, raise
 *   an error event and send the error message as a parameter, e.g. this.emit( 'error', 'connection lost' ) )
 *
 * - whenever an error occurs as part of a get, set or delete operation, pass it to the callback as the first argument,
 *   otherwise pass null
 *
 * - values for set() will be serializable JavaScript objects and are expected to be returned by get as such. It's
 *   therefor up to this class to handle serialisation / de-serialisation, e.g. as JSON or message-pack. Some
 *   systems (e.g. MongoDB) however can also handle raw JSON directly
 */


/*
 * @constructor
 */
var Connector = function( options ) {
  this.isReady = false
  this.name = pckg.name
  this.version = pckg.version
  this._defaultLabel = options.defaultLabel || 'DS_SCHEMA'
  this._splitChar = options.splitChar || null
  this._ds_key = options.ds_key || '_ds_key'
  
  if ( !options.connectionString ) {
    throw new Error( 'Missing setting \'connectionString\'' )
  }
  if ( !options.user ) {
    throw new Error( 'Missing setting \'user\'' )
  }
  if ( !options.password ) {
    throw new Error( 'Missing setting \'password\'' )
  }
  if ( this._ds_key === 'id' ) {
    throw new Error( 'Cannot use the key \'id\' as ds_key because it is used internally by neo4j' )
  }

  this._db = neo4j.driver(options.connectionString, neo4j.auth.basic(options.user, options.password))

  this._db.session()
    .run('return 1')
      .then(this._onCompleted.bind(this))
      .catch(this._onError.bind(this))
}


util.inherits( Connector, events.EventEmitter )


Connector.prototype.set = function( key, values, callback ) {
  let segments = this._getSegments( key )

  if( segments === null ) {
    callback( 'Invalid key ' + key )
    return
  }

  let query = this._getQuery('SET', segments, values)

  let session = this._db.session()
  session.run(query, values)
    .then(() => {
      callback( null )
      session.close()
    })
    .catch((err) => {
      console.log(err)
      callback( err )
      session.close()
    })

}


Connector.prototype.get = function( key, callback ) {
  let segments = this._getSegments( key )

  if( segments === null ) {
    callback( 'Invalid key ' + key )
    return
  }

  let query = this._getQuery('GET', segments)

  let session = this._db.session()
  session.run(query)
    .then((result) => {
      let record = result.records[0]
      if (record) {
        let normalized = record.toObject().value
        let value = _.object(normalized['keys'], normalized['values'])
        console.log(value)
        if (Object.keys(value).length === 0) {
          callback( null, null )
        } else {
          callback( null, value)
        }
      } else {
        callback( null, null )
      }
      session.close()
    })
    .catch((err) => {
      callback( err, null )
      session.close()
    })
}

Connector.prototype.delete = function( key, callback ) {
  let segments = this._getSegments( key )

  if( segments === null ) {
    callback( 'Invalid key ' + key )
    return
  }

  let query = this._getQuery('DELETE', segments)
  
  // let query = `MATCH (n:${segments[0]} { ${this._ds_key}: '${segments[1]}' })-[r]->(m)
  //               WHERE r.ds_schema = true
  //               DETACH DELETE n,m`

  let session = this._db.session()
  session.run(query)
    .then(() => {
      callback( null )
      session.close()
    })
    .catch((err) => {
      callback( err, null )
      session.close()
    })
}

Connector.prototype._onCompleted = function() {
  this.isReady = true
  this.emit( 'ready' )
}

Connector.prototype._onError = function(error) {
  this.emit( 'error', error )
  return
}

Connector.prototype._getSegments = function( key ) {
  let segments = key.split(this._splitChar)

  if( segments[0] === '' || segments[0] === this._defaultLabel ) 
    return null // TODO: separate and throw error

  for( let i in segments ) 
    segments[i] = (i % 2) ? segments[i] : segments[i].toUpperCase()
  
  if( segments.length === 1 ) 
    segments[0] = `${this._defaultLabel}:${segments[0]}`
    
  return segments
}

Connector.prototype._getQuery = function( action, segments, values={'default': true} ) {
  let query = ``

  if( segments.length < 3) {
    let set = (action === 'SET')

    query += `${set ?`MERGE` :`MATCH`} (${this._ds_key}_n:${segments[0]} { ${this._ds_key}: '${segments[1]}' })`

    for( let key in values ) {

      query += ` ${set ?`MERGE` :`MATCH`} (${ this._ds_key }_n)-[${ key }_r${ set ?`:${key}` :`` }]->(${ key }_m) 
                  ${ set ?`ON CREATE SET` :`WHERE` } ${ key }_r.ds_schema = true`

      if( action === 'SET' ) {
        query += ` SET ${ key }_m = $${ key }`
        continue
      }
      if( action === 'GET' ) {
        query += ` RETURN { keys: collect(type(${ key }_r)), values: collect(properties(${ key }_m))} AS value`
        break
      }
      if( action === 'DELETE' ) {
        query += ` DETACH DELETE ${ this._ds_key }_n,${ key }_m`
        break
      }

    }
  } else if( segments.length === 3 ) {
    query += `MATCH (n:${segements[0]} { ${this._ds_key}: '${segments[1]}' })
              `
  }
  return query
}

module.exports = Connector

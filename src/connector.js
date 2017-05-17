'use strict'

const events = require( 'events' )
const util = require( 'util' )
const pckg = require( '../package.json' )
// const dataTransform = require( './transform-data' )
const neo4j = require('neo4j-driver').v1
const dataTransform = require( './transform-data' )
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
  
  if ( !options.connectionString ) {
    throw new Error( 'Missing setting \'connectionString\'' )
  }
  if ( !options.userName ) {
    throw new Error( 'Missing setting \'user\'' )
  }
  if ( !options.password ) {
    throw new Error( 'Missing setting \'password\'' )
  }

  this._db = neo4j.driver(options.connectionString, neo4j.auth.basic(options.userName, options.password))

  this._db.session()
    .run('return 1')
      .then(this._onCompleted.bind(this))
      .catch(this._onError.bind(this))
}


util.inherits( Connector, events.EventEmitter )


Connector.prototype.set = function( key, value, callback ) {
  let segments = this._getSegments( key )

  if( segments === null ) {
    callback( 'Invalid key ' + key )
    return
  }
  
  value = dataTransform.transformValueForStorage( value )
  let query = this._getQuery('SET', segments, value)

  let session = this._db.session()
  session.run(query, value)
    .then(() => {
      callback( null )
      session.close()
    })
    .catch((err) => {
      // console.log(err)
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
        let value = record.toObject().value
        value._rels = _.object(value['_rel_keys'], value['_rel_vals'])
        delete value._rel_keys
        delete value._rel_vals
        value = dataTransform.transformValueFromStorage( value )
        delete value._key
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

Connector.prototype._getQuery = function( action, segments, values ) {
  let query = ``
  let label = segments[0]

  if( segments.length < 3 ) {
    let id = segments[1]

    query += (action === 'SET' ? `MERGE` : `MATCH`)

    query += ` (ds:__DS { _key: '${id}' })-[ds_r:__ds]->(n:${label}) `

    if( values ) {
      query += ` SET ds += $__ds `
      query += ` SET ds_r.schema = keys($_rels) `
      query += ` SET n += $_props `
      let rels = values._rels
      for( let rel in rels ) {
        label = rel.toUpperCase()

        query += `MERGE (n)-[:${ rel }]->(${ label }_m)<-[:__ds]-(${ label }_ds)
                  SET ${ label }_ds += $_rels.${ rel } `
                  // MERGE (${ label }_m)-[:__ds]->(${ label }_s) 
      }
    } else {
      query += `MATCH (n)-[r]->(m) `
      query += `WHERE type(r) IN ds_r.schema `
      if( action === 'DELETE' ) {
        query += `DETACH DELETE n, m`
      } else {
        query +=  `RETURN { __ds: properties(ds),
                            _props: properties(n),
                            _rel_keys: collect(type(r)), 
                            _rel_vals: collect(properties(m)) } AS value`
      }  
    }
  } 
  console.log(query)
  // else if( segments.length === 3 && values.__dsList ) {
  //   query += `MATCH (n:${ label } { _key: '${id}' })`
  // }
  return query
}

module.exports = Connector

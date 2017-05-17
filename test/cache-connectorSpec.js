'use strict'

/* global describe, expect, it, jasmine */
const expect = require('chai').expect
const CacheConnector = require('../src/connector')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const settings = { 
  connectionString: 'bolt://10.0.0.16',
  userName: 'neo4j',
  password: 'neo4j',
  splitChar: '/'
}
const MESSAGE_TIME = 20

describe( 'the message connector has the correct structure', () => {
  var cacheConnector

  it( 'throws an error if required connection parameters are missing', () => {
    expect( () => { new CacheConnector( 'gibberish' ) } ).to.throw()
  })

  it( 'creates the cacheConnector', ( done ) => {
    cacheConnector = new CacheConnector( settings )
    expect( cacheConnector.isReady ).to.equal( false )
    cacheConnector.on( 'ready', done )
  })

  it( 'implements the cache/storage connector interface', () =>  {
    expect( cacheConnector.name ).to.be.a( 'string' )
    expect( cacheConnector.version ).to.be.a( 'string' )
    expect( cacheConnector.get ).to.be.a( 'function' )
    expect( cacheConnector.set ).to.be.a( 'function' )
    expect( cacheConnector.delete ).to.be.a( 'function' )
    expect( cacheConnector instanceof EventEmitter ).to.equal( true )
  })

  it( 'parses keys', () => {
    var segments = cacheConnector._getSegments( 'users/a' )
    expect( segments[0] ).to.equal( 'USERS' )
    expect( segments[1] ).to.equal( 'a' )

    segments = cacheConnector._getSegments( 'bla' )
    expect( segments[0] ).to.equal( 'DS_SCHEMA:BLA' )
    // expect( segments[1] ).to.equal( 'bla' )

    segments = cacheConnector._getSegments( 'a/b/c' )
    expect( segments[0] ).to.equal( 'A' )
    expect( segments[1] ).to.equal( 'b' )
    expect( segments[2] ).to.deep.equal( 'C' )
  })

  it( 'refuses updates with invalid keys', () => {
    cacheConnector.set( '/a/b/c', {}, ( err ) => {
      expect( err ).to.equal( 'Invalid key /a/b/c' )
    })
  })

  it( 'retrieves a non existing node', ( done ) => {
    cacheConnector.get( 'USERS/123', ( error, value ) => {
      expect( error ).to.equal( null )
      expect( value ).to.equal( null )
      done()
    })
  })

  // it( 'sets a schema', ( done ) => {
  //   cacheConnector.set( 'USERS', {  
  //     _d: { v: 10 }, 
  //     schema: { 'firstname': false, 'uid': true, 'lastname': false, 'fb_id': true }, 
  //     // constraints : {
  //     //   unique: ['firstname', 'uid']
  //     // }
  //   }, ( error ) => {
  //     expect( error ).to.equal( null )
  //     done()
  //   })
  // })

  it( 'sets a node', ( done ) => {
    cacheConnector.set( 'USERS/123', {
      _d: { 
        _rels: {
          "friends": {
            _v: 0,
            _count: 0
          },
          "groups":{
            _v: 10,
            _count: 0
          },
          "events": {
            _v: 17,
            _count: 0
          }
        },
        "firstname": "John",
        "lastname": "Smith"
      },
      _v: 12
    }, ( error ) => {
      expect( error ).to.equal( null )
      done()
    })
  })

  it( 'retrieves an existing node', ( done ) => {
    cacheConnector.get( 'USERS/123', ( error, value ) => {
      expect( error ).to.equal( null )
      expect( value ).to.deep.equal({
        _d: { 
          _rels: {
            "friends": {
              _v: 0,
              _count: 0
            },
            "groups":{
              _v: 10,
              _count: 0
            },
            "events": {
              _v: 17,
              _count: 0
            }
          },
          "firstname": "John",
          "lastname": "Smith"
        },
        _v: 12
      } )
      done()
    })
  })

  // it( 'sets another node', ( done ) => {
  //   cacheConnector.set( 'GROUPS/456', {  _d: { v: 10 }, name: 'Indie Rock' }, ( error ) => {
  //     expect( error ).to.equal( null )
  //     done()
  //   })
  // })

  // it( 'links node\'s child nodes of specified relationship type', ( done ) => {
  //   cacheConnector.set( 'USERS/123/MEMBER_OF', {  _d: { v: 10 }, nodes: ['GROUPS/456'] }, ( error ) => {
  //     expect( error ).to.equal( null )
  //     done()
  //   })
  // })

  // it( 'deletes a node', ( done ) => {
  //   cacheConnector.delete( 'USERS/123', ( error ) => {
  //     expect( error ).to.equal( null )
  //     done()
  //   })
  // })

  // it( 'Can\'t retrieve a deleted node', ( done ) => {
  //   cacheConnector.get( 'USERS/123', ( error, value ) => {
  //     expect( error ).to.equal( null )
  //     expect( value ).to.equal( null )
  //     done()
  //   })
  // })

  // it( 'create muliple child nodes and a parent node', ( done ) => {
  //   async.parallel([
  //       function(callback) { 
  //         cacheConnector.set( 'USERS/_parent', { 
  //           // EVENTS: {last_changed: Date.now()}, 
  //           GROUPS: {last_changed: Date.now()}, 
  //           // FRIENDS: {last_changed: Date.now()} 
  //         }, ( error ) => {
  //           expect( error ).to.equal( null )
  //           callback()
  //         })
  //       },
  //       function(callback) { 
  //         cacheConnector.set( 'GROUPS/_child_1', { 
  //           EVENTS: {last_changed: Date.now()}
  //         }, ( error ) => {
  //           expect( error ).to.equal( null )
  //           callback()
  //         })
  //       },
  //       function(callback) { 
  //         cacheConnector.set( 'GROUPS/_child_2', { 
  //           EVENTS: {last_changed: Date.now()}
  //         }, ( error ) => {
  //           expect( error ).to.equal( null )
  //           callback()
  //         })
  //       },
  //       function(callback) { 
  //         cacheConnector.set( 'GROUPS/_child_3', { 
  //           EVENTS: {last_changed: Date.now()}
  //         }, ( error ) => {
  //           expect( error ).to.equal( null )
  //           callback()
  //         })
  //       },
  //   ], function() {
  //       done()
  //   });
    
  // })

  // it( 'sets relationships between parent and child nodes', ( done ) => {
  //   cacheConnector.set( 'USERS/_parent/GROUPS', [
  //     '_child_1',
  //     '_child_2',
  //     '_child_3'
  //   ], ( error ) => {
  //     expect( error ).to.equal( null )
  //     done()
  //   })
  // })

})
/*  global  describe,  expect,  it,  jasmine  */
'use strict'

const expect = require('chai').expect
const TransformData = require( '../src/transform-data' )

describe( 'Transforms outgoing data', () => {

  it( 'Transforms objects', () => {
    const result = TransformData.transformValueForStorage( {
      _d: { 
        _rels: {
          "friends": {
            _v: 0,
            _count: 0
          },
          "groups":{
            _v: 10,
            _count: 20
          },
          "events": {
            _v: 17,
            _count: 10
          }
        },
        "firstname": "John",
        "lastname": "Smith"
      },
      _v: 12
    } )
    expect( result ).to.deep.equal( {
      _props: {
        "firstname": "John",
        "lastname": "Smith"
      },
      _rels: {
        "friends": {
          _v: 0,
          _count: 0
        },
        "groups":{
          _v: 10,
          _count: 20
        },
        "events": {
          _v: 17,
          _count: 10
        }
      },
      __ds: {
        _v: 12
      }
    } )
  } )

  it( 'Transforms Lists', () => {
    const result = TransformData.transformValueForStorage( {
      _d: [ "John", "Smith" ],
      _v: 12
    } )
    expect( result ).to.deep.equal( {
      "__ds": {
        "_v": 12
      },
      "__dsList": [ "John", "Smith" ]
    } )
  } )
} )

describe( 'Transforms incoming data', () => {

  it( 'Transforms objects', () => {
    const result = TransformData.transformValueFromStorage( {
      _props: {
        "firstname": "John",
        "lastname": "Smith"
      },
      _rels: {
        "friends": {
          _v: 0,
          _count: 0
        },
        "groups":{
          _v: 10,
          _count: 20
        },
        "events": {
          _v: 17,
          _count: 10
        }
      },
      __ds: {
        _v: 12
      }
    } )
    expect( result ).to.deep.equal( {
      _d: { 
        _rels: {
          "friends": {
            _v: 0,
            _count: 0
          },
          "groups":{
            _v: 10,
            _count: 20
          },
          "events": {
            _v: 17,
            _count: 10
          }
        },
        "firstname": "John",
        "lastname": "Smith"
      },
      _v: 12
    } )
  } )

  it( 'Transforms Lists', () => {
    const result = TransformData.transformValueFromStorage( {
      "__ds": {
        "_v": 12
      },
      "__dsList": [ "John", "Smith" ]
    } )
    expect( result ).to.deep.equal( {
      _d: [ "John", "Smith" ],
      _v: 12
    } )
  } )
} )
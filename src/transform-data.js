"use strict"
// var flatten = require('flat')
// var unflatten = require('flat').unflatten

/**
 * This method is for the storage connector, to allow queries to happen more naturally
 * do not use in cache connectors
 *
 * Inverts the data from the deepstream structure to reduce nesting.
 *
 * { _v: 1, _d: { name: 'elasticsearch' } } -> { name: 'elasticsearch', __ds = { _v: 1 } }
 *
 * @param  {String} value The data to save
 *
 * @private
 * @returns {Object} data
 */
module.exports.transformValueForStorage = function ( value ) {
  value = JSON.parse( JSON.stringify( value ) )

  var data = value._d
  delete value._d

  if( data instanceof Array ) {
    data = {
      __dsList: data,
      __ds: value
    }
  } else {
    var rels = data._rels
    delete data._rels
    data = {
      _props: data,
      _rels: rels,
      __ds: value
    }
    console.log(data)
  }

  return data
}

/**
 * This method is for the storage connector, to allow queries to happen more naturally
 * do not use in cache connectors
 *
 * Inverts the data from the stored structure back to the deepstream structure
 *
 * { name: 'elasticsearch', __ds = { _v: 1 } } -> { _v: 1, _d: { name: 'elasticsearch' } }
 *
 * @param  {String} value The data to transform
 *
 * @private
 * @returns {Object} data
 */
module.exports.transformValueFromStorage = function( value ) {
  value = JSON.parse( JSON.stringify( value ) )
  if( !value ) return undefined

  var data = value.__ds
  delete value.__ds

  if( value.__dsList instanceof Array ) {
    data._d = value.__dsList
  } else {
    data._d = value._props
    if( value._props ) data._d._rels = value._rels
    delete value._rels
  }

  return data
}
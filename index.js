import blockIterator from 'block-iterator'

export async function * chunkStoreRead (store, opts = {}) {
  if (store?.[Symbol.asyncIterator]) {
    yield * store[Symbol.asyncIterator](opts.offset)
    return
  }
  if (!store?.get) throw new Error('First argument must be an abstract-chunk-store compliant store')

  const chunkLength = opts.chunkLength || store.chunkLength
  if (!chunkLength) throw new Error('missing required `chunkLength` property')

  const length = opts.length || store.length
  if (!Number.isFinite(length)) throw new Error('missing required `length` property')

  const offset = opts.offset || 0
  let index = Math.floor(offset / chunkLength)
  if (offset) {
    const chunkOffset = offset % chunkLength
    yield new Promise((resolve, reject) => {
      store.get(index++, { offset, length: chunkLength - chunkOffset }, (err, chunk) => {
        if (err) reject(err)
        resolve(chunk)
      })
    })
  }
  for (; index * chunkLength <= length; ++index) {
    yield new Promise((resolve, reject) => {
      store.get(index, (err, chunk) => {
        if (err) reject(err)
        resolve(chunk)
      })
    })
  }
}

export async function chunkStoreWrite (store, stream, opts = {}) {
  if (!store?.put) throw new Error('First argument must be an abstract-chunk-store compliant store')

  const chunkLength = opts.chunkLength || store.chunkLength
  if (!chunkLength) throw new Error('missing required `chunkLength` property')

  const storeMaxOutstandingPuts = opts.storeMaxOutstandingPuts || 16
  let outstandingPuts = 0

  let index = 0

  for await (const chunk of blockIterator(stream, chunkLength, opts)) {
    await new Promise(resolve => {
      if (outstandingPuts++ <= storeMaxOutstandingPuts) resolve()
      store.put(index++, chunk, err => {
        if (err) throw err
        --outstandingPuts
        resolve()
      })
    })
  }
}

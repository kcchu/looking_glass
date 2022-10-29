import fs from 'node:fs/promises'

import * as fastq from "fastq"
import * as dotenv from 'dotenv'
import Web3 from 'web3'
import got from 'got'

const fetchConcurrency = 200
const fetchBacklog = 2000
const fetchBackoffTime = 10
const pageSize = 2000

dotenv.config()


main()

async function main() {
  const abiJSON = await fs.readFile('./abi/LensHub.json')
  const abi = JSON.parse(abiJSON)
  const web3 = new Web3(process.env['PROVIDER_ENDPOINT'])
  const hub = new web3.eth.Contract(abi, process.env['LENS_HUB_ADDRESS'])
  const fromBlock = parseInt(process.env['LENS_FROM_BLOCK'], 10)
  const toBlock = await web3.eth.getBlockNumber()

  const outputPath = process.env['OUTPUT_PATH'] ?? 'output.jsonl'
  const f = await fs.open(outputPath, 'w')
  const fetchq = fastq.promise(processEvent, fetchConcurrency)
  const collectq = fastq.promise(collectWorker(f), 1)
  fetchq.error(err => { if (err) console.error(err) })
  collectq.error(err => { if (err) console.error(err) })

  for (let i = fromBlock; i < toBlock; i += pageSize) {
    const to = i + pageSize - 1
    console.debug(`Scanning block ${i}-${to}`)
    try {
      const events = await hub.getPastEvents('allEvents', {
        fromBlock: i,
        toBlock: to
      })
      events.filter(ev => !!ev.event).map(fetchq.push).forEach(collectq.push)

      while (fetchq.length() >= fetchBacklog) {
        console.debug(`Pause scanning for ${fetchBackoffTime}s (fetchq length ${fetchq.length()})`)
        await new Promise(r => setTimeout(r, fetchBackoffTime * 1000))
      }
    } catch (e) {
      console.error(`Failed to scan block ${i}-${to}`)
    }
  }

  console.info(`Waiting ${workq.length()} tasks to finish`)
  await workq.drained()
}

async function processEvent(ev) {
  if (!ev.event) {
    // unknown event
    return
  }
  const values = Object.fromEntries(Object.entries(ev.returnValues).filter(([key]) => isNaN(key)))
  let rec = {
      blockNumber: ev.blockNumber,
      transactionHash: ev.transactionHash,
      transactionIndex: ev.transactionIndex,
      event: ev.event,
      ...values
  }
  if (ev.returnValues.contentURI) {
    const uri = fixContentURI(ev.returnValues.contentURI)
    try {
      const content = await got(uri).json()
      rec = {
        ...rec,
        ...content
      }
    } catch (e) {
      console.error(`Failed to fetch ${uri}: ${e}`)
      rec = {
        ...rec,
        fetchFailed: true
      }
    }
  }
  return rec
}

function collectWorker(f) {
  return async (promise) => {
    const rec = await promise
    f.write(JSON.stringify(rec) + "\n")
  }
}

function fixContentURI(s) {
  return s
    .replace('ipfs://', 'https://cf-ipfs.com/ipfs/')
    .replace('https://ipfs.infura.io', 'https://infura-ipfs.io')
}
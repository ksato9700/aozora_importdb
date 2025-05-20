const fetch = require('node-fetch'); // For making HTTP requests
const cheerio = require('cheerio');   // For parsing HTML (jQuery-like syntax)

require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

// LIST_URL_BASE and LISTFILE_INP/LIST_URL_PUB are not used in pid2db.js,
// but keeping them here for consistency with the previous file if needed elsewhere.
const LIST_URL_BASE = 'https://github.com/aozorabunko/aozorabunko/raw/master/index_pages/';
const LISTFILE_INP = 'list_inp_person_all_utf8.zip';
const LIST_URL_PUB = 'list_person_all_extended_utf8.zip';


// Replaced scraperjs.StaticScraper.create with a custom async function using node-fetch and cheerio
const scrape_url = async (idurl) => {
  try {
    const response = await fetch(idurl);

    if (!response.ok) {
      throw new Error(`Failed to fetch ${idurl}: ${response.statusText}`);
    }

    const html = await response.text(); // Get the HTML content as text
    const $ = cheerio.load(html);       // Load HTML into Cheerio for parsing

    const items = $("tr[valign]").map(function() {
      return {
        id: $(this).find(':nth-child(1)').text().trim(),
        name: $(this).find(':nth-child(2)').text().trim().replace('　', ' ')
      };
    }).get(); // .get() converts Cheerio object to a plain array

    // As per original code, slice from the second element to skip header or irrelevant first row
    return items.slice(1);

  } catch (error) {
    console.error(`Error scraping URL ${idurl}:`, error);
    throw error; // Re-throw the error to be caught by the calling function
  }
};

const idurls = {
  // 'persons': 'http://reception.aozora.gr.jp/pidlist.php?page=1&pagerow=-1',
  'workers': 'http://reception.aozora.gr.jp/widlist.php?page=1&pagerow=-1'
};

const run = async () => {
  let client;
  try {
    // Use recommended options for MongoClient.connect
    client = await mongodb.MongoClient.connect(mongo_url, { useNewUrlParser: true, useUnifiedTopology: true });
    const db = client.db('aozora'); // Explicitly get the database instance

    for(let idname in idurls) {
      const idurl = idurls[idname];
      const scrapedItems = await scrape_url(idurl); // Await the result of the scraping

      const bulk_ops = scrapedItems.map((item) => {
        item.id = parseInt(item.id); // Ensure ID is an integer
        return {
          updateOne: {
            filter: {id: item.id},
            update: item,
            upsert: true
          }
        };
      });

      if (bulk_ops.length > 0) {
        console.log(`Updating ${bulk_ops.length} entries for collection '${idname}'`);
        await db.collection(idname).bulkWrite(bulk_ops);
      } else {
        console.log(`No entries to update for collection '${idname}'`);
      }
    }
  } catch (err) {
    console.error('An error occurred during the import process:', err);
  } finally {
    if (client) {
      client.close(); // Ensure MongoDB client is closed
    }
  }
};

run();
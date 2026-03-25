const { ApifyClient } = require('apify-client');
const { createObjectCsvWriter } = require('csv-writer');
const fs = require('fs');
const path = require('path');

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) {
  console.error('Error: APIFY_TOKEN environment variable is not set.');
  console.error('Run: export APIFY_TOKEN=your_token_here');
  process.exit(1);
}

// Edit these to target specific cities/states
const LOCATIONS = [
  'New York, NY',
  'Los Angeles, CA',
  'Miami, FL',
  'Houston, TX',
  'Chicago, IL',
  'Phoenix, AZ',
  'Dallas, TX',
  'Atlanta, GA',
  'Las Vegas, NV',
  'Austin, TX',
];

const SEARCH_TERM = 'med spa';
const OUTPUT_DIR = path.join(__dirname, 'output');
// ──────────────────────────────────────────────────────────────────────────────

const client = new ApifyClient({ token: APIFY_TOKEN });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizePhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+\-() ]/g, '').trim();
}

function cleanUrl(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.origin + u.pathname;
  } catch {
    return url;
  }
}

function mergeDedupe(existing, incoming) {
  const map = new Map();
  for (const lead of existing) {
    const key = (lead.businessName || '').toLowerCase().trim();
    if (key) map.set(key, lead);
  }
  for (const lead of incoming) {
    const key = (lead.businessName || '').toLowerCase().trim();
    if (!key) continue;
    if (map.has(key)) {
      // Merge — fill in missing fields
      const existing = map.get(key);
      for (const [k, v] of Object.entries(lead)) {
        if (!existing[k] && v) existing[k] = v;
      }
    } else {
      map.set(key, lead);
    }
  }
  return Array.from(map.values());
}

// ─── SOURCE 1: Google Maps ────────────────────────────────────────────────────
async function scrapeGoogleMaps() {
  console.log('\n[1/3] Scraping Google Maps...');

  const queries = LOCATIONS.map((loc) => `${SEARCH_TERM} near ${loc}`);

  const run = await client.actor('compass/crawler-google-places').call({
    searchStringsArray: queries,
    maxCrawledPlacesPerSearch: 20,
    language: 'en',
    exportPlaceUrls: false,
    includeHistogram: false,
    includeOpeningHours: true,
    includePeopleAlsoSearch: false,
    maxImages: 0,
    maxReviews: 0,
    scrapeDirectories: false,
  });

  console.log(`   Google Maps run ID: ${run.id} — waiting for results...`);
  await client.run(run.id).waitForFinish();

  const { items } = await client.dataset(run.defaultDatasetId).listItems();
  console.log(`   Found ${items.length} results from Google Maps`);

  return items.map((p) => ({
    source: 'Google Maps',
    businessName: p.title || '',
    category: (p.categoryName || p.categories?.[0] || '').trim(),
    address: p.address || '',
    city: p.city || '',
    state: p.state || '',
    zip: p.postalCode || '',
    country: p.countryCode || 'US',
    phone: normalizePhone(p.phone),
    website: cleanUrl(p.website),
    email: p.email || '',
    googleMapsUrl: p.url || '',
    rating: p.totalScore || '',
    reviewCount: p.reviewsCount || '',
    hours: p.openingHours ? p.openingHours.map((h) => `${h.day}: ${h.hours}`).join(' | ') : '',
    description: p.description || '',
    plusCode: p.plusCode || '',
    temporarilyClosed: p.temporarilyClosed || false,
    permanentlyClosed: p.permanentlyClosed || false,
  }));
}

// ─── SOURCE 2: Yellow Pages ───────────────────────────────────────────────────
async function scrapeYellowPages() {
  console.log('\n[2/3] Scraping Yellow Pages...');

  const allLeads = [];

  for (const location of LOCATIONS) {
    const [city, state] = location.split(', ');
    try {
      const run = await client.actor('petr_cermak/yellow-pages-scraper').call({
        search: SEARCH_TERM,
        location: location,
        maxItems: 30,
      });

      console.log(`   YP run for ${location}: ${run.id} — waiting...`);
      await client.run(run.id).waitForFinish();

      const { items } = await client.dataset(run.defaultDatasetId).listItems();
      console.log(`   ${location}: ${items.length} results`);

      for (const p of items) {
        allLeads.push({
          source: 'Yellow Pages',
          businessName: p.name || p.businessName || '',
          category: p.categories?.[0] || p.category || '',
          address: p.street || p.address || '',
          city: p.city || city || '',
          state: p.state || state || '',
          zip: p.zipCode || p.zip || '',
          country: 'US',
          phone: normalizePhone(p.phone),
          website: cleanUrl(p.website || p.websiteUrl),
          email: p.email || '',
          googleMapsUrl: '',
          rating: p.rating || '',
          reviewCount: p.reviewCount || '',
          hours: p.hours || '',
          description: p.description || p.tagline || '',
          plusCode: '',
          temporarilyClosed: false,
          permanentlyClosed: false,
        });
      }

      await sleep(1500); // Be polite between runs
    } catch (err) {
      console.warn(`   Warning: Yellow Pages scrape failed for ${location}: ${err.message}`);
    }
  }

  console.log(`   Total from Yellow Pages: ${allLeads.length}`);
  return allLeads;
}

// ─── SOURCE 3: LinkedIn ───────────────────────────────────────────────────────
async function scrapeLinkedIn() {
  console.log('\n[3/3] Scraping LinkedIn Companies...');

  try {
    const run = await client.actor('curious_coder/linkedin-company-scraper').call({
      queries: LOCATIONS.map((loc) => `${SEARCH_TERM} ${loc}`),
      maxResults: 100,
      proxy: { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
    });

    console.log(`   LinkedIn run ID: ${run.id} — waiting...`);
    await client.run(run.id).waitForFinish();

    const { items } = await client.dataset(run.defaultDatasetId).listItems();
    console.log(`   Found ${items.length} results from LinkedIn`);

    return items.map((p) => ({
      source: 'LinkedIn',
      businessName: p.name || p.companyName || '',
      category: p.industries?.[0] || p.industry || '',
      address: p.headquarters || p.address || '',
      city: p.city || '',
      state: p.state || '',
      zip: '',
      country: p.country || 'US',
      phone: normalizePhone(p.phone),
      website: cleanUrl(p.website || p.websiteUrl),
      email: p.email || '',
      googleMapsUrl: '',
      rating: '',
      reviewCount: p.followerCount || '',
      hours: '',
      description: p.description || p.tagline || '',
      plusCode: '',
      linkedInUrl: p.linkedInUrl || p.url || '',
      employeeCount: p.employeeCount || p.staffCount || '',
      founded: p.foundedYear || p.founded || '',
      temporarilyClosed: false,
      permanentlyClosed: false,
    }));
  } catch (err) {
    console.warn(`   Warning: LinkedIn scrape failed: ${err.message}`);
    return [];
  }
}

// ─── OUTPUT ───────────────────────────────────────────────────────────────────
async function saveResults(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const csvPath = path.join(OUTPUT_DIR, `med-spa-leads-${timestamp}.csv`);
  const jsonPath = path.join(OUTPUT_DIR, `med-spa-leads-${timestamp}.json`);

  // Save JSON
  fs.writeFileSync(jsonPath, JSON.stringify(leads, null, 2));

  // Save CSV
  const csvWriter = createObjectCsvWriter({
    path: csvPath,
    header: [
      { id: 'businessName', title: 'Business Name' },
      { id: 'source', title: 'Source' },
      { id: 'category', title: 'Category' },
      { id: 'phone', title: 'Phone' },
      { id: 'email', title: 'Email' },
      { id: 'website', title: 'Website' },
      { id: 'address', title: 'Address' },
      { id: 'city', title: 'City' },
      { id: 'state', title: 'State' },
      { id: 'zip', title: 'ZIP' },
      { id: 'rating', title: 'Rating' },
      { id: 'reviewCount', title: 'Review Count' },
      { id: 'hours', title: 'Hours' },
      { id: 'description', title: 'Description' },
      { id: 'googleMapsUrl', title: 'Google Maps URL' },
      { id: 'linkedInUrl', title: 'LinkedIn URL' },
      { id: 'employeeCount', title: 'Employee Count' },
      { id: 'founded', title: 'Founded' },
    ],
  });

  await csvWriter.writeRecords(leads);

  console.log(`\n✓ Saved ${leads.length} leads`);
  console.log(`  CSV  → ${csvPath}`);
  console.log(`  JSON → ${jsonPath}`);
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== Med Spa Lead Scraper ===');
  console.log(`Targeting: "${SEARCH_TERM}" across ${LOCATIONS.length} locations`);
  console.log(`Sources: Google Maps, Yellow Pages, LinkedIn\n`);

  let leads = [];

  // Run all three sources
  const [googleLeads, ypLeads, linkedInLeads] = await Promise.allSettled([
    scrapeGoogleMaps(),
    scrapeYellowPages(),
    scrapeLinkedIn(),
  ]);

  if (googleLeads.status === 'fulfilled') leads = mergeDedupe(leads, googleLeads.value);
  else console.warn('Google Maps failed:', googleLeads.reason?.message);

  if (ypLeads.status === 'fulfilled') leads = mergeDedupe(leads, ypLeads.value);
  else console.warn('Yellow Pages failed:', ypLeads.reason?.message);

  if (linkedInLeads.status === 'fulfilled') leads = mergeDedupe(leads, linkedInLeads.value);
  else console.warn('LinkedIn failed:', linkedInLeads.reason?.message);

  // Filter out permanently closed
  leads = leads.filter((l) => !l.permanentlyClosed);

  console.log(`\n=== Summary ===`);
  console.log(`Total unique leads after deduplication: ${leads.length}`);

  await saveResults(leads);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

"""
Texas auction tracker - CLOUD edition (runs on GitHub Actions, free, no AI, no paid API)

  * Finds the Texas properties ITSELF from each website (no spreadsheet needed):
        Auction.com, Xome, ServiceLink, Hubzu   (+ any site you add to EXTRA_SITES below)
  * Watches every property with a real (headless) browser, so bids and countdowns are read exactly as on the site.
  * Remembers the LAST BID SEEN WHILE LIVE, re-checks after the auction ends (+15 min, +2 h) and records the FINAL bid.
  * Writes results.csv / cloud_log.csv (committed to the repo every 2 min); your Google Sheet pulls them in.
  * Soft-close safe: if a late bid extends the auction it simply keeps watching.
"""
import asyncio, csv, json, os, re, sys, time, tempfile, urllib.request, datetime as dt
from playwright.async_api import async_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_F = os.path.join(HERE, 'tracker_state.json')
LISTINGS_F = os.path.join(HERE, 'listings.json')
LOG_F = os.path.join(HERE, 'tracker_log.csv')          # not committed (see .gitignore)
SNAP_D = os.path.join(HERE, 'snapshots')               # not committed
CONCURRENCY = int(os.environ.get('TRACKER_CONCURRENCY', '6'))
MAX_RUN = int(os.environ.get('TRACKER_MAX_SECONDS', '0'))        # 0 = run forever
DISCOVER_EVERY = int(os.environ.get('TRACKER_DISCOVER_SECONDS', '10800'))   # look for NEW properties every 3 h
KEEP_FINAL_DAYS = 2                                    # finished listings leave the watch list after this (they stay in the Sheet)
T0 = time.time()


# ---------------------------------------------------------------- parsing
PRICE_RE = re.compile(
    r'(Current\s+Highest\s+Bid|Current\s+Bid|Highest\s+Bid|High\s+Bid|Winning\s+Bid|Final\s+Bid|'
    r'Sold\s+Price|Sale\s+Price|Sold\s+For|Final\s+Price|Closing\s+Bid|Current\s+Price)\s*:?\s*\$\s*([\d,]+(?:\.\d\d)?)',
    re.I)
STRONG_END = ['auction has ended', 'auction ended', 'bidding has ended', 'bidding ended', 'bidding closed',
              'auction closed', 'auction is closed', 'under review', 'has been sold', 'sold to',
              'auction complete', 'bidding is closed', 'this auction is over', 'in contracting', 'property is in contracting']
WEAK_END = ['sale pending', 'pending', 'under contract', 'cancelled', 'canceled', 'withdrawn',
            'no longer available']
LIVE_HINTS = ['bidding ends in', 'time left', 'ends in', 'closes in', 'place bid', 'now bidding', 'place a bid']
CUT_MARKERS = ['Recommended Properties', 'Similar Properties', 'Nearby Properties', 'You May Also Like',
               'Top FAQs', 'Buying with Hubzu']
COUNTDOWN_LABELS = re.compile(r'(Bidding\s+Ends\s+In|Time\s+Left|Ends\s+In|Closes\s+In|Auction\s+Ends\s+In|Time\s+Remaining)\s*:?\s*',
                              re.I)


def cut(text):
    cutpos = len(text)
    for m in CUT_MARKERS:
        i = text.find(m)
        if i != -1:
            cutpos = min(cutpos, i)
    return text[:cutpos]


def parse_duration(s):
    """'27m 2s', '01 Days : 07 Hours : 56 Mins', '2:03:11' -> seconds (None if nothing found)."""
    s = s.strip()
    m = re.fullmatch(r'(\d+):(\d\d):(\d\d)', s)
    if m:
        return int(m[1]) * 3600 + int(m[2]) * 60 + int(m[3])
    tot, found = 0, False
    for num, unit in re.findall(r'(\d+)\s*(d(?:ays?)?|h(?:ours?|rs?)?|m(?:ins?|inutes?)?|s(?:ecs?|econds?)?)(?![a-z])', s, re.I):
        u = unit[0].lower()
        tot += int(num) * {'d': 86400, 'h': 3600, 'm': 60, 's': 1}[u]
        found = True
    return tot if found else None


def find_countdown(text):
    m = re.search(r'(?:AUCTION\s+)?ENDS\s+IN\s*(\d+)\s*Days?\s*:?\s*(\d+)\s*Hours?\s*:?\s*(\d+)\s*Mins?\s*:?\s*(\d+)\s*Secs?', text, re.I)
    if m:
        return int(m[1]) * 86400 + int(m[2]) * 3600 + int(m[3]) * 60 + int(m[4])
    m = COUNTDOWN_LABELS.search(text)
    if m:
        tail = text[m.end():m.end() + 90]
        tail = ' '.join(tail.split('\n')[:4])
        return parse_duration(tail)
    # Hubzu style: "01 Days : 07 Hours : 56 Mins" with no label
    m = re.search(r'(\d+)\s*Days?\s*:?\s*(\d+)\s*Hours?\s*:?\s*(\d+)\s*Mins?', text, re.I)
    if m:
        return int(m[1]) * 86400 + int(m[2]) * 3600 + int(m[3]) * 60
    return None


def parse_page(raw):
    text = cut(raw)
    low = text.lower()
    pm = PRICE_RE.search(text)
    price = float(pm.group(2).replace(',', '')) if pm else None
    price_label = pm.group(1) if pm else None
    if price is None:
        pm2 = re.search(r'\$\s*([\d,]+(?:\.\d\d)?)\s*\n+\s*(Current\s+Highest\s+Bid|Current\s+Bid|High\s+Bid|Winning\s+Bid|Final\s+Bid)', text, re.I)
        if pm2:
            price = float(pm2.group(1).replace(',', '')); price_label = pm2.group(2)
    cd = find_countdown(text)
    strong = [p for p in STRONG_END if p in low]
    weak = [p for p in WEAK_END if p in low]
    ended_hits = strong + weak
    live = cd is not None or any(h in low for h in LIVE_HINTS)
    if cd is not None and cd > 0:
        status = 'LIVE'
    elif strong:
        status = 'ENDED'
    elif weak and not live:
        status = 'ENDED'
    elif live:
        status = 'LIVE'
    else:
        status = 'UNKNOWN'
    start_cd = None
    sm = re.search(r'(Auction\s+Starts\s+In|Bidding\s+Starts\s+In|Starts\s+In|Starting\s+In)\s*:?\s*', text, re.I)
    if sm and cd is None:
        start_cd = parse_duration(' '.join(text[sm.end():sm.end() + 60].split('\n')[:3]))
        if start_cd is not None and not strong:
            status = 'UPCOMING'; cd = start_cd
    reserve = None
    if re.search(r'reserve\s+not\s+met', low): reserve = 'Not met'
    elif re.search(r'reserve\s+met', low): reserve = 'Met'
    elif re.search(r'no\s+reserve', low): reserve = 'No reserve'
    return dict(price=price, price_label=price_label, countdown=cd, status=status,
                reserve=reserve, ended_words=','.join(ended_hits[:3]))


def next_delay(info):
    """seconds until the next look at a LIVE listing"""
    r = info.get('countdown')
    if r is None:       return 600
    if info.get('status') == 'UPCOMING':
        return int(min(21600, max(900, r * 0.5)))
    if r <= 120:        return 20
    if r <= 600:        return 30
    if r <= 14400:      return 60          # anything ending within 4 hours: look EVERY MINUTE
    if r <= 43200:      return 300
    return min(3600, max(600, r - 43200))




# ---------------------------------------------------------------- listings / state
S = {'_v': 2}    # per-listing memory
L = []            # all listings being watched
KNOWN = set()     # ids already in L
LOGBUF = []       # rows waiting to be sent to the Sheet log
_LAST_LOG = {}    # id -> (status, bid) so only CHANGES go to the Sheet log


def slug_to_address(url):
    tail = url.split('?')[0].rstrip('/').split('/')[-1]
    tail = re.sub(r'^\d{6,}-', '', tail)                 # Hubzu: id first
    tail = re.sub(r'-(united-states-)?[a-z]{3}$', '', tail) if tail.endswith(('-trd', '-tps')) else tail
    return re.sub(r'[-_]+', ' ', tail).strip().title()[:70]


def add_listing(site, key, url, address=None, city='', zip_='', date=None, start_price=None):
    lid = f'{site}|{key}'
    if lid in KNOWN:
        return next(x for x in L if x['id'] == lid)
    l = dict(id=lid, site=site, address=address or slug_to_address(url), city=city, zip=zip_,
             date=date, start_price=start_price, url=url, added=time.strftime('%Y-%m-%d %H:%M:%S'))
    L.append(l); KNOWN.add(lid)
    S.setdefault(lid, {})['next'] = time.time()          # look at new properties straight away
    return l


def load_all():
    if os.path.exists(STATE_F):
        try:
            S.update(json.load(open(STATE_F)))
        except Exception:
            pass
    if os.path.exists(LISTINGS_F):
        try:
            for l in json.load(open(LISTINGS_F)):
                if l['id'] not in KNOWN:
                    L.append(l); KNOWN.add(l['id']); S.setdefault(l['id'], {})
        except Exception:
            pass
    S['_v'] = 2


def save_all():
    for path, obj in ((STATE_F, S), (LISTINGS_F, L)):
        tmp = path + '.tmp'
        json.dump(obj, open(tmp, 'w'))
        os.replace(tmp, path)


def log_row(row):
    new = not os.path.exists(LOG_F)
    with open(LOG_F, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if new:
            w.writerow(['time', 'id', 'address', 'status', 'bid', 'bid_label', 'countdown_sec', 'reserve', 'ended_words', 'note'])
        w.writerow(row)
    try:
        key = (row[3], row[4])
        if row[3] != 'ERROR' and _LAST_LOG.get(row[1]) != key:
            _LAST_LOG[row[1]] = key
            LOGBUF.append([row[0], row[1], row[2], row[3], row[4], row[6], row[7]])
    except Exception:
        pass


def prune():
    """finished listings drop off the watch list a couple of days after the final bid is saved"""
    cutoff = time.time() - KEEP_FINAL_DAYS * 86400
    keep = []
    for l in L:
        s = S.get(l['id'], {})
        fin = s.get('final_ts')
        if s.get('status') == 'FINAL' and fin and fin < cutoff:
            KNOWN.discard(l['id']); S.pop(l['id'], None)
        else:
            keep.append(l)
    L[:] = keep


# ---------------------------------------------------------------- results files (the Google Sheet pulls these from GitHub)
RESULTS_F = os.path.join(HERE, 'results.csv')
CLOG_F = os.path.join(HERE, 'cloud_log.csv')
RES_HEAD = ['id', 'site', 'address', 'city', 'zip', 'start_price', 'status', 'last_live_bid', 'last_bid_at', 'FINAL_bid',
            'final_at', 'reserve', 'seconds_left', 'looks', 'url', 'note', 'updated']
LOG_HEAD = ['time', 'id', 'address', 'status', 'bid', 'seconds_left', 'reserve']


def results_rows():
    rows = []
    for l in L:
        s = S.get(l['id'], {})
        rows.append([l['id'], l['site'], l['address'], l['city'], l['zip'], l['start_price'], s.get('status', 'waiting'),
                     s.get('last_bid'), s.get('last_bid_at'), s.get('final_bid'), s.get('final_at'), s.get('reserve'),
                     s.get('countdown'), s.get('polls', 0), l['url'], s.get('note'),
                     time.strftime('%Y-%m-%d %H:%M:%S')])
    return rows


def write_outputs(log_rows):
    tmp = RESULTS_F + '.tmp'
    with open(tmp, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(RES_HEAD)
        for r in results_rows():
            w.writerow(['' if v is None else v for v in r])
    os.replace(tmp, RESULTS_F)
    if log_rows:
        new = not os.path.exists(CLOG_F)
        with open(CLOG_F, 'a', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            if new: w.writerow(LOG_HEAD)
            for r in log_rows:
                w.writerow(['' if v is None else v for v in r])
        with open(CLOG_F, encoding='utf-8') as f:          # keep the file small
            lines = f.readlines()
        if len(lines) > 40000:
            with open(CLOG_F, 'w', encoding='utf-8') as f:
                f.writelines(lines[:1] + lines[-30000:])


async def push_sheet():
    global LOGBUF
    log, LOGBUF = LOGBUF, []
    try:
        write_outputs(log)
        return 'files'
    except Exception as e:
        LOGBUF = log + LOGBUF
        print('writing results failed:', type(e).__name__, str(e)[:100], flush=True)


# ---------------------------------------------------------------- polling
async def look(ctx, l, S, sem):
    async with sem:
        s = S.setdefault(l['id'], {})
        now = time.time()
        stamp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        page = await ctx.new_page()
        try:
            await page.goto(l['url'], wait_until='domcontentloaded', timeout=45000)
            raw = ''
            for _ in range(14):      # wait up to ~14s for the bid / countdown to render
                await page.wait_for_timeout(1000)
                raw = await page.inner_text('body')
                if re.search(r'Bidding Ends In|Time Remaining|Time Left|Current Bid|Highest Bid|Auction Ended|In Contracting|Under Review|Starts In|Bidding Starts|Auction Closed', raw, re.I):
                    break
            if len(raw) < 300:
                raise RuntimeError('page text too short (blocked?)')
        except Exception as e:
            s['next'] = now + 300
            s['note'] = f'error {type(e).__name__}: {str(e)[:60]}'
            log_row([stamp, l['id'], l['address'], 'ERROR', '', '', '', '', '', s['note']])
            await page.close()
            return
        await page.close()
        info = parse_page(raw)
        s['polls'] = s.get('polls', 0) + 1
        s['countdown'] = info['countdown']
        rp = re.search(r'Reserve\s+Price\s*:?\s*\$\s*([\d,]+)', cut(raw), re.I)
        if rp: s['reserve_price'] = float(rp.group(1).replace(',', ''))
        if info['reserve']: s['reserve'] = info['reserve']
        s['note'] = ''
        log_row([stamp, l['id'], l['address'], info['status'], info['price'] or '', info['price_label'] or '',
                 info['countdown'] if info['countdown'] is not None else '', info['reserve'] or '', info['ended_words'], ''])

        if info['status'] in ('LIVE', 'UNKNOWN', 'UPCOMING'):
            sk = f"live_{l['site']}"
            S.setdefault('_snaps', {})
            if S['_snaps'].get(sk, 0) < 4:      # keep a few raw pages per site so parsing can be checked
                S['_snaps'][sk] = S['_snaps'].get(sk, 0) + 1
                os.makedirs(SNAP_D, exist_ok=True)
                open(os.path.join(SNAP_D, f"{sk}_{S['_snaps'][sk]}_{re.sub(r'[^A-Za-z0-9]+', '_', l['address'])[:25]}.txt"), 'w', encoding='utf-8').write(raw[:9000])
            s['status'] = info['status']
            if info['price'] and info['status'] == 'LIVE':
                s['last_bid'] = info['price']; s['last_bid_at'] = stamp
            if s.get('end_looks'):   # looked ended before but is live again (soft close / relist)
                s['end_looks'] = 0; s['final_bid'] = None; s['final_at'] = None
            s['ended_seen_at'] = None
            delay = next_delay(info) if info['status'] in ('LIVE', 'UPCOMING') else 1800
            if info['status'] == 'LIVE' and info['countdown'] is None:   # site shows no readable timer: be safe near the listed end day
                try: dd = dt.datetime.fromisoformat(l['date']).date() if l['date'] else None
                except Exception: dd = None
                delay = 90 if dd == dt.date.today() else 1800
            s['next'] = now + delay
        else:  # ENDED
            os.makedirs(SNAP_D, exist_ok=True)
            safe = re.sub(r'[^A-Za-z0-9]+', '_', l['id'])[:60]
            n = s.get('end_looks', 0)
            open(os.path.join(SNAP_D, f'{safe}_post{n}.txt'), 'w', encoding='utf-8').write(cut(raw)[:6000])
            s['end_looks'] = n + 1
            if info['price']:
                s['post_end_price'] = info['price']
            if n == 0:
                s['status'] = 'ENDED (verifying)'
                s['ended_seen_at'] = stamp
                s['next'] = now + 900
                # provisional final = last live bid
                if s.get('last_bid') and not s.get('final_bid'):
                    s['final_bid'] = s['last_bid']; s['final_at'] = stamp
            elif n == 1:
                s['status'] = 'ENDED (verifying)'; s['next'] = now + 7200
            else:
                s['status'] = 'FINAL'; s['final_ts'] = time.time()
                fb = s.get('post_end_price') or s.get('last_bid')
                s['final_bid'] = fb; s['final_at'] = stamp; s['next'] = None
                if s.get('post_end_price') and s.get('last_bid') and s['post_end_price'] != s['last_bid']:
                    s['note'] = f"post-end page shows ${s['post_end_price']:,.0f} vs last live ${s['last_bid']:,.0f}"
                if not fb:
                    s['note'] = 'ended but no bid captured (no bids, or page never showed a price)'


# ---------------------------------------------------------------- Hubzu: read bids from the public search pages
# (Hubzu hides the bid on the detail page unless you are logged in, but its search cards show it.)
def parse_hubzu_card(text):
    cd = None
    hm = re.search(r'(\d+)\|Hours?\|:\|(\d+)\|Minutes?\|:\|(\d+)\|Seconds?', text)
    if hm:
        dm = re.search(r'(\d+)\|Days?\|:', text)
        cd = (int(dm.group(1)) * 86400 if dm else 0) + int(hm.group(1)) * 3600 + int(hm.group(2)) * 60 + int(hm.group(3))
    pm = re.search(r'Current Highest Bid\|\$([\d,]+)', text)
    return dict(countdown=cd, price=float(pm.group(1).replace(',', '')) if pm else None,
                starting=bool(re.search(r'Starting Bid', text)))


async def hubzu_cards_loop(ctx, L, S):
    byid = {l['id']: l for l in L if l['site'] == 'Hubzu'}
    while True:
        minleft = None
        try:
            for pn in range(1, 31):
                page = await ctx.new_page()
                cards = []
                try:
                    await page.goto(f'https://www.hubzu.com/tx/auctions?pageNumber={pn}', wait_until='domcontentloaded', timeout=45000)
                    for _ in range(14):
                        await page.wait_for_timeout(1000)
                        if await page.evaluate("document.querySelectorAll('.feat_property').length"):
                            break
                    cards = await page.evaluate(r"""() => [...document.querySelectorAll('.feat_property')].map(c => {
                        const a = c.querySelector('a[href*="-home/"]');
                        return {href: a ? a.getAttribute('href') : '', text: c.innerText.replace(/\n+/g, '|')}; })""")
                except Exception as e:
                    log_row([dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Hubzu page {pn}', '', 'ERROR', '', '', '', '', '', f'cards {type(e).__name__}'])
                finally:
                    await page.close()
                if not cards:
                    break
                stamp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for c in cards:
                    m = re.search(r'/(\d+)-', c['href'])
                    sid = f"Hubzu|{m.group(1)}" if m else None
                    if not sid:
                        continue
                    if sid not in byid:
                        href = c['href']
                        byid[sid] = add_listing('Hubzu', m.group(1), href if href.startswith('http') else 'https://www.hubzu.com' + href)
                    info = parse_hubzu_card(c['text'])
                    s = S.setdefault(sid, {})
                    if s.get('status') == 'FINAL':
                        continue
                    if info['countdown'] is not None:
                        s['countdown'] = info['countdown']
                        if info['countdown'] > 0 and not str(s.get('status', '')).startswith('ENDED'):
                            s['status'] = 'LIVE'
                            minleft = info['countdown'] if minleft is None else min(minleft, info['countdown'])
                            nxt = time.time() + next_delay(dict(countdown=info['countdown'], status='LIVE'))
                            if not s.get('next') or nxt < s['next']:
                                s['next'] = nxt
                    if info['price']:
                        s['last_bid'] = info['price']; s['last_bid_at'] = stamp
                    s['hz_card_at'] = stamp
                    log_row([stamp, sid, byid[sid]['address'], 'LIVE(card)', info['price'] or '', 'Current Highest Bid' if info['price'] else 'Starting Bid',
                             info['countdown'] if info['countdown'] is not None else '', '', '', ''])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print('hubzu cards error', type(e).__name__, str(e)[:80], flush=True)
        await asyncio.sleep(30 if (minleft is not None and minleft < 900) else 120)


# ---------------------------------------------------------------- finding NEW properties on each website
# Each entry: where the Texas list is, and how to recognise a property link on it.
#   rx  = regular expression applied to every link on the page; group gid is the property id
EXTRA_SITES = [
    # dict(site='RealtyBid', urls=['https://www.realtybid.com/houses-for-auction/'], rx=r'/property/[^/]*-tx-[^/]*?-(\d+)', gid=1, max_pages=20),
]
SCRAPE = [
    dict(site='Auction.com', urls=['https://www.auction.com/residential/tx/active_lt/auction_date_order,resi_sort_v2_st'],
         rx=r'/details/([a-z0-9\-]+-tx-(\d+))(?:[/?#]|$)', gid=2, max_pages=40),
    dict(site='Xome', urls=['https://www.xome.com/auctions/tx'],
         rx=r'/auctions/([a-z0-9\-]+-tx-\d{5}-(\d+))(?:[/?#]|$)', gid=2, max_pages=40),
] + EXTRA_SITES

NEXT_SEL = ('a[rel=next]:not([aria-disabled=true]), a[aria-label*="Next" i]:not([aria-disabled=true]), '
            'button[aria-label*="Next" i]:not([disabled]), li.next:not(.disabled) a, .pagination-next:not(.disabled)')


async def scrape_site(ctx, cfg):
    found = {}
    page = await ctx.new_page()
    try:
        for url in cfg['urls']:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            for pg in range(cfg.get('max_pages', 30)):
                await page.wait_for_timeout(3500)
                for _ in range(5):
                    await page.mouse.wheel(0, 4000)
                    await page.wait_for_timeout(700)
                hrefs = await page.evaluate("[...document.querySelectorAll('a[href]')].map(a => a.href)")
                before = len(found)
                for h in hrefs:
                    m = re.search(cfg['rx'], h, re.I)
                    if m:
                        found[m.group(cfg['gid'])] = h.split('#')[0]
                nxt = await page.query_selector(NEXT_SEL)
                if not nxt or (pg > 0 and len(found) == before):
                    break
                await nxt.click()
    except Exception as e:
        print(f"discover {cfg['site']} error {type(e).__name__}: {str(e)[:80]}", flush=True)
    finally:
        await page.close()
    new = 0
    for key, href in found.items():
        if f"{cfg['site']}|{key}" not in KNOWN:
            add_listing(cfg['site'], key, href); new += 1
    print(f"discover {cfg['site']}: {len(found)} on site, {new} new", flush=True)


async def discover_servicelink(ctx):
    page = await ctx.new_page()
    new = total = 0
    try:
        await page.goto('https://www.servicelinkauction.com/properties', wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_timeout(5000)
        seen = {}
        for order in ('true', 'false'):
            data = await page.evaluate("""async (o) => { try { const r = await fetch('/api/listingsvc/v1/listings?State=TX&limit=100&sortByEndingSoonest=' + o);
                       return r.ok ? ((await r.json()).data || []) : []; } catch (e) { return []; } }""", order)
            for x in data:
                p = x.get('propertyInfo') or {}
                url = p.get('websiteUrl') or ''
                if '/property-details/' not in url or '-tx-' not in url:
                    continue
                seen[url.rstrip('/').split('/')[-1]] = (url, p, x.get('auctionRunInfo') or {})
        total = len(seen)
        for key, (url, p, run) in seen.items():
            if f'ServiceLink|{key}' not in KNOWN:
                add_listing('ServiceLink', key, url, address=p.get('address'), city=p.get('city', ''), zip_=p.get('zip', '') or p.get('postalCode', ''),
                            date=(run.get('endDate') or '')[:10] or None, start_price=run.get('startingBid')); new += 1
    except Exception as e:
        print(f'discover ServiceLink error {type(e).__name__}: {str(e)[:80]}', flush=True)
    finally:
        await page.close()
    print(f'discover ServiceLink: {total} on site, {new} new', flush=True)


async def discover_loop(ctx):
    while True:
        for cfg in SCRAPE:
            await scrape_site(ctx, cfg)
        await discover_servicelink(ctx)
        # Hubzu is found by hubzu_cards_loop below (its list pages are read there every 30 s - 2 min)
        await asyncio.sleep(DISCOVER_EVERY)


# ---------------------------------------------------------------- main
async def main():
    load_all()
    print(f'Cloud tracker start: {len(L)} saved listings', flush=True)
    sem = asyncio.Semaphore(CONCURRENCY)
    prof = tempfile.mkdtemp(prefix='trk_')
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            prof, headless=True, viewport={'width': 1366, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox'])
        last_save = 0; inflight = set()
        tasks = [asyncio.create_task(hubzu_cards_loop(ctx, L, S)), asyncio.create_task(discover_loop(ctx))]
        try:
            while True:
                now = time.time()
                if MAX_RUN and now - T0 > MAX_RUN:
                    print('max run time reached - saving and handing over to the next run', flush=True)
                    break
                due = [l for l in L if S.get(l['id'], {}).get('next') and S[l['id']]['next'] <= now
                       and l['id'] not in inflight and S[l['id']].get('status') != 'FINAL']
                due.sort(key=lambda l: S[l['id']]['next'])
                for l in due[:CONCURRENCY * 2]:
                    inflight.add(l['id'])
                    t = asyncio.create_task(look(ctx, l, S, sem))
                    t.add_done_callback(lambda _t, i=l['id']: inflight.discard(i))
                if now - last_save > 60:
                    prune(); save_all(); last_save = now
                    st = await push_sheet()
                    live = sum(1 for l in L if S.get(l['id'], {}).get('status') == 'LIVE')
                    fin = sum(1 for l in L if S.get(l['id'], {}).get('status') == 'FINAL')
                    err = sum(1 for l in L if str(S.get(l['id'], {}).get('note', '')).startswith('error'))
                    print(time.strftime('%H:%M:%S'), f'listings={len(L)} live={live} final={fin} errors={err} due={len(due)} out={st}', flush=True)
                await asyncio.sleep(3)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.sleep(1)
            prune(); save_all()
            await push_sheet()
            await ctx.close()


# ---- v2 scrape_site (overrides the one above): list pages that scroll inside a box (Auction.com) are scrolled fully
COLLECT_JS = """async () => {
  const seen = new Set();
  const grab = () => document.querySelectorAll('a[href]').forEach(a => seen.add(a.href));
  const scrollers = () => [document.scrollingElement, ...document.querySelectorAll('div,main,section,ul')].filter(
    e => e && e.scrollHeight > e.clientHeight + 200 && (e === document.scrollingElement || /auto|scroll/.test(getComputedStyle(e).overflowY)));
  let stall = 0, steps = 0;
  while (stall < 8 && steps < 500) {
    grab(); const b = seen.size;
    for (const e of scrollers()) e.scrollBy(0, 450);
    await new Promise(r => setTimeout(r, 600));
    grab(); stall = seen.size === b ? stall + 1 : 0; steps++;
  }
  return [...seen];
}"""


async def scrape_site(ctx, cfg):
    found = {}
    page = await ctx.new_page()
    try:
        for url in cfg['urls']:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            for pg in range(cfg.get('max_pages', 30)):
                await page.wait_for_timeout(4000)
                hrefs = await page.evaluate(COLLECT_JS)
                before = len(found)
                for h in hrefs:
                    m = re.search(cfg['rx'], h, re.I)
                    if m:
                        found[m.group(cfg['gid'])] = h.split('#')[0]
                nxt = await page.query_selector(NEXT_SEL)
                if not nxt or (pg > 0 and len(found) == before):
                    break
                try:
                    await nxt.click(timeout=8000)
                except Exception:
                    break
    except Exception as e:
        print(f"discover {cfg['site']} error {type(e).__name__}: {str(e)[:80]}", flush=True)
    finally:
        await page.close()
    new = 0
    for key, href in found.items():
        if f"{cfg['site']}|{key}" not in KNOWN:
            add_listing(cfg['site'], key, href); new += 1
    print(f"discover {cfg['site']}: {len(found)} on site, {new} new", flush=True)


# Auction.com shows at most 500 properties per search, so also read it sorted by value (high->low and low->high)
for _c in SCRAPE:
    if _c['site'] == 'Auction.com':
        _c['urls'] += ['https://www.auction.com/residential/tx/active_lt/valuation_estimate%20desc_st',
                       'https://www.auction.com/residential/tx/active_lt/valuation_estimate%20asc_st']


# ---------------------------------------------------------------- Public Surplus (Texas real estate only)
PS_JS = """() => {
  const out = [];
  const as = [...document.querySelectorAll('a[href*="auc="]')];
  for (let i = 0; i < as.length; i++) {
    const m = as[i].href.match(/auc=(\\d+)/);
    if (!m) continue;
    const t = as[i].innerText.trim();
    if (/^[A-Z]{2}$/.test(t)) {
      const nx = as.slice(i + 1, i + 3).find(a => a.href === as[i].href && !/^[A-Z]{2}$/.test(a.innerText.trim()));
      out.push([m[1], t, nx ? nx.innerText.trim() : '']);
    }
  }
  return out;
}"""


async def discover_publicsurplus(ctx):
    found = {}
    page = await ctx.new_page()
    try:
        await page.goto('https://www.publicsurplus.com/sms/browse/cataucs?catid=15', wait_until='domcontentloaded', timeout=60000)
        for pg in range(40):
            await page.wait_for_timeout(3000)
            before = len(found)
            for aid, st, title in await page.evaluate(PS_JS):
                if st == 'TX':
                    found[aid] = title
            nxt = page.locator('a:has-text("Next")').first
            if await nxt.count() == 0 or (pg > 0 and len(found) == before and pg > 8):
                break
            try:
                await nxt.click(timeout=8000)
            except Exception:
                break
    except Exception as e:
        print(f'discover PublicSurplus error {type(e).__name__}: {str(e)[:80]}', flush=True)
    finally:
        await page.close()
    new = 0
    for aid, title in found.items():
        if f'PublicSurplus|{aid}' not in KNOWN:
            add_listing('PublicSurplus', aid, f'https://www.publicsurplus.com/sms/auction/view?auc={aid}',
                        address=(title or 'Public Surplus ' + aid) + ' TX'); new += 1
    print(f'discover PublicSurplus: {len(found)} TX on site, {new} new', flush=True)


async def discover_loop(ctx):
    while True:
        for cfg in SCRAPE:
            await scrape_site(ctx, cfg)
        await discover_servicelink(ctx)
        await discover_publicsurplus(ctx)
        await asyncio.sleep(DISCOVER_EVERY)



if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

import asyncio, html, json, logging, math, os, time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp, aiosqlite, uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

BINANCE_BASE=os.getenv('BINANCE_BASE_URL','https://fapi.binance.com').rstrip('/')
BOT_TOKEN=os.getenv('TELEGRAM_BOT_TOKEN','').strip(); CHAT_ID=os.getenv('TELEGRAM_CHAT_ID','').strip()
PORT=int(os.getenv('PORT','8080')); TZ=ZoneInfo(os.getenv('TZ','Asia/Riyadh'))
SCAN_SECONDS=int(os.getenv('SCAN_SECONDS','15')); MAX_CONCURRENCY=int(os.getenv('MAX_CONCURRENCY','16'))
RADAR_POOL=int(os.getenv('RADAR_POOL','140')); DEEP_CANDIDATES=int(os.getenv('DEEP_CANDIDATES','50'))
MIN_QUOTE_VOLUME=float(os.getenv('MIN_QUOTE_VOLUME_USDT','750000'))
ENTRY_MODE=os.getenv('ENTRY_MODE','BALANCED').upper(); DIRECTION_GAP=float(os.getenv('DIRECTION_GAP','7'))
MAX_EARLY_EXTENSION_ATR=float(os.getenv('MAX_EARLY_EXTENSION_ATR','0.85'))
MAX_ENTRY_EXTENSION_ATR=float(os.getenv('MAX_ENTRY_EXTENSION_ATR','0.62'))
PRESSURE_HISTORY=int(os.getenv('PRESSURE_HISTORY','12')); MIN_DRIFT=float(os.getenv('MIN_DRIFT','2.5'))
SEND_STARTUP_MESSAGE=os.getenv('SEND_STARTUP_MESSAGE','true').lower()=='true'
SEND_TEST_MESSAGE=os.getenv('SEND_TEST_MESSAGE','true').lower()=='true'
ENABLE_MANUAL_TEST_ENDPOINT=os.getenv('ENABLE_MANUAL_TEST_ENDPOINT','true').lower()=='true'
EXCHANGE_INFO_CACHE_SECONDS=int(os.getenv('EXCHANGE_INFO_CACHE_SECONDS','3600'))
TICKER_CACHE_SECONDS=int(os.getenv('TICKER_CACHE_SECONDS','10'))
BINANCE_RETRIES=int(os.getenv('BINANCE_RETRIES','4'))
SYMBOL_TIMEOUT=float(os.getenv('SYMBOL_TIMEOUT','12'))
SCAN_TIMEOUT=float(os.getenv('SCAN_TIMEOUT','45'))
DB_PATH=os.getenv('DB_PATH','data/quantum_entry_v2.db'); Path(DB_PATH).parent.mkdir(parents=True,exist_ok=True)
MODES={'AGGRESSIVE':(54,63,71,2),'BALANCED':(58,67,75,3),'CONSERVATIVE':(63,72,81,4)}
BUILDUP,READY,IGNITION,MIN_FACTORS=MODES.get(ENTRY_MODE,MODES['BALANCED'])
logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s'); log=logging.getLogger('quantum')

def clamp(x,lo=0,hi=100): return max(lo,min(hi,x))
def safe_div(a,b,d=0): return a/b if b else d
def pct(a,b): return safe_div(a-b,abs(b),0)*100
def now(): return datetime.now(TZ)
def fmt(x):
    if x>=1000:return f'{x:,.2f}'
    if x>=1:return f'{x:,.4f}'.rstrip('0').rstrip('.')
    if x>=.01:return f'{x:.6f}'.rstrip('0').rstrip('.')
    return f'{x:.8f}'.rstrip('0').rstrip('.')
def atr(r,n=14):
    if len(r)<2:return 0
    tr=[]
    for i in range(1,len(r)):
        h,l,pc=float(r[i][2]),float(r[i][3]),float(r[i-1][4]); tr.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(tr[-n:])/max(1,min(n,len(tr)))
def ohlcv(r):
    return {'o':[float(x[1]) for x in r],'h':[float(x[2]) for x in r],'l':[float(x[3]) for x in r],'c':[float(x[4]) for x in r],'v':[float(x[5]) for x in r],'tb':[float(x[9]) for x in r]}
def ema(vals,n):
    if not vals:return []
    a=2/(n+1); out=[vals[0]]
    for v in vals[1:]:out.append(a*v+(1-a)*out[-1])
    return out
def stdev(vals):
    m=sum(vals)/len(vals); return math.sqrt(sum((x-m)**2 for x in vals)/len(vals))
def vwap(r):
    pv=vol=0
    for x in r:
        tp=(float(x[2])+float(x[3])+float(x[4]))/3; q=float(x[5]); pv+=tp*q; vol+=q
    return safe_div(pv,vol,float(r[-1][4]) if r else 0)

@dataclass
class Signal:
    symbol:str; direction:str; stage:str; engine:str; score:float; timing:float; opportunity:float; mood:float
    price:float; entry_low:float; entry_high:float; stop:float; tp1:float; tp2:float; tp3:float
    rr1:float; rr2:float; rr3:float; factors:list[str]; details:dict[str,Any]

SCHEMA='''
CREATE TABLE IF NOT EXISTS opportunities(id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,direction TEXT,engine TEXT,current_stage TEXT,status TEXT DEFAULT 'OPEN',opened_at TEXT,updated_at TEXT,score REAL,timing REAL,opportunity REAL,mood REAL,entry_low REAL,entry_high REAL,stop REAL,tp1 REAL,tp2 REAL,tp3 REAL,rr1 REAL,rr2 REAL,rr3 REAL,factors_json TEXT,details_json TEXT,entered_at TEXT,entered_price REAL,tp1_at TEXT,tp2_at TEXT,tp3_at TEXT,stop_at TEXT,best_price REAL,worst_price REAL,mfe_pct REAL DEFAULT 0,mae_pct REAL DEFAULT 0,closed_at TEXT,outcome TEXT);
CREATE INDEX IF NOT EXISTS idx_open ON opportunities(status,symbol,direction,engine);
CREATE TABLE IF NOT EXISTS checkpoints(id INTEGER PRIMARY KEY AUTOINCREMENT,created_at TEXT,scan_number INTEGER,symbols_total INTEGER,candidates_total INTEGER,analyzed_total INTEGER,alerts_sent INTEGER,scan_seconds REAL,market_mood REAL,error TEXT);
CREATE TABLE IF NOT EXISTS rejected(id INTEGER PRIMARY KEY AUTOINCREMENT,created_at TEXT,symbol TEXT,direction TEXT,engine TEXT,score REAL,reason TEXT,details_json TEXT);
'''
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db: await db.executescript(SCHEMA); await db.commit()
async def open_opp(s,d,e):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row
        return await (await db.execute("SELECT * FROM opportunities WHERE symbol=? AND direction=? AND engine=? AND status='OPEN' ORDER BY id DESC LIMIT 1",(s,d,e))).fetchone()
async def save_signal(sig):
    ex=await open_opp(sig.symbol,sig.direction,sig.engine); ts=now().isoformat(); order={'BUILDUP':1,'READY':2,'IGNITION':3}
    async with aiosqlite.connect(DB_PATH) as db:
        if ex:
            if order[sig.stage]<=order[ex['current_stage']]: return ex['id'],False
            await db.execute("UPDATE opportunities SET current_stage=?,updated_at=?,score=?,timing=?,opportunity=?,mood=?,entry_low=?,entry_high=?,stop=?,tp1=?,tp2=?,tp3=?,rr1=?,rr2=?,rr3=?,factors_json=?,details_json=? WHERE id=?",(sig.stage,ts,sig.score,sig.timing,sig.opportunity,sig.mood,sig.entry_low,sig.entry_high,sig.stop,sig.tp1,sig.tp2,sig.tp3,sig.rr1,sig.rr2,sig.rr3,json.dumps(sig.factors),json.dumps(sig.details),ex['id']))
            await db.commit(); return ex['id'],True
        cur=await db.execute("INSERT INTO opportunities(symbol,direction,engine,current_stage,status,opened_at,updated_at,score,timing,opportunity,mood,entry_low,entry_high,stop,tp1,tp2,tp3,rr1,rr2,rr3,factors_json,details_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(sig.symbol,sig.direction,sig.engine,sig.stage,'OPEN',ts,ts,sig.score,sig.timing,sig.opportunity,sig.mood,sig.entry_low,sig.entry_high,sig.stop,sig.tp1,sig.tp2,sig.tp3,sig.rr1,sig.rr2,sig.rr3,json.dumps(sig.factors),json.dumps(sig.details)))
        await db.commit(); return cur.lastrowid,True
async def checkpoint(n,s,c,a,al,sec,mood,error=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO checkpoints(created_at,scan_number,symbols_total,candidates_total,analyzed_total,alerts_sent,scan_seconds,market_mood,error) VALUES(?,?,?,?,?,?,?,?,?)",(now().isoformat(),n,s,c,a,al,sec,mood,error)); await db.commit()

class Binance:
    def __init__(self):
        self.s=None;self.sem=asyncio.Semaphore(MAX_CONCURRENCY);self.cache={};self.health={'ok':False,'last_error':None,'last_success':None}
    async def start(self):
        self.s=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20),connector=aiohttp.TCPConnector(limit=max(32,MAX_CONCURRENCY*2),ttl_dns_cache=300))
    async def close(self):
        if self.s and not self.s.closed:await self.s.close()
    def cget(self,k,ttl):
        x=self.cache.get(k)
        if not x:return None
        if time.time()-x[0]>ttl:self.cache.pop(k,None);return None
        return x[1]
    async def get(self,path,params=None,cache_key=None,cache_ttl=0):
        if cache_key and cache_ttl:
            c=self.cget(cache_key,cache_ttl)
            if c is not None:return c
        last=None
        async with self.sem:
            for i in range(BINANCE_RETRIES):
                try:
                    async with self.s.get(BINANCE_BASE+path,params=params) as r:
                        body=await r.text()
                        if r.status in (418,429):await asyncio.sleep(min(2**i+1,10));last=RuntimeError(f'rate {r.status}');continue
                        if r.status>=500:await asyncio.sleep(min(1.5**i,8));last=RuntimeError(f'server {r.status}');continue
                        if r.status!=200:raise RuntimeError(f'HTTP {r.status}: {body[:200]}')
                        d=json.loads(body)
                        if d is None:raise RuntimeError('null response')
                        self.health={'ok':True,'last_error':None,'last_success':now().isoformat()}
                        if cache_key and cache_ttl:self.cache[cache_key]=(time.time(),d)
                        return d
                except Exception as e:
                    last=e;self.health['last_error']=repr(e)
                    if i<BINANCE_RETRIES-1:await asyncio.sleep(min(1.5**i,8))
        self.health['ok']=False
        raise RuntimeError(f'Binance request failed {path}: {last!r}')
    async def symbols(self):
        d=await self.get('/fapi/v1/exchangeInfo',cache_key='exchange',cache_ttl=EXCHANGE_INFO_CACHE_SECONDS)
        if not isinstance(d,dict) or not isinstance(d.get('symbols'),list):raise RuntimeError('invalid exchangeInfo')
        return [x.get('symbol') for x in d['symbols'] if isinstance(x,dict) and x.get('symbol') and x.get('status')=='TRADING' and x.get('contractType')=='PERPETUAL' and x.get('quoteAsset')=='USDT']
    async def tickers(self):
        d=await self.get('/fapi/v1/ticker/24hr',cache_key='tickers',cache_ttl=TICKER_CACHE_SECONDS)
        if not isinstance(d,list):raise RuntimeError('invalid tickers')
        return d
    async def klines(self,s,i,l=100):
        d=await self.get('/fapi/v1/klines',{'symbol':s,'interval':i,'limit':l})
        if not isinstance(d,list) or len(d)<30:raise RuntimeError(f'invalid klines {s} {i}')
        return d
    async def oi(self,s):
        d=await self.get('/futures/data/openInterestHist',{'symbol':s,'period':'5m','limit':12})
        if not isinstance(d,list):raise RuntimeError(f'invalid OI {s}')
        return d
    async def depth(self,s):
        d=await self.get('/fapi/v1/depth',{'symbol':s,'limit':100})
        if not isinstance(d,dict):raise RuntimeError(f'invalid depth {s}')
        return d
    async def premium(self,s):
        d=await self.get('/fapi/v1/premiumIndex',{'symbol':s})
        if not isinstance(d,dict):raise RuntimeError(f'invalid premium {s}')
        return d
    async def prices(self):
        d=await self.get('/fapi/v1/ticker/price')
        if not isinstance(d,list):raise RuntimeError('invalid prices')
        out={}
        for x in d:
            try:out[x['symbol']]=float(x['price'])
            except Exception:pass
        return out

def mood(t):
    ch=[float(x.get('priceChangePercent',0) or 0) for x in t if str(x.get('symbol','')).endswith('USDT')]
    if not ch:return 50
    p=sum(x>0 for x in ch)/len(ch); med=sorted(ch)[len(ch)//2];return clamp(50+(p-.5)*60+med*2)
def anomaly(t,prev):
    p=float(t.get('lastPrice',0) or 0); q=float(t.get('quoteVolume',0) or 0); n=float(t.get('count',0) or 0)
    pb=vb=tb=0
    if prev:pb=abs(pct(p,prev['p']));vb=max(0,pct(q,prev['q']));tb=max(0,pct(n,prev['n']))
    liq=clamp((math.log10(max(q,1))-5)*20);score=clamp(.35*clamp(pb*1200)+.25*clamp(vb*10)+.2*clamp(tb*8)+.2*liq)
    return score,{'p':p,'q':q,'n':n}
def micro(r,direction):
    d=ohlcv(r);sign=1 if direction=='BUY' else -1;a=atr(r);price=d['c'][-1];delta=[2*x-y for x,y in zip(d['tb'],d['v'])]
    dn=sum(delta[-2:])*sign;dp=sum(delta[-6:-2])*sign;cn=sum(delta[-12:])*sign;cp=sum(delta[-20:-8])*sign
    rh=max(d['h'][-8:-1]);rl=min(d['l'][-8:-1])
    if direction=='BUY':sweep=d['l'][-1]<rl and d['c'][-1]>rl;reject=(min(d['o'][-1],d['c'][-1])-d['l'][-1])>=(d['h'][-1]-d['l'][-1])*.42;mb=d['c'][-1]>max(d['h'][-4:-1]);pivot=min(d['l'][-8:]);ext=safe_div(price-pivot,a,99)
    else:sweep=d['h'][-1]>rh and d['c'][-1]<rh;reject=(d['h'][-1]-max(d['o'][-1],d['c'][-1]))>=(d['h'][-1]-d['l'][-1])*.42;mb=d['c'][-1]<min(d['l'][-4:-1]);pivot=max(d['h'][-8:]);ext=safe_div(pivot-price,a,99)
    av=sum(d['v'][-20:-1])/19;vr=safe_div(d['v'][-1],av,1)
    return {'price':price,'atr':a,'delta':clamp(50+safe_div(dn,max(sum(d['v'][-2:]),1),0)*280),'delta_accel':clamp(50+safe_div(dn-dp,max(sum(d['v'][-6:]),1),0)*320),'cvd':clamp(50+safe_div(cn,max(sum(d['v'][-12:]),1),0)*260),'cvd_shift':clamp(50+safe_div(cn-cp,max(sum(d['v'][-20:]),1),0)*300),'sweep':sweep,'reject':reject,'micro_break':mb,'extension':ext,'volume_expansion':vr>=1.2}
def context(r,direction):
    d=ohlcv(r);sign=1 if direction=='BUY' else -1;a=atr(r);price=d['c'][-1];delta=[2*x-y for x,y in zip(d['tb'],d['v'])]
    cv=sum(delta[-20:])*sign;cvp=sum(delta[-40:-20])*sign;rr=max(d['h'][-8:])-min(d['l'][-8:]);comp=clamp((2.4-safe_div(rr,a,0))/1.8*100)
    e9=ema(d['c'],9)[-1];e21=ema(d['c'],21)[-1];trend=clamp(50+pct(e9,e21)*sign*18);basis=sum(d['c'][-20:])/20;dev=stdev(d['c'][-20:]);vw=vwap(r[-48:])
    if direction=='BUY':vs=price>=vw;bs=price<=basis or d['l'][-1]<=basis-2*dev
    else:vs=price<=vw;bs=price>=basis or d['h'][-1]>=basis+2*dev
    return {'atr':a,'cvd_shift':clamp(50+safe_div(cv-cvp,max(sum(d['v'][-40:]),1),0)*300),'compression':comp,'trend':trend,'vwap_side':vs,'bb_side':bs,'swing_low':min(d['l'][-20:]),'swing_high':max(d['h'][-20:])}
def oi_feat(h):
    if len(h)<2:return {'change':0,'accel':50}
    v=[float(x.get('sumOpenInterest',0) or 0) for x in h];chg=pct(v[-1],v[-4] if len(v)>=4 else v[0]);r=pct(v[-1],v[-2]);p=pct(v[-2],v[-3]) if len(v)>=3 else 0
    return {'change':chg,'accel':clamp(50+(r-p)*18)}
def ob_feat(depth,direction):
    bids=[(float(p),float(q)) for p,q in depth.get('bids',[])];asks=[(float(p),float(q)) for p,q in depth.get('asks',[])];bn=sum(p*q for p,q in bids[:30]);an=sum(p*q for p,q in asks[:30]);raw=safe_div(bn-an,bn+an,0);signed=raw if direction=='BUY' else -raw
    side=[q for _,q in (bids[:50] if direction=='BUY' else asks[:50])];avg=sum(side)/max(1,len(side));wall=safe_div(max(side,default=0),avg,0)
    return {'imbalance':clamp(50+signed*180),'absorption':clamp(35+max(0,wall-2)*12),'spoof':clamp(max(0,wall-8)*14)}
def zone(r,direction):
    d=ohlcv(r);a=atr(r);best=None
    for i in range(max(2,len(d['c'])-24),len(d['c'])-3):
        ab=sum(abs(d['c'][j]-d['o'][j]) for j in range(max(0,i-8),i+1))/max(1,min(9,i+1));disp=abs(d['c'][i+1]-d['o'][i+1])>=max(ab*1.5,a*.35)
        if not disp:continue
        ok=(d['c'][i]<d['o'][i] and d['c'][i+1]>d['h'][i]) if direction=='BUY' else (d['c'][i]>d['o'][i] and d['c'][i+1]<d['l'][i])
        if ok:best={'active':True,'low':d['l'][i],'high':d['h'][i],'strength':65}
    return best or {'active':False,'low':0,'high':0,'strength':0}
def in_zone(price,z,a):return z['active'] and z['low']-a*.18<=price<=z['high']+a*.18
def plan(direction,price,a,sl,sh,z=None):
    zl,zh=(z['low'],z['high']) if z and z['active'] else (price-a*.2,price+a*.2)
    if direction=='BUY':el=min(price,zh);eh=max(price,zh+a*.08);stop=min(sl,zl)-a*.18;mid=(el+eh)/2;r=max(mid-stop,a*.55);t1,t2,t3=mid+r,mid+2*r,mid+3*r
    else:el=min(price,zl-a*.08);eh=max(price,zl);stop=max(sh,zh)+a*.18;mid=(el+eh)/2;r=max(stop-mid,a*.55);t1,t2,t3=mid-r,mid-2*r,mid-3*r
    rr=lambda t:abs(t-mid)/max(abs(mid-stop),1e-12);return el,eh,stop,t1,t2,t3,rr(t1),rr(t2),rr(t3)
def stage(score,timing,ext,n):
    if n<MIN_FACTORS:return None
    if score>=IGNITION and timing>=68 and ext<=MAX_ENTRY_EXTENSION_ATR:return 'IGNITION'
    if score>=READY and timing>=54 and ext<=MAX_EARLY_EXTENSION_ATR:return 'READY'
    if score>=BUILDUP and ext<=MAX_EARLY_EXTENSION_ATR:return 'BUILDUP'
def message(s):
    title={'BUILDUP':'🟡 مراقبة ما قبل الانفجار','READY':'🟠 دخول مبكر قبل الانفجار','IGNITION':'🔥 دخول الآن — بداية الانطلاق'}[s.stage];side='شراء' if s.direction=='BUY' else 'بيع';checks='\n'.join('✅ '+html.escape(x) for x in s.factors[:8]);tv=f'https://www.tradingview.com/chart/?symbol=BINANCE:{s.symbol}.P';bn=f'https://www.binance.com/en/futures/{s.symbol}'
    return f'''<b>{title} — {side}</b>\n\n💰 العملة: <b>#{s.symbol}.P</b>\n🧠 المحرك: <b>{s.engine}</b>\n💵 السعر: <b>{fmt(s.price)}</b>\n\n⚡ درجة الفرصة: <b>{s.score:.1f}%</b>\n⏱️ توقيت الدخول: <b>{s.timing:.1f}%</b>\n🎯 جدوى المخاطرة: <b>{s.opportunity:.1f}%</b>\n🌍 مزاج السوق: <b>{s.mood:.1f}%</b>\n\n🎯 منطقة الدخول: <b>{fmt(s.entry_low)} – {fmt(s.entry_high)}</b>\n🛑 الإبطال: <b>{fmt(s.stop)}</b>\n✅ TP1: <b>{fmt(s.tp1)}</b> ({s.rr1:.1f}R)\n✅ TP2: <b>{fmt(s.tp2)}</b> ({s.rr2:.1f}R)\n✅ TP3: <b>{fmt(s.tp3)}</b> ({s.rr3:.1f}R)\n\n{checks}\n\n📏 امتداد الحركة: <b>{s.details.get('extension',0):.2f} ATR</b>\n📈 تغير الثقة: <b>{s.details.get('drift',0):+.1f}</b>\n\n🕒 {now().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)\n🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>\n\n⚠️ خطة احتمالية وليست ضمانًا أو تنفيذًا تلقائيًا.'''

class Engine:
    def __init__(self):self.b=Binance();self.ts=None;self.running=True;self.scan_no=0;self.last_scan=None;self.last_error=None;self.symbols=0;self.candidates=0;self.alerts=0;self.fast={};self.hist={};self.mood=50;self.pipeline={'radar':0,'selected':0,'analyzed':0,'signals':0,'alerts':0};self.health={'telegram':False,'database':True,'scanner':False,'radar':False,'ai':False}
    async def send(self,text):
        if not BOT_TOKEN or not CHAT_ID or not self.ts:return False
        try:
            async with self.ts.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',json={'chat_id':CHAT_ID,'text':text,'parse_mode':'HTML','disable_web_page_preview':True},timeout=20) as r:
                if r.status!=200:self.health['telegram']=False;log.error('Telegram %s %s',r.status,await r.text());return False
                self.health['telegram']=True;return True
        except Exception:self.health['telegram']=False;log.exception('telegram');return False
    async def start(self):
        await init_db();await self.b.start();self.ts=aiohttp.ClientSession()
        if SEND_STARTUP_MESSAGE:await self.send(f'✅ <b>Ahmed Quantum Entry AI v2 STABLE بدأ العمل</b>\n\n🎛️ الوضع: <b>{ENTRY_MODE}</b>\n🧠 Pre-Explosion + Order Flow First-Reaction\n⏰ 1M/3M للتوقيت — 15M/1H/4H للسياق')
        if SEND_TEST_MESSAGE:await self.send('🧪 <b>رسالة اختبار ناجحة</b>\n\n✅ Telegram\n✅ Railway\n✅ قاعدة البيانات\n✅ المحرك جاهز')
        asyncio.create_task(self.loop());asyncio.create_task(self.track())
    async def close(self):self.running=False;await self.b.close();await self.ts.close()
    async def loop(self):
        while self.running:
            t=time.monotonic();self.scan_no+=1;al=an=0;err=None
            try:al,an=await asyncio.wait_for(self.scan(),timeout=SCAN_TIMEOUT);self.last_error=None;self.health['scanner']=True
            except asyncio.TimeoutError:err=f'scan timeout {SCAN_TIMEOUT}s';self.last_error=err;self.health['scanner']=False;log.error(err)
            except Exception as e:err=repr(e);self.last_error=err;self.health['scanner']=False;log.exception('scan')
            sec=time.monotonic()-t;self.last_scan=now().isoformat();await checkpoint(self.scan_no,self.symbols,self.candidates,an,al,sec,self.mood,err);log.info('scan=%s symbols=%s candidates=%s analyzed=%s alerts=%s mood=%.1f seconds=%.1f',self.scan_no,self.symbols,self.candidates,an,al,self.mood,sec);await asyncio.sleep(max(5,SCAN_SECONDS-sec))
    async def scan(self):
        sy,t=await asyncio.gather(self.b.symbols(),self.b.tickers());self.symbols=len(sy);self.mood=mood(t);allowed=set(sy);rank=[]
        for x in t:
            s=x.get('symbol');q=float(x.get('quoteVolume',0) or 0)
            if s not in allowed or q<MIN_QUOTE_VOLUME:continue
            sc,st=anomaly(x,self.fast.get(s));self.fast[s]=st;rank.append((sc,q,s))
        rank.sort(reverse=True);cand=[x[2] for x in rank[:RADAR_POOL][:DEEP_CANDIDATES]];self.candidates=len(cand);self.health['radar']=True
        async def guard(s):
            try:return await asyncio.wait_for(self.analyze(s),timeout=SYMBOL_TIMEOUT)
            except asyncio.TimeoutError:log.warning('symbol timeout %s',s);return []
            except Exception as e:log.debug('symbol failed %s %r',s,e);return []
        res=await asyncio.gather(*[guard(s) for s in cand]);al=an=sigs=0
        for r in res:
            an+=1;sigs+=len(r)
            for sig in r:
                try:
                    _,chg=await save_signal(sig)
                    if chg:ok=await self.send(message(sig));al+=int(ok);self.alerts+=int(ok)
                except Exception:self.health['database']=False;log.exception('save/send')
        self.health['ai']=an>0;self.pipeline={'radar':len(rank),'selected':len(cand),'analyzed':an,'signals':sigs,'alerts':al}
        return al,an
    async def analyze(self,symbol):
        try:k1,k3,k15,k1h,k4h,oi,depth,prem=await asyncio.gather(self.b.klines(symbol,'1m'),self.b.klines(symbol,'3m'),self.b.klines(symbol,'15m'),self.b.klines(symbol,'1h'),self.b.klines(symbol,'4h'),self.b.oi(symbol),self.b.depth(symbol),self.b.premium(symbol))
        except Exception as e:log.debug('data fetch %s %r',symbol,e);return []
        out=[]
        for direction in ('BUY','SELL'):
            m1,m3,c15,c1,c4=micro(k1,direction),micro(k3,direction),context(k15,direction),context(k1h,direction),context(k4h,direction);o=oi_feat(oi);ob=ob_feat(depth,direction);mood_support=self.mood if direction=='BUY' else 100-self.mood
            pos=clamp(48+max(0,o['change'])*9+(o['accel']-50)*.35);exe=clamp(.3*m1['delta_accel']+.2*m3['delta_accel']+.2*m1['cvd_shift']+.15*c15['cvd_shift']+.15*ob['imbalance']);liq=clamp(.5*ob['imbalance']+.3*ob['absorption']+.2*(100-ob['spoof']));price=clamp(.34*c15['compression']+.18*(70 if c15['vwap_side'] else 40)+.14*(70 if c15['bb_side'] else 42)+.18*m1['delta']+.16*(70 if m1['volume_expansion'] else 42));tim=clamp(.3*(80 if m1['micro_break'] else 45)+.2*(75 if m1['reject'] else 42)+.15*(75 if m1['sweep'] else 42)+.2*m1['delta_accel']+.15*(70 if m1['volume_expansion'] else 42));score=clamp(.18*pos+.29*exe+.18*liq+.2*price+.15*tim+(mood_support-50)*.08)
            fac=[]
            if o['change']>.1:fac.append(f"OI يرتفع {o['change']:+.2f}%")
            if m1['delta_accel']>=58:fac.append('Delta يتسارع')
            if m1['cvd_shift']>=58 or c15['cvd_shift']>=58:fac.append('CVD يتحول قبل السعر')
            if ob['imbalance']>=57:fac.append('دفتر الأوامر داعم')
            if ob['absorption']>=55:fac.append('Absorption محتمل')
            if c15['compression']>=55:fac.append('ضغط سعري')
            if m1['volume_expansion']:fac.append('Volume Expansion')
            if m1['micro_break']:fac.append('أول كسر صغير')
            key=(symbol,direction,'PRE');h=self.hist.setdefault(key,deque(maxlen=PRESSURE_HISTORY));prev=h[-1] if h else score;h.append(score);drift=score-prev;st=stage(score,tim,m1['extension'],len(fac));opp=clamp(.5*score+.3*tim+.2*min(100,55+max(0,1.4-m1['extension'])*25))
            if st and (st!='IGNITION' or drift>=MIN_DRIFT):
                p=plan(direction,m1['price'],max(m1['atr'],m3['atr']),c15['swing_low'],c15['swing_high'])
                out.append(Signal(symbol,direction,st,'PRE_EXPLOSION',score,tim,opp,self.mood,m1['price'],*p,fac or ['توافق ضغط مركب'],{'extension':m1['extension'],'drift':drift}))
            zones=[('15M',zone(k15,direction)),('1H',zone(k1h,direction)),('4H',zone(k4h,direction))];active=[x for x in zones if in_zone(m1['price'],x[1],m1['atr'])]
            if active:
                tf,z=max(active,key=lambda x:x[1]['strength']);rs=clamp(.25*z['strength']+.23*m1['delta_accel']+.18*m1['cvd_shift']+.14*ob['imbalance']+.1*ob['absorption']+.1*tim);rf=[f'منطقة Order Flow على {tf}']
                if m1['reject']:rf.append('رفض سعري')
                if m1['sweep']:rf.append('سحب سيولة')
                if m1['delta_accel']>=56:rf.append('Delta بدأ ينقلب')
                if m1['cvd_shift']>=56:rf.append('CVD بدأ يتحول')
                if ob['absorption']>=55:rf.append('امتصاص الطرف المقابل')
                if m1['micro_break']:rf.append('أول حركة من المنطقة')
                rt=clamp(.35*tim+.25*m1['delta_accel']+.2*(80 if m1['reject'] else 40)+.2*(80 if m1['micro_break'] else 40));key=(symbol,direction,'OF');h=self.hist.setdefault(key,deque(maxlen=PRESSURE_HISTORY));prev=h[-1] if h else rs;h.append(rs);dr=rs-prev;st=stage(rs,rt,m1['extension'],len(rf)-1);opp=clamp(.5*rs+.32*rt+.18*z['strength'])
                if st and (st!='IGNITION' or dr>=MIN_DRIFT):
                    p=plan(direction,m1['price'],max(m1['atr'],m3['atr']),c15['swing_low'],c15['swing_high'],z);out.append(Signal(symbol,direction,st,'ORDER_FLOW_FIRST_REACTION',rs,rt,opp,self.mood,m1['price'],*p,rf,{'extension':m1['extension'],'drift':dr,'zone_tf':tf}))
        if not out:return []
        out.sort(key=lambda x:({'BUILDUP':1,'READY':2,'IGNITION':3}[x.stage],x.opportunity,x.timing),reverse=True);best=out[0]
        if len(out)>1 and best.direction!=out[1].direction and best.opportunity-out[1].opportunity<DIRECTION_GAP:return []
        return [best]
    async def track(self):
        while self.running:
            try:
                async with aiosqlite.connect(DB_PATH) as db:db.row_factory=aiosqlite.Row;rows=await (await db.execute("SELECT * FROM opportunities WHERE status='OPEN' ORDER BY id DESC LIMIT 500")).fetchall()
                if not rows:await asyncio.sleep(60);continue
                prices=await self.b.prices()
                async with aiosqlite.connect(DB_PATH) as db:
                    for r in rows:
                        p=prices.get(r['symbol']);
                        if not p:continue
                        d=r['direction'];mid=(r['entry_low']+r['entry_high'])/2;entered=r['entered_at'] is not None;ts=now().isoformat()
                        if not entered and r['entry_low']<=p<=r['entry_high']:await db.execute("UPDATE opportunities SET entered_at=?,entered_price=?,best_price=?,worst_price=?,updated_at=? WHERE id=?",(ts,p,p,p,ts,r['id']));entered=True
                        if not entered:continue
                        best=r['best_price'] if r['best_price'] is not None else p;worst=r['worst_price'] if r['worst_price'] is not None else p
                        if d=='BUY':best=max(best,p);worst=min(worst,p);hs=p<=r['stop'];h1=p>=r['tp1'];h2=p>=r['tp2'];h3=p>=r['tp3']
                        else:best=min(best,p);worst=max(worst,p);hs=p>=r['stop'];h1=p<=r['tp1'];h2=p<=r['tp2'];h3=p<=r['tp3']
                        up={'best_price':best,'worst_price':worst,'mfe_pct':max(0,pct(best,mid)*(1 if d=='BUY' else -1)),'mae_pct':max(0,pct(worst,mid)*(-1 if d=='BUY' else 1))}
                        if h1 and not r['tp1_at']:up['tp1_at']=ts
                        if h2 and not r['tp2_at']:up['tp2_at']=ts
                        if h3 and not r['tp3_at']:up.update({'tp3_at':ts,'closed_at':ts,'status':'CLOSED','outcome':'TP3'})
                        elif hs and not r['stop_at']:up.update({'stop_at':ts,'closed_at':ts,'status':'CLOSED','outcome':'SL_AFTER_TP' if (r['tp1_at'] or h1) else 'SL'})
                        ss=', '.join(f'{k}=?' for k in up);await db.execute(f"UPDATE opportunities SET {ss},updated_at=? WHERE id=?",(*up.values(),ts,r['id']))
                    await db.commit()
            except Exception:log.exception('tracker')
            await asyncio.sleep(60)

engine=Engine()
@asynccontextmanager
async def lifespan(app):await engine.start();yield;await engine.close()
app=FastAPI(title='Ahmed Quantum Entry AI v2 STABLE',lifespan=lifespan)
@app.get('/health')
async def health():return {'ok':engine.last_error is None and engine.b.health.get('ok',False),'service':'Ahmed Quantum Entry AI v2 STABLE','entry_mode':ENTRY_MODE,'last_scan':engine.last_scan,'last_error':engine.last_error,'scan_number':engine.scan_no,'symbols':engine.symbols,'candidates':engine.candidates,'alerts':engine.alerts,'market_mood':engine.mood,'pipeline':engine.pipeline,'components':{'binance':engine.b.health,**engine.health},'time':now().isoformat()}
@app.get('/test-telegram')
async def test_telegram():
    if not ENABLE_MANUAL_TEST_ENDPOINT:return JSONResponse({'ok':False},status_code=403)
    return {'ok':await engine.send(f'🧪 <b>اختبار يدوي ناجح</b>\n\n✅ Ahmed Quantum Entry AI\n🎛️ {ENTRY_MODE}\n🕒 {now().strftime("%d-%m-%Y %H:%M:%S")}')}
@app.get('/opportunities')
async def opportunities(limit:int=100):
    async with aiosqlite.connect(DB_PATH) as db:db.row_factory=aiosqlite.Row;rows=await (await db.execute('SELECT * FROM opportunities ORDER BY id DESC LIMIT ?',(max(1,min(limit,500)),))).fetchall();return [dict(x) for x in rows]
@app.get('/checkpoints')
async def checkpoints(limit:int=100):
    async with aiosqlite.connect(DB_PATH) as db:db.row_factory=aiosqlite.Row;rows=await (await db.execute('SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?',(max(1,min(limit,500)),))).fetchall();return [dict(x) for x in rows]
@app.get('/stats')
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:db.row_factory=aiosqlite.Row;o=await (await db.execute("SELECT COUNT(*) total,SUM(status='OPEN') open_count,SUM(outcome='TP3') tp3,SUM(outcome='SL') sl,AVG(mfe_pct) avg_mfe,AVG(mae_pct) avg_mae FROM opportunities")).fetchone();g=await (await db.execute("SELECT engine,direction,current_stage,COUNT(*) cases,SUM(outcome='TP3') tp3,SUM(outcome='SL') sl FROM opportunities GROUP BY engine,direction,current_stage")).fetchall();return {'overall':dict(o),'groups':[dict(x) for x in g]}
@app.get('/',response_class=HTMLResponse)
async def dashboard():
    h=await health();return f'''<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Ahmed Quantum Entry AI v2 STABLE</title><style>body{{font-family:Arial;background:#0a1020;color:#edf2ff;padding:24px}}.card{{background:#151d33;border:1px solid #2b3658;border-radius:16px;padding:18px;margin:12px}}a{{color:#8db9ff}}</style></head><body><h1>Ahmed Quantum Entry AI v2 STABLE</h1><div class="card">الحالة: {'يعمل ✅' if h['ok'] else 'خطأ ⚠️'}<br>الوضع: {ENTRY_MODE}<br>العقود: {h['symbols']}<br>المرشحون: {h['candidates']}<br>التنبيهات: {h['alerts']}<br>مزاج السوق: {h['market_mood']:.1f}</div><div class="card"><a href="/health">Health</a> · <a href="/test-telegram">Test</a> · <a href="/opportunities">Opportunities</a> · <a href="/stats">Stats</a> · <a href="/checkpoints">Checkpoints</a></div></body></html>'''
if __name__=='__main__':uvicorn.run('app:app',host='0.0.0.0',port=PORT,log_level='info')

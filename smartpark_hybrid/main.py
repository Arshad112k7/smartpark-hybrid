import os, uuid, random, asyncio
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, String, Integer, DateTime, ForeignKey, select, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

ROOT=Path(__file__).resolve().parent
URL=os.getenv('DATABASE_URL','sqlite:///./smartpark_dev.db')
if URL.startswith('postgres://'): URL=URL.replace('postgres://','postgresql+psycopg://',1)
if URL.startswith('postgresql://'): URL=URL.replace('postgresql://','postgresql+psycopg://',1)
engine=create_engine(URL, pool_pre_ping=True, connect_args={'check_same_thread':False} if URL.startswith('sqlite') else {})
Session=sessionmaker(engine,expire_on_commit=False)
now=lambda: datetime.now(timezone.utc)
INTERVAL=int(os.getenv('SIM_INTERVAL_SECONDS','15'))
sim_on=os.getenv('SIMULATION_ENABLED','true').lower()=='true'

class Base(DeclarativeBase): pass
class Slot(Base):
    __tablename__='slots'
    id:Mapped[str]=mapped_column(String(12),primary_key=True)
    floor:Mapped[str]=mapped_column(String(2),index=True)
    number:Mapped[int]=mapped_column(Integer)
    status:Mapped[str]=mapped_column(String(12),index=True)
    vehicle:Mapped[Optional[str]]=mapped_column(String(32),nullable=True)
    updated_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=now)
class Booking(Base):
    __tablename__='bookings'
    id:Mapped[str]=mapped_column(String(36),primary_key=True,default=lambda:str(uuid.uuid4()))
    token:Mapped[str]=mapped_column(String(24),unique=True,index=True)
    user:Mapped[str]=mapped_column(String(80))
    slot_id:Mapped[str]=mapped_column(ForeignKey('slots.id'),index=True)
    status:Mapped[str]=mapped_column(String(16),default='active')
    created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=now)
    expires_at:Mapped[datetime]=mapped_column(DateTime(timezone=True))
class Event(Base):
    __tablename__='events'
    id:Mapped[int]=mapped_column(Integer,primary_key=True,autoincrement=True)
    kind:Mapped[str]=mapped_column(String(24),index=True)
    message:Mapped[str]=mapped_column(String(240))
    slot_id:Mapped[Optional[str]]=mapped_column(String(12),nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=now)

class Hub:
    def __init__(self): self.clients=set()
    async def add(self,w): await w.accept(); self.clients.add(w)
    def remove(self,w): self.clients.discard(w)
    async def send(self,payload):
        dead=[]
        for w in list(self.clients):
            try: await w.send_json(payload)
            except Exception: dead.append(w)
        for w in dead: self.remove(w)
hub=Hub()

class BookingIn(BaseModel):
    slot_id:str=Field(min_length=5,max_length=12)
    user_label:str=Field(min_length=1,max_length=80)
    duration_minutes:int=Field(default=60,ge=5,le=240)
class SimulationIn(BaseModel): enabled:bool

def log(db,kind,msg,slot=None): db.add(Event(kind=kind,message=msg,slot_id=slot,created_at=now()))
def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        if db.scalar(select(func.count(Slot.id)))==0:
            for floor in ('P1','P2','P3'):
                states=['available']*4+['occupied']*5+['reserved']*3
                random.Random(floor).shuffle(states)
                for n,status in enumerate(states,1):
                    db.add(Slot(id=f'{floor}-{n:02d}',floor=floor,number=n,status=status,vehicle=f'TN 07 AB {1200+n}' if status=='occupied' else None,updated_at=now()))
            log(db,'system','SmartPark demo environment initialized'); db.commit()

def overview(db):
    slots=db.scalars(select(Slot).order_by(Slot.floor,Slot.number)).all()
    floors={}
    for f in ('P1','P2','P3'):
        floors[f]={s:sum(x.floor==f and x.status==s for x in slots) for s in ('available','occupied','reserved')}
    counts={s:sum(x.status==s for x in slots) for s in ('available','occupied','reserved')}
    return {'total':len(slots),**counts,'floors':floors,'slots':[{'id':x.id,'floor':x.floor,'number':x.number,'status':x.status,'vehicle':x.vehicle} for x in slots],'simulation_enabled':sim_on,'simulation_interval_seconds':INTERVAL}
async def broadcast(event_type='state_changed'):
    with Session() as db: data=overview(db)
    await hub.send({'type':event_type,'data':data})

def set_sim(value):
    global sim_on; sim_on=value
    with Session() as db: log(db,'system',f'Automatic simulation {"started" if value else "paused"}'); db.commit()

async def simulator():
    while True:
        await asyncio.sleep(INTERVAL)
        if not sim_on: continue
        with Session() as db:
            rows=db.scalars(select(Slot).where(Slot.status.in_(['available','occupied']))).all()
            if not rows: continue
            s=random.choice(rows)
            if s.status=='available':
                s.status='occupied'; s.vehicle=f'TN 07 AB {random.randint(1000,9999)}'; log(db,'vehicle_entry',f'Simulated vehicle entered {s.id}',s.id)
            else:
                s.status='available'; s.vehicle=None; log(db,'vehicle_exit',f'Simulated vehicle exited {s.id}',s.id)
            s.updated_at=now(); db.commit()
        await broadcast('simulation_event')

@asynccontextmanager
async def lifespan(app):
    seed(); task=asyncio.create_task(simulator())
    yield
    task.cancel()
    try: await task
    except asyncio.CancelledError: pass
app=FastAPI(title='SmartPark API',version='1.0.0',lifespan=lifespan)
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
@app.get('/')
def home(): return FileResponse(ROOT/'static'/'index.html')
@app.get('/health')
def health(): return {'status':'ok','service':'smartpark-api','time':now().isoformat()}
@app.get('/api/overview')
def get_overview():
    with Session() as db: return overview(db)
@app.get('/api/events')
def events(limit:int=25):
    with Session() as db:
        rows=db.scalars(select(Event).order_by(Event.id.desc()).limit(max(1,min(limit,100)))).all()
        return [{'id':e.id,'type':e.kind,'message':e.message,'slot_id':e.slot_id,'created_at':e.created_at.isoformat()} for e in rows]
@app.post('/api/bookings')
async def booking(req:BookingIn):
    with Session() as db:
        # PostgreSQL uses row-level locking. For SQLite local demos, the database serializes writes.
        slot=db.scalar(select(Slot).where(Slot.id==req.slot_id).with_for_update())
        if not slot: raise HTTPException(404,'Parking slot not found')
        if slot.status!='available': raise HTTPException(409,f'{slot.id} is {slot.status}; choose an available slot')
        slot.status='reserved'; slot.updated_at=now()
        token='SPK-'+uuid.uuid4().hex[:8].upper()
        b=Booking(token=token,user=req.user_label.strip(),slot_id=slot.id,status='active',created_at=now(),expires_at=now()+timedelta(minutes=req.duration_minutes))
        db.add(b); log(db,'booking_created',f'{req.user_label} reserved {slot.id}',slot.id); db.commit()
        result={'booking_id':b.id,'token':token,'slot_id':slot.id,'user':b.user,'status':b.status,'expires_at':b.expires_at.isoformat()}
    await broadcast('booking_created'); return result
@app.post('/api/simulation')
async def simulation(req:SimulationIn):
    set_sim(req.enabled); await broadcast('simulation_status'); return {'simulation_enabled':sim_on}
@app.post('/api/simulation/event')
async def vehicle_event():
    with Session() as db:
        rows=db.scalars(select(Slot).where(Slot.status.in_(['available','occupied']))).all()
        if not rows: raise HTTPException(409,'No eligible parking slots')
        s=random.choice(rows)
        if s.status=='available': s.status='occupied'; s.vehicle=f'TN 07 AB {random.randint(1000,9999)}'; log(db,'vehicle_entry',f'Manual demo arrival at {s.id}',s.id)
        else: s.status='available'; s.vehicle=None; log(db,'vehicle_exit',f'Manual demo departure from {s.id}',s.id)
        s.updated_at=now(); changed=s.id; db.commit()
    await broadcast('manual_vehicle_event'); return {'changed_slot':changed}
@app.post('/api/simulation/reset')
async def reset():
    with Session() as db:
        for f in ('P1','P2','P3'):
            states=['available']*4+['occupied']*5+['reserved']*3; random.Random(f).shuffle(states)
            for s in db.scalars(select(Slot).where(Slot.floor==f)).all():
                s.status=states[s.number-1]; s.vehicle=f'TN 07 AB {1200+s.number}' if s.status=='occupied' else None; s.updated_at=now()
        for b in db.scalars(select(Booking).where(Booking.status=='active')).all(): b.status='reset'
        log(db,'system','Demo facility reset to baseline occupancy'); db.commit()
    await broadcast('simulation_reset'); return {'status':'reset'}
@app.websocket('/ws')
async def websocket(websocket:WebSocket):
    await hub.add(websocket)
    try:
        with Session() as db: await websocket.send_json({'type':'initial_state','data':overview(db)})
        while True: await websocket.receive_text()
    except WebSocketDisconnect: hub.remove(websocket)

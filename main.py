from typing import List, Optional
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# Ini connection info-nya, jangan lupa diganti ya~ 💖
db_params = {
    'host': 'us-east-1.0f5d9b46-10a7-4c9c-8d86-2c88c29da69e.aws.ybdb.io',
    'port': '5433',
    'dbName': 'yugabyte',
    'dbUser': 'admin',
    'dbPassword': 'vOxSfT8qN7Gaw7BW0dHPTB6WODvHJQ',
    'sslMode': '',
    'sslRootCert': 'rootAug.crt'
}

class Risk(BaseModel):
    base: str
    code: str
    risk: str
    sasaran: str
    resiko: str
    penyebab: str
    preventif: str
    dampak: str
    korektif: str

class SubItem(BaseModel):
    base: str
    code: str
    activity: str
    goal: str
    check: str
    notes: str
    percentage: int
    risk: List[Risk]

class SubmitItem(BaseModel):
    base: str
    code: str
    check: str
    notes: str
    

class Item(BaseModel):
    base: str
    color: Optional[str]
    percentage: Optional[int]
    title: Optional[str]
    description: Optional[str]
    SubItem: List[SubItem]

@app.post("/")
async def get_actDat(dat: SubmitItem):
    try:
        if db_params['sslMode'] != '':
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  sslmode=db_params['sslMode'], sslrootcert=db_params['sslRootCert'],
                                  connect_timeout=10)
        else:
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  connect_timeout=10)
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        print(e)
        exit(1)
    with yb.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as yb_cursor:
        yb_cursor.execute("update public.kmpa_subitem set \"check\"='"+ dat.check +"', notes='"+ dat.notes +"' where base='"+ dat.base +"' and code='"+ dat.code +"' ")
    yb.commit()
    yb.close()    
    return "Success"    


@app.get("/")
async def get_actDat(base: str,type: str):
    try:
        if db_params['sslMode'] != '':
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  sslmode=db_params['sslMode'], sslrootcert=db_params['sslRootCert'],
                                  connect_timeout=10)
        else:
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  connect_timeout=10)
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        print(e)
        exit(1)
    itemDat: List(Item)
    itemDat = []
    if base == "0":
         with yb.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as yb_cursor:
            yb_cursor.execute("select a.base,a.color,case when jum>0 then round(jum*100/cnt,0) else 0 end percentage,a.title,a.description from public.kmpa_item a, (select type,base,sum(case when \"check\"='Y' then 1 else 0 end) jum, count(code) cnt from public.kmpa_subitem group by type,base) b where a.type=b.type and a.type = '"+ type +"' and a.base=b.base order by a.base")

            results = yb_cursor.fetchall()
            for row in results:
                itemData = Item(base=row["base"],color=row["color"],percentage=row["percentage"],title=row["title"],description=row["description"],SubItem=[])
                itemDat.append(itemData)
    else:
        with yb.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as yb_cursor:
            yb_cursor.execute("select case when \"check\"='Y' then round(1*100/cnt,0) else 0 end qpercentage, a.base qbase, a.code qcode, a.activity qactivity, a.goal qgoal, a.check qcheck, a.notes qnotes from public.kmpa_subitem a, (select base basep,sum(case when \"check\"='Y' then 1 else 0 end) jum, count(code) cnt from public.kmpa_subitem group by base) b where a.base=b.basep and a.base='"+base+"' order by a.code")

            results = yb_cursor.fetchall()
            itemData= Item(base=base,color="",percentage=0,title="",description="",SubItem=[])
            for row in results:
                subData= SubItem(base=row["qbase"],code=row["qcode"],activity=row["qactivity"],goal=row["qgoal"],check=row["qcheck"],notes=row["qnotes"],percentage=row["qpercentage"],risk=[])
                itemData.SubItem.append(subData)
            itemDat.append(itemData)
        for rowData in itemDat[0].SubItem:
            with yb.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as yb_cursor:
                yb_cursor.execute("select * from public.kmpa_risk kr where base = '"+rowData.base+"' and code = '"+rowData.code+"'")

                results = yb_cursor.fetchall()
                for row in results:
                    riskData= Risk(base=row["base"],code=row["code"],risk=row["risk"],sasaran=row["sasaran"],resiko=row["resiko"],penyebab=row["penyebab"],preventif=row["preventif"],dampak=row["dampak"],korektif=row["korektif"])
                    rowData.risk.append(riskData)
    yb.close()    
    return itemDat

@app.get("/home")
async def get_homeDat():
    try:
        if db_params['sslMode'] != '':
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  sslmode=db_params['sslMode'], sslrootcert=db_params['sslRootCert'],
                                  connect_timeout=10)
        else:
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  connect_timeout=10)
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        print(e)
        exit(1)
    itemDat: List(Item)
    itemDat = []
    with yb.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as yb_cursor:
        yb_cursor.execute("select type base,'yellow' color,round(sum(case when \"check\"='Y' then 1 else 0 end)*100/count(code),0)  percentage, case type when 'X2.1'  then 'Pengelolaan Audit' when 'X2.2' then 'Pelaksanaan Audit' when 'X2.3' then 'Kriteria Audit' end title,'' description from public.kmpa_subitem where type is not null group by type order by type")

        results = yb_cursor.fetchall()
        for row in results:
            itemData = Item(base=row["base"],color=row["color"],percentage=row["percentage"],title=row["title"],description=row["description"],SubItem=[])
            itemDat.append(itemData)
   
    yb.close()    
    return itemDat


@app.get("/bim")
async def get_bimDat(id: str):
    urn: str = ""
    try:
        if db_params['sslMode'] != '':
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  sslmode=db_params['sslMode'], sslrootcert=db_params['sslRootCert'],
                                  connect_timeout=10)
        else:
            yb = psycopg2.connect(host=db_params['host'], port=db_params['port'], database=db_params['dbName'],
                                  user=db_params['dbUser'], password=db_params['dbPassword'],
                                  connect_timeout=10)
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        print(e)
        exit(1)
    with yb.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as yb_cursor:
        yb_cursor.execute("select urn from kmpa_model where id = '"+ id +"' ")

        results = yb_cursor.fetchall()
        for row in results:
            urn = row["urn"]
   
    yb.close()    
    return urn
package.path='/opt/tarantool/?.lua;/opt/tarantool/?/init.lua;/opt/TarantoolModules/?.lua;/opt/TarantoolModules/?/init.lua;'..package.path

box.cfg{
  listen=3301,
  work_dir='/var/lib/tarantool',
  wal_dir = 'WAL',
  memtx_dir = 'MemTX',
  vinyl_dir = 'Vinyl'
}

local Cfg=require("Config")

local console=require('console')
console.listen('/var/lib/tarantool/admin.sock')
local log=require('log')
local json=require('json')
local crypto=require('crypto')
local digest=require('digest')
local clock=require('clock')

--Spaces
local Click=box.space.Click

--functions
local function RunWithAdmin(InternalKey,CallBack)
  if InternalKey==Cfg.InternalKey then
    return CallBack()
  else
    return 409
  end
end

box.once("schema",function()
  -- основной пользователь
  box.schema.user.passwd(Cfg.Tarantool.AdminPassword)

  Click=box.schema.space.create('Click',{engine="vinyl",format={
    {name='ID',type='unsigned'},
    {name='x',type='integer'},
    {name='y',type='integer'},
    {name='timestamp',type='number'},
    {name='page',type='string'},
    {name='metaData',type='scalar',is_nullable=true},
    --unsigned|string|integer|number|boolean|array|scalar
  }})
  box.schema.sequence.create('ClickSeq')
  Click:create_index('primary',{unique=true,parts={{1,"unsigned"}},sequence='ClickSeq'})
  Click:create_index('timestamp',{unique=false,parts={{5,'string'},{4,"number"}}})
end)

function SaveClick(x,y,page,metaData)
    Click:insert{nil,tonumber(x),tonumber(y),tonumber(string.sub(tostring(clock.time64()),1,-13)),page,metaData and json.encode(metaData)}
    return 200
end

function GetClicksOfTime(InternalKey,timestamps,page)
  return RunWithAdmin(InternalKey,function()
    local Data={}
    for _,i in Click.index.timestamp:pairs({page,tonumber(timestamps.min)},{iterator="GE"}) do
       if i[4]>tonumber(timestamps.max) or i[5]~=page then
         break
       end
       Data[#Data+1]={ID=i[1],x=i[2],y=i[3],timestamp=i[4],page=i[5],metaData=i[6] and json.decode(i[6])}
    end
    return 200,Data
  end)
end

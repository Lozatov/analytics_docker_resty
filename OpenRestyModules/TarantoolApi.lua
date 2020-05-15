local Calls=require("TarantoolApi_Calls")
local bit=require('bit')
local MsgPack=require("cmsgpack")
local inspect=require("inspect")

local Cfg=require("Config")

local Module={}

for Name,_ in pairs(Calls) do
  Module[Name]=function(arg)
    local function TarantoolReq(Socket,Code,Body)
      Head=MsgPack.pack({[0]=Code,[1]=0})
      BBody=MsgPack.pack(Body)
      local len=MsgPack.pack(#Head+#BBody)
      local _,err=Socket:send(len)
      if err then
        return nil,nil,true
      end
      Socket:send(Head)
      Socket:send(BBody)

      local Data=Socket:receive(6)
      local Offset,length=MsgPack.unpack_one(Data)
      local Data=Data..Socket:receive(Offset+length-6)
      local Offset,Head=MsgPack.unpack_one(Data,Offset)
      local Offset,Data=MsgPack.unpack_one(Data,Offset)
      return Head[0],Data
    end

    local Socket,err,res
    local retry
    repeat
      retry=false
      Socket=ngx.socket.tcp()
      Socket:settimeout(Cfg.Tarantool.Timeout or 1000)
      Socket:connect(Cfg.Tarantool.Host or 'tarantool',Cfg.Tarantool.Port or 3301)

      --init
      local ReUses=Socket:getreusedtimes()
      if not ReUses or ReUses==0 then
        local Version,err=Socket:receive(64) do
          ngx.log(err and ngx.ERR or ngx.INFO,err or "connected to tarantool version:"..Version)
        end
        local BinSalt=Socket:receive(44)
        retry=not BinSalt
        if not retry then
          local Salt=ngx.decode_base64(BinSalt)
          Socket:receive(20)

          --Auth
          local first=ngx.sha1_bin(Cfg.Tarantool.AdminPassword)
          local Salt2=""
          for i=1,20 do
            Salt2=Salt2..string.char(string.byte(Salt,i))
          end
          local last=ngx.sha1_bin(Salt2..ngx.sha1_bin(first))
          local res=""
          for i=1,20 do
            res=res..string.char(bit.bxor(string.byte(first,i),string.byte(last,i)))
          end
          local err,res=TarantoolReq(Socket,7,{[35]='admin',[33]={"chap-sha1",res}})
          if err==0 then
            --ngx.log(ngx.ERR,"Login Successfull!!!!!!!!!!!!!!!!!!!!!!")
          else
            ngx.log(ngx.ERR,'Error OR->T:{"Name":"Tarantool does not work","Data":'..(res[49] or '').."}")
            return 500
          end
        end
      end
      if not retry then
        err,res,retry=TarantoolReq(Socket,10,{[34]=Name,[33]=arg})
      end
      if retry then
        Socket:close()
      end
    until not retry
    Socket:close()

    if err==0 then
      --ngx.log(ngx.ERR,'Error OR->T:{"Name":"Tarantool does not work","Data":'..inspect(res[48]).."}")
      return res[48][1],res[48][2]
    else
      ngx.log(ngx.ERR,'Error OR->T:{"Name":"Tarantool does not work","Data":'..res[49].."}")
      return 500
    end
  end
end

return Module

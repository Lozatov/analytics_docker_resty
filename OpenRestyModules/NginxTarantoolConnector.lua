local Calls=require("TarantoolApi_Calls")
local MsgPack=require("cmsgpack")
local TarantoolApi=require("TarantoolApi")
local cjson=require("cjson")

return function(ngx)
  ngx.req.read_body()
  local body = ngx.req.get_body_data()

  local Call=string.sub(ngx.var.uri,6)
  local CallData=Calls[Call]
  if not CallData then
    Call=string.sub(ngx.var.uri,6):sub(1,-2)
    CallData=Calls[Call]
  end
  local ParsedBody
  ngx.log(ngx.INFO,CallData)
  if CallData then
    if body and not pcall(function()
          ParsedBody="PUT"==ngx.req.get_method()and MsgPack.unpack(body)or cjson.decode(body)
          ngx.header.content_type ="PUT"==ngx.req.get_method()and 'application/x-msgpack'or 'application/json'
        end)then
      ngx.status=400
      ngx.print('{"Error":{"Name":"No json"}}')
    else
      local Params={}
      local PreParams=ngx.req.get_uri_args(0)
      for i,k in pairs(CallData) do
        Params[i]=PreParams and PreParams[k]or ParsedBody and ParsedBody[k] or nil
      end

      local result1,result2=TarantoolApi[Call](Params)
      if result1~=500 then
        ngx.status=result1
        if not result2 then
          if "PUT"==ngx.req.get_method() then
            ngx.eof()
          else
            ngx.print("{}")
          end
        elseif type(result2) == "string" then
          if "PUT"==ngx.req.get_method() then
            ngx.print(MsgPack.pack(result2))
          else
            ngx.header["content_type"] = "text/plain"
            ngx.print(result2)
          end
        elseif type(result2) == "table" then
          ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack(result2)or cjson.encode(result2))
        elseif type(result2) == "number" then
          ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack(result2)or tostring(result2))
        else
          ngx.status = 500
          ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack({Error={Name="Unexpected response from Tarantool"}})or '{"Error":{"Data":"Unexpected response from Tarantool"}}')
        end
      else
        ngx.status=500
        ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack({Error={Name="Unexpected response from Tarantool"}})or '{"Error":{"Data":"Unexpected response from Tarantool"}}')
      end
    end
  else
    ngx.status=405
    ngx.print("PUT"==ngx.req.get_method()and MsgPack.pack({Error={Name="Method Not Allowed"}})or '{"Error":{"Name":"Method Not Allowed"}}')
  end
end

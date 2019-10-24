#!/usr/bin/env tarantool

box.cfg{
    listen = 3301,
    background = true,
    log = 'KV.log',
    pid_file = 'KV.pid'
}
s = box.schema.space.create('KV')
s:format({  {name = 'key', type = 'string'}, {name = 'value', type = 'string'}  })
s:create_index('primary', { type = 'hash', parts = {'key'}   })
box.schema.user.grant('guest', 'read,write,execute', 'universe')
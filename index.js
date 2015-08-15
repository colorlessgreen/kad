var secureRandom = require('secure-random'),
    xor = require('bitwise-xor')    

var KEY_SIZE = 256,
    K = 20

var conf = {
    nodeId: secureRandom(KEY_SIZE/8, {type: 'Buffer'}).toString('hex'),
    port: process.env.PORT,
    seeds: process.argv.slice(2),
    msgTimeout: 3000
}

var server = require('dgram').createSocket('udp4'),
    sentMsgs = {}

var kBuckets = (function() {
    var buckets = []
    for (var i=0; i<KEY_SIZE; i++)
        buckets.push([])
    return buckets
})(KEY_SIZE)

function distance(key1, key2) {
    var dist = xor(new Buffer(key1, 'hex'), new Buffer(key2, 'hex'))
    for (var i=0; i<dist.length; i++) {
        if (dist[i]) {
            var bitString = dist[i].toString(2)
            var index = 255 - i*8 - (8 - bitString.length)
            return index
        }
    }
    return 0
}

function orderByDistance(key1, key2) {
    var d1 = distance(conf.nodeId, key1),
        d2 = distance(conf.nodeId, key2)
    if (d1 < d2) return -1
    if (d1 > d2) return  1
    return 0
}

function insertNode(bucket, node) {
    var i=0
    for (; i<bucket.length; i++) {

        if (bucket[i].nodeId == node.nodeId)
            return // dupe

        var d1 = distance(conf.nodeId, bucket[i].nodeId)
        var d2 = distance(conf.nodeId, node.nodeId)
        if (d2 < d1) {
            break
        }
    }

    if (i < K) {
        bucket.splice(i, 0, node)
        return true
    }
}

function addNode(node) {
    var index = distance(conf.nodeId, node.nodeId),
        bucket = kBuckets[index]
    insertNode(bucket, node)
}

function closestNodes(key, n) {
    var d = distance(conf.nodeId, key),
        nodes = kBuckets[d].slice(0, K),
        i = 1

    while (nodes.length < n) {
        var moreNodes = [],
            added = false
        
        if (d-i > 0) {
            moreNodes = moreNodes.concat(kBuckets[d-i].slice(0, K))
            added = true
        }
        
        if (d+i < KEY_SIZE) {
            moreNodes = moreNodes.concat(kBuckets[d+i].slice(0, K))
            added = true
        }

        if (!added) break
        nodes = nodes.concat(moreNodes.sort(orderByDistance))
        i+=1
    }

    if (nodes.length > n) { nodes.splice(0, n) }
    return nodes
}

function lookup(key, callback) {
    var kNodes = closestNodes(key, K),
        queried = {}

    function queryNodes(nodes, callback) {
        var found = [],
            count = nodes.length
        
        nodes.forEach(function(node) {
            queried[node.nodeId] = true
            sendFindNode(node, key, function(resp) {
                if (resp) { found = found.concat(resp.result) }
                count -= 1; if (count == 0) {
                    callback(found.sort(orderByDistance))
                }
            })
        })
    }

    function performRound(nodes) {
        queryNodes(nodes, function(resultNodes) {
            var added = resultNodes.filter(function(node) {
                if (node.nodeId != conf.nodeId)
                    return insertNode(kNodes, node)
            })
            
            var nextRound = kNodes.filter(function(node) {
                !queried[node.nodeId]
            })

            if (nextRound.length == 0) {
                console.log(kNodes)
                callback(kNodes)
            }
            
            if (added.length > 0)
                performRound(nextRound.slice(0, 3))
            else
                performRound(nextRound)
        })
    }
    
    performRound(kNodes.slice(0, 3))
}


function sendMsg(node, msg, timeout, callback) {
    if (!msg.id) {
        msg.id = secureRandom(32, {type: 'Buffer'}).toString('hex')
    }

    msg.sender = {nodeId: conf.nodeId, host: conf.host, port: conf.port}
    var buf = new Buffer(JSON.stringify(msg))
    
    server.send(buf, 0, buf.length, node.port, node.host, function(err) {
        if (err) {
            console.log(err)            
            if (callback) {
                callback()
            }
        } else {
            console.log('<- %s %s:%s', msg.cmd, node.host, node.port)
            if (callback) {
                sentMsgs[msg.id] = callback
                if (timeout == 0)
                    timeout = conf.msgTimeout
                if (timeout > 0) {
                    setTimeout(function() {
                        if (sentMsgs[msg.id]) {
                            delete sentMsgs[msg.id]
                            callback()
                        }
                    }, timeout)
                }
            }
        }
    })
}

function pingNode(node) {
    sendMsg(node, {cmd: 'PING'}, 0, function(resp) {
        if (resp && resp.cmd == 'PONG') {
            addNode(resp.sender)
        }
    })
}

function sendFindNode(node, key, callback) {
    sendMsg(node, {cmd: 'FIND_NODE', args: [key]}, 0, callback)
}


server.on('message', function(buf, rinfo) {
    var msg = JSON.parse(buf.toString())
    console.log('-> %s %s:%s', msg.cmd, msg.sender.host, msg.sender.port)

    if (msg.cmd == 'PING') {
        addNode(msg.sender)
        sendMsg(msg.sender, {id: msg.id, cmd: 'PONG'})
        
    } else if (msg.cmd == 'FIND_NODE') {
        sendMsg(msg.sender, {
            id: msg.id,
            cmd: 'FIND_NODE_RESULT',
            result: closestNodes(msg.args[0], K)
        })
        
    } else if (sentMsgs[msg.id]) {
        var callback = sentMsgs[msg.id]
        delete sentMsgs[msg.id]
        callback(msg)
    }
})

server.on('listening', function() {
    conf.host = server.address().address
    conf.port = server.address().port

    console.log('NodeId: %s', conf.nodeId)
    console.log('Address: %s:%s', conf.host, conf.port)

    conf.seeds.forEach(function(seed) {
        pingNode({host: seed.split(':')[0], port:seed.split(':')[1]})
    })

    setTimeout(function() {
        lookup(conf.nodeId, function(nodes) { nodes.forEach(addNode) })
    }, 3000)
})

server.bind(conf.port)


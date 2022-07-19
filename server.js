const WebSocketServer = require('ws');
var amqp = require('amqplib/callback_api');

chart_clients = [];

var amqpConn = null;

var rmOptions = {
    protocol: 'amqp',
    hostname: '64.227.173.41', 
    port: 5672,
    username: 'ds',
    password: 'windows2020',
    locale: 'en_US',
    frameMax: 0,
    heartbeat: 0,
    vhost: '/',
  }; 

function startampq() {
    amqp.connect(rmOptions, function(err, conn) {
      if (err) {
        console.error("[AMQP]", err.message);
        return;
      }
      conn.on("error", function(err) {
        if (err.message !== "Connection closing") {
          console.error("[AMQP] conn error", err.message);
        }
      });
      conn.on("close", function() {
        console.error("[AMQP] reconnecting");
        return setTimeout(startampq, 1000);
      });
      console.log("[AMQP] connected");
      amqpConn = conn;
      //startWorker('SILVERMIC21APR') 
    });
}

function closeOnErr(err) {
    if (!err) return false;
    console.error("[AMQP] error", err);
    amqpConn.close();
    return true;
  }

function processMsg(data) {
     
    try 
    {
        var message = JSON.parse(data.content.toString());
         
        var output = {type:"tick-update",t: message.ts,price: message.ltp,
                    volume: message.volume, exchange: message.exchange, 
                    tradingsymbol: message.tradingsymbol, change: message.change,change_percent: message.change_percent} ;           
         
        chart_clients.forEach(function (client) {           
            if (client.symbols.indexOf(message.exchange+"-"+message.tradingsymbol) > -1){                 
                client.socket.send(JSON.stringify(output));                
            }
        });

    }
    catch(er)
    {
        console.log(er);
    }
     
}

function startConsumer(qname, callback) {

    amqpConn.createChannel(function(err, ch) {
        if (closeOnErr(err)) return;
        ch.on("error", function(err) {
            console.error("[AMQP] channel error", err.message);
        });
        ch.on("close", function() {
            console.log("[AMQP] channel closed");
        });
        
        var exchange = 'ticks';
        ch.assertExchange(exchange, 'direct', {
            durable: false
        });
         
        ch.assertQueue(qname, { durable: false, autoDelete: true }, function(err, _ok) {
            if (closeOnErr(err)) return;
            ch.consume(qname, processMsg, { noAck: true , consumerTag: qname});
            //ch.cancel(qname);
            console.log("Consumer started: ", qname);
        });

        ch.bindQueue(qname, exchange, qname);

        callback(ch); 
    });
}

startampq();

// Creating a new websocket server
const wss = new WebSocketServer.Server({ port: 3000, clientTracking: true})


// Creating connection using websocket
wss.on("connection", function connection(ws, req) {
    console.log("new client connected ", req.headers['sec-websocket-key']);
     
    chart_clients.push({id: req.headers['sec-websocket-key'], socket: ws, symbols : [], ch: null});
    //console.log(chart_clients);                                         16485560400
    //#output = {id:"chart",type:"overlay-update",data:[{id:"chart",data:[1646238720,15120,15159,15081,15139,57]}]} ;
    // ws.send(JSON.stringify(output));

    // sending message
    ws.on("message", data => {
        
        let message;
        try {
            message = JSON.parse(data);
            console.log(message)
            if (message.type == 'subscribe') {                
                chart_clients.forEach(function (client) {
                     
                    if(client.id == req.headers['sec-websocket-key'])
                    {    
                        if (client.symbols.indexOf(message.symbol) > -1){
                             
                        }
                        else 
                        {
                            startConsumer(message.symbol,function(ch){
                                client.ch = ch;
                            }); 
                             
                            client.symbols.push(message.symbol);
                            console.log(message.symbol,' subscribed');
                        }
                    }                    
                });
            }
            if (message.type == 'unsubscribe') {  
                chart_clients.forEach(function (client,index,object) {                     
                    if(client.id == req.headers['sec-websocket-key'])
                    { 
                        var index = client.symbols.indexOf(message.symbol);
                        if (index !== -1) {
                            client.symbols.splice(index, 1);
                            console.log(message.symbol,' unsubscribed');
                        } 
                        client.ch.cancel(message.symbol);
                        //object.splice(index, 1);                         
                    }                    
                });
            }
        } catch (e) {
            sendError(ws, 'Wrong format');
            return;
        }


    });
    // handling what to do when clients disconnects from server
    ws.on("close", () => {
        chart_clients.forEach(function (client,index,object) {             
            if(client.id == req.headers['sec-websocket-key'])
            {  
                client.symbols.forEach(function(symbol){
                    client.ch.cancel(symbol);
                });

                object.splice(index, 1);
            }
        });
        //console.log(chart_clients);
        console.log("the client has disconnected");
    });
    // handling client connection error
    ws.onerror = function () {
        console.log("Some Error occurred")
    }
});

const sendError = (ws, message) => {
    const messageObject = {
      type: 'ERROR',
      payload: message,
    };
  
    ws.send(JSON.stringify(messageObject));
  };

console.log("Data Subscription Server is running on port 3000");
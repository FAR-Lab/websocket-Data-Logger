#!/usr/bin/env node
var port = 8123;
var fileStorageAddress= "./"
var fileStorageBackupAddress="./BackUp/" //We want to point this at the NAS
var keyValueName = 'k';
var timeValueName ='t';
var valueName = 'v';
///Code///
var WebSocketServer = require('websocket').server;
var http = require('http');

var server = http.createServer(function(request, response) {
   console.log((new Date()) + ' Received request for ' + request.url);
    response.writeHead(404);
    response.end();
});
server.listen(8123, function() {
    console.log((new Date()) + ' Server is listening on port 8080');
});

wsServer = new WebSocketServer({
    httpServer: server,
    //David on September 11 2018 => never!! :-)
    // You should not use autoAcceptConnections for production
    // applications, as it defeats all standard cross-origin protection
    // facilities built into the protocol and the browser.  You should
    // *always* verify the connection's origin and decide whether or not
    // to accept it.
    autoAcceptConnections: false
});



wsServer.on('request', function(client) {
 console.log('New Client');
 var keyValueLine = [];
 keyValueLine.push(timeValueName);// the first value should always be time
 var outputLine=[];

 var connection = request.accept('echo-protocol', request.origin);
console.log((new Date()) + ' Connection accepted.');

    connection.on('message', function(message){
        var incomingData = JSON.parse(message);
        console.log(incomingData);
        if(! keyValueLine.includes(incomingData.get(keyValueName))) {
            keyValueLine.push(incomingData.get(keyValueName))
        }
        var index = keyValueLine.indexOf(incomingData.get(keyValueName));
        var time  = incomingData.get(timeValueName);
        if(!outputLine[0].value){
          outputLine[0]=time;
        }
        if(outputLine[0] !=time){
          console.log(outputLine);
          outputLine=[];
        }
        else{
          outputLine[index]=incomingData.get(valueName);
        }
    });
    connection.on('close', function(reasonCode, description) {
        console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');
        console.log(outputLine);
        console.log("Flush and close the file");
    });
});

console.log("we are up");

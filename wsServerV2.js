#!/usr/bin/env node
var port = 8123;
var fileStorageAddress= "./data/" // besure to end with a '/'
var fileStorageBackupAddress="./BackUp/" //We want to point this at the NAS
var keyValueName = 'k';
var timeValueName ='t';
var valueName = 'v';
///Code///
var WebSocketServer = require('websocket').server;
var http = require('http');
const uuidv4 = require('uuid/v4')
var fs = require('fs');
var mkdirp = require('mkdirp');


var messageCount=0;
var clientCount=0;

var server = http.createServer(function(request, response) {
  console.log((new Date()) + ' Received request for ' + request.url);
  response.writeHead(404);
  response.end();
});
server.listen(8123, function() {
  console.log((new Date()) + ' Server is listening on port:\t'+ port);
  mkdirp(fileStorageAddress, function(err) {//From: https://stackoverflow.com/questions/13696148/node-js-create-folder-or-use-existing
    if(!err){console.log('Create main data folder');}
    console.log(err);
  });
  mkdirp(fileStorageBackupAddress, function(err) {
    if(!err){console.log('Create Backup Folder');}
    console.log(err);
  });
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
  let id = uuidv4();
  let unidetifiedValues=" \n";
  let fileAdd = fileStorageAddress+id.toString()+'.csv';
  while(fs.existsSync(fileAdd)){
    id = uuidv4();
    fileAdd = fileStorageAddress+id.toString()+'.csv';// befor we continue we will con tinue to create UUIDs until we get one that does nt esist befor
  } //from: https://stackoverflow.com/questions/4482686/check-synchronously-if-file-directory-exists-in-node-js
  let keyValueLine = [];
  let outputLine=[];
  let outputQueue = [];
  keyValueLine.push(timeValueName);// the first value should always be time

  let writeStream = fs.createWriteStream(fileAdd, {flags: 'w'});
  clientCount++;
  let connection = client.accept(null, client.origin);
  console.log((new Date()) + ' Connection accepted.');
  connection.on('message', function(message){
    if (message.type !== 'utf8') {
      return;
    }
    message = message.utf8Data;
    try{
      let incomingData = JSON.parse(message);

      if(! keyValueLine.includes(incomingData[keyValueName])) {
        keyValueLine.push(incomingData[keyValueName])
      }
      let index = keyValueLine.indexOf(incomingData[keyValueName]);
      let time  = incomingData[timeValueName];
      if(outputLine[0]==null){
        outputLine[0]=time;
      }
      if(outputLine[0]<time){
        let written = writeStream.write(outputLine.toString()+'\n');
        messageCount++;
        if(!written){
          throw "Message could not be written";
        }
        outputLine=[];
      }else if(outputLine[0]>time){
        throw "NewMessage was older";
      }
      outputLine[index]=incomingData[valueName];
    }catch(error){
      unidetifiedValues+=message+'\t'+error.toString()+',';
    }

  });
  connection.on('close', function(reasonCode, description) {
    clientCount--;
    console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');
    writeStream.write(outputLine.toString());// the last data line needs to be added
    writeStream.write(unidetifiedValues);
    writeStream.close(function(){
      let writeStream2 = fs.createWriteStream(fileStorageAddress+id.toString()+'_final.csv', {flags: 'w'});
      writeStream2.write(keyValueLine.toString()+'\n ');
      let readStreamer = fs.createReadStream(fileAdd);
      readStreamer.pipe(writeStream2);
      readStreamer.on('end',()=>{
        writeStream2.close(function(){console.log('Finishied adding the header file');})
      });
    });
    console.log("Flushed the file, added the header and closed the file");
  });
});

setInterval(function(){console.log('Our current message count is:\t'+messageCount+' and:\t'+clientCount+' clients.');}, 1000);

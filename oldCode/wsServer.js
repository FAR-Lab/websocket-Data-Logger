var port = 8123;
var fileStorageAddress= "./"
var fileStorageBackupAddress="./BackUp/" //We want to point this at the NAS
var keyValueName = 'k';
var timeValueName ='t';
var valueName = 'v';
///Code///
var io = require('socket.io')();

io.on('connect', function(client){
 console.log('New Client');
 var keyValueLine = [];
 keyValueLine.push(timeValueName);// the first value should always be time
 var outputLine=[];

    client.on('data', function(data){
        var incomingData = JSON.parse(data);
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
    client.on('disconnect',function(client){
      console.log(outputLine);
      console.log("Flush and close the file");

    });
});
io.listen(port);
console.log("we are up");

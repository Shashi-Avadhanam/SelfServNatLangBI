var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var async = require("async");
var pg = require('pg');

exports.myHandler = function (event, context, callback) {
console.log(event);

//get the slots data from lex event and construct jsonobj
//jsonobj is taken from s3 put event code and so needs to be renamed later
lexdata=event.currentIntent.slots;
console.log(lexdata)
jsonobj= {
SourceTables:[lexdata.sourcetablea,lexdata.sourcetableb],
TargetTable:lexdata.targettable,
ReconSourceTable:lexdata.reconsourcetable,
ReconCriteria:lexdata.reconCriteria
}
console.log(jsonobj);


//Initialize variables to hold the results
    conString = "postgres://awsuser:xxx@myfirstdb.cui9nrt0hd0c.us-west-2.rds.amazonaws.com:5432/postgres";
    srcTableCnts=[]
    targetTableCnt={}
    reconStatus=''


            processdata(jsonobj)
callback({
dialogAction: {
    type: 'Close',
    fulfillmentState: 'Fulfilled',
    message: {
      contentType: 'PlainText',
      content: 'Results are in datarecon table.Please check'
    }
  }
 } )

}

//process the details taken from lex event
processdata= function (jsonobj){
    async.parallel([getSourceCnt.bind(null,jsonobj.SourceTables),getTargetCnt.bind(null,jsonobj.TargetTable)], function (err, results) {
        if (err) { console.log(err.message) }
        console.log("Retrieved Source and Target table counts:")
        console.log(srcTableCnts)
        console.log(targetTableCnt)
        reconStatus=getReconStatus(srcTableCnts,targetTableCnt,jsonobj.ReconSourceTable,jsonobj.ReconCriteria)
        console.log("Retrieved reconstatus " + reconStatus + " and Inserting into datarecon table")
        putReconResults(srcTableCnts,targetTableCnt,reconStatus,function (err, results) {
            if (err) { console.log(err.message)}
        })
    });
}

//get the counts for all the source tables
getSourceCnt = function(SrcTables,cb2) {
    async.each(SrcTables, function (SrcTable, cb) {
        getCount(SrcTable,function(err,count){
            srcTableCnts.push({SrcTable,count});
            cb(null,count);
        });

    }, function (err, results) {
        if (err) { cb2(err.message) }
        cb2(null,results)
    });
}

//get the count for target table
getTargetCnt = function(TargetTable,cb) {
    getCount(TargetTable,function(err,count){
        targetTableCnt.TargetTable=TargetTable;
        targetTableCnt.count=count;
        cb(null,count);
    });
}

//get count from the database
getCount=function(table,cb){
    
    const results = [];
    var client = new pg.Client(conString);
    var count=0
    client.connect();
    var query= 'select count(*) from ' + table
    qry=client.query(query);
    qry.on('row', (row) => {
        cb(null,row.count);
});

    qry.on('end', () => {
        client.end();

});

}

//get the recon status from collected counts
getReconStatus= function(srcTableCnts1,targetTableCnt1,reconSourceTable,reconCriteria) {
    reconTable=srcTableCnts1.filter(function(table){ return table.SrcTable==reconSourceTable})
    if (reconCriteria == 'EQUAL') {
        if (reconTable[0].count == targetTableCnt1.count) { return 'SUCCESS' } else { return 'FAIL'}
    }
}

//put the counts and recon status for all the tables
putReconResults = function(srcTableCnts,targetTableCnt,reconStatus,cb2) {
    async.each(srcTableCnts, function (SrcTablecnt, cb) {

        insertRecon(SrcTablecnt,targetTableCnt,reconStatus,function(err,count){

            cb(null,count);
        });

    }, function (err, results) {
        if (err) { cb2(err.message) }
        cb2(null,results)
    });
}

//insert record into database
insertRecon=function(SrcTablecnt,targetTableCnt,reconStatus,cb){
    var client = new pg.Client(conString);
    client.connect();
    var query= 'insert into datarecon values(\'' + SrcTablecnt.SrcTable +'\','+ SrcTablecnt.count+',\''+targetTableCnt.TargetTable+'\','+targetTableCnt.count+',\''+reconStatus+'\');'
    qry=client.query(query);
  
    qry.on('end', () => {
        client.end();

});
}


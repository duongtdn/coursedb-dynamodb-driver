"use strict"

const AWS = require('aws-sdk')

let dynamodb = null

const table = 'COURSES';

const Courses = {
  TableName : "COURSES",
  KeySchema: [       
    { AttributeName: "courseId", KeyType: "HASH" }
  ],
  AttributeDefinitions: [       
    { AttributeName: "courseId", AttributeType: "S" }
  ],
  ProvisionedThroughput: {       
      ReadCapacityUnits: 1, 
      WriteCapacityUnits: 1
  }
}

const db = {
  _ready: false,

  createTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }

    dynamodb.createTable(Courses, function(err, data) {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });

    return this;
  },

  dropTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }
    dynamodb.deleteTable({ TableName: table }, done)
  },

  getCourse({ courseId }, done) {

    if (!courseId) {
      done && done({error: 'must specify courseId'}, null)
      return
    }
    
    const params = { 
      TableName: table, 
      Key: {
        "courseId": courseId
      }
    }
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.get(params, function(err, data) {
      if (err) {
        done && done({ error:`Unable to read item: ${JSON.stringify(err, null, 2)}`}, null);
      } else {
        if (data && data.Item) {
          done && done(null, data.Item);
        } else {
          done && done(null, null);
        }
      }
    });

  },

  batchGetCourses(courseIds, done) {
    const param = { RequestItems: {} };
    
    param.RequestItems[table] = { Keys: [] };
    courseIds.forEach( id => {
      param.RequestItems[table].Keys.push({ 'courseId' :id })
    })
    param.RequestItems[table].AttributesToGet = ['courseId', 'title', 'level', 'snippet']; // option (attributes to retrieve from this table)
    param.RequestItems[table].ConsistentRead = false; // optional (true | false)

    param.ReturnConsumedCapacity = 'NONE'; // optional (NONE | TOTAL | INDEXES)

    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.batchGet(param, (err, data) => {
      if (err) {
        done && done(err, null)
      } else {
        done && done(null, data.Responses[table])
      }
    })
  },

  createCourses( {uid, course}, done) {

    if (!uid) {
      done && done({error: 'require user id'}, null)
    }

    if (!course) {
      done && done({error: 'empty data'}, null)
    }

    if (!course.courseId) {
      done && done({error: 'missing courseId'}, null)
    }

    if (!course.detail) {
      course.detail = {
        createdBy: uid
      };
    }

    const now = new Date();
    course.detail.createdAt = now.getTime();

    const params = {
      TableName: table,
      Item: course
    };
    
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, (err, data) => {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });
  },

  removeCourses({courseId}, done) {

  },

}

function DynamoDB({ region = 'us-west-2', endpoint = 'http://localhost:8000' }, onReady) {
 
  AWS.config.update({ region, endpoint });
 
  dynamodb = new AWS.DynamoDB();

  if (onReady) {
    dynamodb.listTables(function (err, data) {
      if (err) {
        console.log("Error when checking DynamoDB status")
        db._ready = false;
        onReady(err, null);
      } else {
        db._ready = true;
        onReady(null, data);
      }
    });
  } else {
    db._ready = true;
  }

  return db;

}

module.exports = DynamoDB;


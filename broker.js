//Broker
const aedes = require ('aedes') ()
const server = require('net').createServer(aedes.handle)
const port = 1883

//Express Server and MySQL
const express = require('express');
const mysql = require('mysql');
const dotenv = require('dotenv');
const bodyParser = require('body-parser');
const { log } = require('console');

// App set-up
const app = express();
app.use(bodyParser.urlencoded({ extended: false }));
//To do: use MVC architecture
var hostname = "localhost";
var port_app = 8080;

// Create connection https://pimylifeup.com/raspberry-pi-mysql/
const db = mysql.createConnection({
  host : 'localhost',
  user: 'blast',
  password: 'shift12345',
  database: 'shiftdb'
});

//Connect 
db.connect(function(err){
  if(err){
    displayError("An error has occured when trying to connect to db.");
    throw err;
  }
  console.log("MySQL Connected...");
});

server.listen(port, function(){
  console.log('server started and listening on port ', port)
})

/** This function generates the error time in datetime format */  
function getErrorTime() {
  var currentdate = new Date();
  return currentdate;
}

function displayError(errorMsg) {
  let errorDateTime = getErrorTime();
  console.log("======================");
  console.log(errorMsg);
  console.log("@ " + errorDateTime);
  console.log("======================");
}

/** This function inserts data to a table, if not it creates a new table */
function insertData(data, db_name, location, error) {
    /** If not error, proceed with processing if table exists */
    if(!error) {
        let sql = `SELECT count(*) FROM information_schema.tables
                WHERE table_schema = '${db_name}'
                AND table_name = '${location}';`;

        /** Check if table exists */
        db.query(sql, (err, result, fields) => {
            if(err) {
              displayError("An error has occured when checking table " + location);
              throw err; 
            }
            var rows = Object.values(JSON.parse(JSON.stringify(result)))[0];

            /** If table does not exist  */
            if(rows['count(*)'] == 0) {
              console.log(`Table ${location} does not exists, creating new table...`);
              // Always either master or edge since it was checked prior
              if(location.includes("master")) {
                //Create table then insert 
                console.log(location);
                let sql = `CREATE TABLE ${location}(
                            id INT(11) AUTO_INCREMENT, 
                            node_id TINYINT(1), 
                            datetime DATETIME,
                            temperature FLOAT,
                            pressure FLOAT,
                            humidity FLOAT, 
                            PRIMARY KEY (id))`
                db.query(sql, (err, result) => {
                  if(err) {
                    displayError("An error has occured when creating table " + location);
                    throw err; 
                  }
                  console.log("Table created, inserting data...");
                  let sql = `INSERT INTO ${location} SET ?`;
                  //Query to insert data
                  db.query(sql, data, (err, result) => {
                    if(err) {
                      displayError("An error has occured when inserting elements after creating table "  + location);
                      throw err; 
                    }
                    
                    console.log(`Data successfully inserted to ${location} table.`);
                  });
                });
              } else if (location.includes("edge")) {
                //Create table then insert
                let sql = `CREATE TABLE ${location}(
                          id INT(11) AUTO_INCREMENT, 
                          node_id  TINYINT(1), 
                          datetime DATETIME,
                          light_intensity FLOAT, 
                          PRIMARY KEY (id))`
                db.query(sql, (err, result) => {
                  if(err) {
                    displayError("An error has occured when creating table " + location);
                    throw err; 
                  }
                  
                  console.log("Table created, inserting data...");
                  let sql = `INSERT INTO ${location} SET ?`;
                  //Query to insert data
                  db.query(sql, data, (err, result) => {
                    if(err) {
                      displayError("An error has occured when inserting in the table " + location);
                      throw err; 
                    }
                    
                    console.log(`Data successfully inserted to ${location} table.`);
                  });
                });
              } 
            } else {
              //Table exists so insert
              console.log(`Table ${location} exists, inserting...`);
              let sql = `INSERT INTO ${location} SET ?`;
              db.query(sql, data, (err, result) => {
                if(err) {
                  displayError("An error has occured when inserting at table " + location );
                  throw err; 
                }
                
                console.log(`Data successfully inserted to ${location} table.`);
              });
            }
        });
    } else {
      let sql = `INSERT INTO error_msg SET ?`;
      db.query(sql, data, (err, result) => {
        if(err) {
          displayError("An error has occured when inserting at table " + location);
          throw err; 
        }
        console.log("Error recorded to the DB.");
      });
    }  
}

/** This function checks if the dev type in the topic is either master or edge */
function checkDevType(devTopic){
    if (devTopic.includes("master") || devTopic.includes("edge")) return true;
    else return false;
}

/** This function assumes that location is either master or edge */
function hasNan(location, data){
  var boolVal;
  if(location.includes("master")) {
    //If either are NaN
    if(isNaN(data.node_id) || isNaN(data.datetime) || isNaN(data.temperature) || isNaN(data.pressure) || isNaN(data.humidity)) {
      console.log("NaN Detected");
      boolVal = true;
    } else {
      console.log("None are NaN");
      boolVal = false; //none are nan
    }
  } else { //If edge
    if(isNaN(data.node_id) || isNaN(data.datetime) || isNaN(data.light_intensity)) {
      boolVal = true;
    } else {
      console.log("None are NaN");
      boolVal = false; //none are nan
    }
  }

  return boolVal;
}
/** This function parses the data based on the location to fit a certain table */
function parseData(raw_data, location) {
  if(location.includes("master")) {
    try {
      let numId = raw_data["uname"]["0"].split("0");
      console.log("Num id: " + numId);
      let tempId = numId[1].split("-");
      let node_id = parseInt(tempId[1]);
      let datetime = new Date(raw_data["datetime"]["0"]);
      let temperature = parseFloat(raw_data["temperature"]["0"]);
      let pressure = parseFloat(raw_data["pressure"]["0"]);
      let humidity = parseFloat(raw_data["humidity"]["0"]);
      let data = {node_id: node_id, datetime: datetime, temperature: temperature, pressure: pressure, humidity: humidity};
      return data;
    } catch (e) {
      console.log("Something happpened in the parsing of data. Returning Null...");
      return null;
    }
    
  } 
  else if (location.includes("edge")) {
    let numId = raw_data["uname"]["0"].split("0");
    console.log("Num id: " + numId);
    let tempId = numId[1].split("-");
    let node_id = parseInt(tempId[1]);
    let datetime = new Date(raw_data["datetime"]["0"]);
    let light_intensity = parseFloat(raw_data["lightintensity"]["0"]);
    let data = {node_id: node_id, datetime: datetime, light_intensity: light_intensity}
    return data;    
  }
}

function checkPacketTopic(topic) {
  let topicsplitted = topic.split('/');
   // If invalid format
  if(topicsplitted.length == 5) 
    return true
  else
    return false
}
// When client publishes
aedes.on('publish', async function (packet, client){
  
  validTopic = checkPacketTopic(packet.topic);
  if(client && validTopic) {
    if(!packet.topic.includes("images")) {
      console.log('Client \x1b[31m' + (client ? client.id : 'BROKER_' + aedes.id)+ '\x1b[0m has published', packet.payload.toString(), 'on', packet.topic, 'to broker', aedes.id)
      var raw_data = JSON.parse(packet.payload.toString());
      console.log("RAW DATA-------");
      console.log(raw_data);
      console.log(raw_data["uname"]["0"]);
      
      console.log(packet.payload.toString());
    } else {
      console.log("An image is being published...");
    }
      
    
    // Split topic /shift/dlsau-dft/dlsau-dft0edge-pi3/sensorvalues
    var splittedTopic = packet.topic.split('/');
    if(splittedTopic[1] === "shift") {
      let db_name = `shiftdb`;
      let validDevType = checkDevType(splittedTopic[3]);

      //Checks if either master or edge 
      if(validDevType) {
          let devType = splittedTopic[3].split('0');
          console.log("Dev type: " + devType[1]);
          devType = devType[1].split("-"); //to get edge or master string
          let temp = splittedTopic[2].replace("-", "_"); //Split dlsau-dft
          let location = temp + "_" + devType[0]; //concatenate to form table name

          // Check if sensor value or image
          console.log(splittedTopic[4]);
          if (splittedTopic[4] === "sensorvalues") {
            let parsed_data = parseData(raw_data, location);
            if(parsed_data != null) {
              console.log(parsed_data);
              if(!hasNan(location, parsed_data)) {
                console.log("Data is valid to be processed in the db.");
                insertData(parsed_data, db_name, location, false);
              } else {
                let errorDateTime = getErrorTime();
                let errorMsg = "Data does not follow correct packet format.";
                console.log(errorDateTime);
                console.log(errorMsg);
                let msg = {datetime: errorDateTime, msg: errorMsg};
                let location = "error_msg";
                insertData(msg, db_name, location, true);
              }
            } else {
              let errorDateTime = getErrorTime();
              let errorMsg = "Packet format incorrect.";
              console.log(errorDateTime);
              console.log(errorMsg);
              let msg = {datetime: errorDateTime, msg: errorMsg};
              let location = "error_msg";
              insertData(msg, db_name, location, true);
            }
          } 
          
          else if (splittedTopic[4] === "images") {
              //save locally
              //Should save based on the splittedTopc name and date
              console.log("Image has been successfully saved locally. (test)");
          } 
          
          // If neither sensor value or image
          else {
            let errorDateTime = getErrorTime();
            let errorMsg = "Invalid topic: neither sensor value or image.";
            console.log(errorDateTime);
            console.log(errorMsg);
            let msg = {datetime: errorDateTime, msg: errorMsg};
            let location = "error_msg";
            insertData(msg, db_name, location, true);
          }

      } else {
        let errorDateTime = getErrorTime();
        let errorMsg = "Invalid dev type: neither master or edge.";
        console.log(errorDateTime);
        console.log(errorMsg);
        let msg = {datetime: errorDateTime, msg: errorMsg};
        let db_name = "shiftdb";
        let location = "error_msg";
        insertData(msg, db_name, location, true);
      }
    } else {
      //If error, enter error message to database
      let errorDateTime = getErrorTime();
      let errorMsg = "Invalid topic: not shift.";
      console.log(errorDateTime);
      console.log(errorMsg);
      let msg = {datetime: errorDateTime, msg: errorMsg};
      let db_name = "shiftdb";
      let location = "error_msg";
      insertData(msg, db_name, location, true);
    } 
  } else if (client && !validTopic) {
    //console.log('Client \x1b[31m' + (client ? client.id : 'BROKER_' + aedes.id)+ '\x1b[0m has published', packet.payload.toString(), 'on', packet.topic, 'to broker', aedes.id)
    
    let errorDateTime = getErrorTime();
    let errorMsg = "Packet format incorrect. Length not at least 4.";
    console.log(errorDateTime);
    console.log(errorMsg);
    let msg = {datetime: errorDateTime, msg: errorMsg};
    let db_name = "shiftdb";
    let location = "error_msg";
    insertData(msg, db_name, location, true);
  }
});

app.listen(port_app, hostname, ()=> {
  console.log(`Server running at:`);
  console.log('http://' + hostname + ':' + port_app);
});

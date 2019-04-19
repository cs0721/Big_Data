//Creating database
use fit5148_db

//Question 1 - Import the data (Fire data-part 1 and climate data-part 1) into two different collections in MongoDB
//Import FireData-Part 1
//mongoimport --host localhost --db fit5148_db --collection fire --type csv --file ~/env3/FIT5148/FireData-Part1.csv --headerline

//Import ClimateData-Part 1
//mongoimport --host localhost --db fit5148_db --collection fire --type csv --file ~/env3/FIT5148/ClimateData-Part1.csv --headerline

//Question 2 - Find climate data on 15th December 2017
db.climate.find({"Date":"2017-12-15"})

//Question 3 - Find the latitude, longitude and confidence when the surface temperature (C) was between 65 and 100
db.fire.find({"Surface Temperature (Celcius)":{$gte:65,$lte:100}},{"Latitude":1,"Longitude":1,"Confidence":1})

//Question 4 - Find surface temperature(C) air temperature (C), relative humidity and maximum wind speed on 15th and 16th of December
db.climate.aggregate([{$unwind:"$Date"},{$match:{$or:[{Date:"2017-12-15"},{Date:"2017-12-16"}]}},{$lookup:{from:"fire",localField:"Date",foreignField:"Date",as:"fires"}},{$project:{"Date":1,"Air Temperature(Celcius)":1,"fires.Surface Temperature (Celcius)":1,"Relative Humidity":1,"Max Wind Speed":1}}]).pretty()

//Question 5 - Find Datetime, air temperature(C), surface temperature(C), and confidence when the confidence is between 80 and 100. 
db.fire.aggregate([{$match:{"Confidence":{$gte:80,$lte:100}}},{$lookup:{from:"climate",localField:"Date",foreignField:"Date",as:"climates"}},{$project:{"Datetime":1,"climates.Air Temperature(Celcius)":1,"Surface Temperature (Celcius)":1,"Confidence":1}} ]).pretty()



//Question 6 - Find top 10 records with highest surface temperature(C). 
db.fire.find().sort({"Surface Temperature (Celcius)":-1}).limit(10)

//Question 7 - Find the number of fire in each day. You are required to only displayed total number of fire and date in the output. 
db.fire.aggregate({$group:{_id:"$Date",count:{$sum:1}}})

//Question 8 - Find the average surface temperature (C) for each day. You are required to only display average surface temperature and the date in the output.
db.fire.aggregate({$group:{_id:"$Date",avgSurfaceTemp:{$avg:"$Surface Temperature (Celcius)"}}})



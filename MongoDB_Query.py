from pymongo import MongoClient
# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
result1 = client['olympichistory']['olympic'].aggregate([
    {
        '$group': {
            '_id': '$NOC'
        }
    }, {
        '$count': 'Total Countries'
    }
])
print("Unique Countries");
for item in result1:
    print(item)

result1 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 1,
            'Sport': 1,
            'Year': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Year': {
                '$lte': 1900
            }
        }
    }, {
        '$group': {
            '_id': '$Sport'
        }
    }, {
        '$count': 'TotalSportTill1900'
    }
])

print("\nTotal Sports till 1900");
for item in result1:
    print(item)

result2 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 1,
            'Sport': 1,
            'Year': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Year': {
                '$lte': 1950
            }
        }
    }, {
        '$group': {
            '_id': '$Sport'
        }
    }, {
        '$count': 'TotalSportTill1950'
    }
])

print("\nTotal Sports till 1950");
for item in result2:
    print(item)

result3 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 1,
            'Sport': 1,
            'Year': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Year': {
                '$lte': 2000
            }
        }
    }, {
        '$group': {
            '_id': '$Sport'
        }
    }, {
        '$count': 'TotalSportTill2000'
    }
])

print("\nTotal Sports till 2000");
for item in result3:
    print(item)

result3 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 1,
            'Sport': 1,
            'Year': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Year': {
                '$lte': 2016
            }
        }
    }, {
        '$group': {
            '_id': '$Sport'
        }
    }, {
        '$count': 'TotalSportTill2016'
    }
])

print("\nTotal Sports till 2016");
for item in result3:
    print(item)

client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
result4 = client['olympichistory']['olympic'].aggregate([
    {
        '$group': {
            '_id': '$NOC',
            'Count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])

print("\nNumber of times particular country participated");
for item in result4:
    print(item)

result5 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Year': 1,
            'Sex': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Sex': 'F'
        }
    }, {
        '$group': {
            '_id': '$Year',
            'TotalFemaleParticipants': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])

print("\nNumber of female participants by year");
for item in result5:
    print(item)

result6 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Year': 1,
            'Sex': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Sex': 'M'
        }
    }, {
        '$group': {
            '_id': '$Year',
            'TotalMaleParticipants': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])

print("\nNumber of male participants by year");
for item in result6:
    print(item)

result7 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Year': 1,
            'Sex': 1,
            'Medal': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Sex': 'F',
            'Medal': 'Gold'
        }
    }, {
        '$group': {
            '_id': '$Year',
            'TotalFemaleGoldWinner': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])

print("\nNumber of Female GOLD winner by year");
for item in result7:
    print(item)

result8 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Year': 1,
            'Sex': 1,
            'Medal': 1
        }
    }, {
        '$addFields': {
            'Year': {
                '$toInt': '$Year'
            }
        }
    }, {
        '$match': {
            'Sex': 'M',
            'Medal': 'Gold'
        }
    }, {
        '$group': {
            '_id': '$Year',
            'TotalMaleGoldWinner': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])

print("\nNumber of Male GOLD winner by year");
for item in result8:
    print(item)

result9 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Medal': 1,
            'Age': 1
        }
    }, {
        '$match': {
            'Medal': 'Gold',
            'Age': {
                '$ne': 'NA'
            }
        }
    }, {
        '$addFields': {
            'Age': {
                '$toInt': '$Age'
            }
        }
    }, {
        '$match': {
            'Age': {
                '$lte': 20
            }
        }
    }, {
        '$group': {
            '_id': None,
            'TotalMedal': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of GOLD winner under 21 year's of age");
for item in result9:
    print(item)

result10 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Medal': 1,
            'Age': 1
        }
    }, {
        '$match': {
            'Medal': 'Gold',
            'Age': {
                '$ne': 'NA'
            }
        }
    }, {
        '$addFields': {
            'Age': {
                '$toInt': '$Age'
            }
        }
    }, {
        '$match': {
            'Age': {
                '$gt': 20,
                '$lte': 35
            }
        }
    }, {
        '$group': {
            '_id': None,
            'TotalMedal': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of GOLD winner above 20 and less than 36 year's of age");
for item in result10:
    print(item)

result11 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Medal': 1,
            'Age': 1
        }
    }, {
        '$match': {
            'Medal': 'Gold',
            'Age': {
                '$ne': 'NA'
            }
        }
    }, {
        '$addFields': {
            'Age': {
                '$toInt': '$Age'
            }
        }
    }, {
        '$match': {
            'Age': {
                '$gt': 35,
                '$lte': 45
            }
        }
    }, {
        '$group': {
            '_id': None,
            'TotalMedal': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of GOLD winner above 35 and less than 46 year's of age");
for item in result11:
    print(item)

result12 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Medal': 1,
            'Age': 1
        }
    }, {
        '$match': {
            'Medal': 'Gold',
            'Age': {
                '$ne': 'NA'
            }
        }
    }, {
        '$addFields': {
            'Age': {
                '$toInt': '$Age'
            }
        }
    }, {
        '$match': {
            'Age': {
                '$gt': 45,
                '$lte': 55
            }
        }
    }, {
        '$group': {
            '_id': None,
            'TotalMedal45to55Age': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of GOLD winner above 45 and less than 56 year's of age");
for item in result12:
    print(item)

result13 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Medal': 1,
            'Age': 1
        }
    }, {
        '$match': {
            'Medal': 'Gold',
            'Age': {
                '$ne': 'NA'
            }
        }
    }, {
        '$addFields': {
            'Age': {
                '$toInt': '$Age'
            }
        }
    }, {
        '$match': {
            'Age': {
                '$gt': 55
            }
        }
    }, {
        '$group': {
            '_id': None,
            'TotalMedal45to55Age': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of GOLD winner above 55 year's of age");
for item in result13:
    print(item)

result14 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Sex': 1,
            'Medal': 1
        }
    }, {
        '$match': {
            'Sex': 'F',
            'Medal': {
                '$ne': 'NA'
            }
        }
    }, {
        '$group': {
            '_id': '$Sex',
            'TotalFemalWinner': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of Female winners till now");
for item in result14:
    print(item)

result15 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'Sex': 1,
            'Medal': 1
        }
    }, {
        '$match': {
            'Sex': 'M',
            'Medal': {
                '$ne': 'NA'
            }
        }
    }, {
        '$group': {
            '_id': '$Sex',
            'TotalMaleWinner': {
                '$sum': 1
            }
        }
    }
])

print("\nNumber of Male winners till now");
for item in result15:
    print(item)

result16 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            '_id': 0,
            'ID': 1,
            'Name': 1,
            'Medal': 1,
            'Team': 1
        }
    }, {
        '$match': {
            'Medal': {
                '$ne': 'NA'
            }
        }
    }, {
        '$addFields': {
            'ID': {
                '$toInt': '$ID'
            }
        }
    }, {
        '$group': {
            '_id': '$ID',
            'TotalMedal': {
                '$sum': 1
            },
            'Name': {
                '$first': '$Name'
            },
            'Team': {
                '$first': '$Team'
            }
        }
    }, {
        '$match': {
            'TotalMedal': {
                '$gte': 10
            }
        }
    }, {
        '$sort': {
            'TotalMedal': 1
        }
    }
])

print("\nPlayer details who won more than or equal to 10 Medals");
for item in result16:
    print(item)

result17 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            'Season': 1
        }
    }, {
        '$match': {
            'Season': 'Summer'
        }
    }, {
        '$count': 'Total summer olympic Participants'
    }
])

print("\nNumber of summer olympic participants");
for item in result17:
    print(item)

result18 = client['olympichistory']['olympic'].aggregate([
    {
        '$project': {
            'Season': 1
        }
    }, {
        '$match': {
            'Season': 'Winter'
        }
    }, {
        '$count': 'Total summer olympic Participants'
    }
])

print("\nNumber of WINTER olympic participants");
for item in result18:
    print(item)
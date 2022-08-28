import yfinance as yahooFinance
import mysql.connector
from numerize import numerize
from datetime import date, timedelta
import math

def getToday():
    today = str(date.today())
    return today
def getYesterday(n):
    yesterday = str(date.today() - timedelta(days=n))
    return yesterday

# split addCompany into a list of tuples | required to manipulate the data
def getCompanyData(addCompany):
    addCompany = str(addCompany)
    addCompany = addCompany.replace("(", "")
    addCompany = addCompany.replace(")", "")
    addCompany = addCompany.replace("[", "")
    addCompany = addCompany.replace("]", "")
    addCompany = addCompany.replace("'", "")
    addCompany = addCompany.replace(",", "")
    addCompany = addCompany.split()
    return addCompany
    # print(addCompany[0])

# functions that executes the sql query
def fetchNewCompanies(addCompany):
    cursor.execute("SELECT * FROM newcompany")
    addCompany = cursor.fetchall()
    return getCompanyData(addCompany)

def fetchHoldCompanies():
    cursor.execute("SELECT * FROM company")
    holdCompanies = cursor.fetchall()
    return holdCompanies

def fetchCompany(addCompany):
    cursor.execute("SELECT * FROM company")
    addCompany = cursor.fetchall()
    return addCompany

# function that deletes the nth element in the database newcompany
def deleteNewCompany(addCompany, n):
    cursor.execute("DELETE FROM newcompany WHERE Ticker = %s", (addCompany[n],))
    db.commit()

# function add addCompany to the database company if it is valid and not already in the database
def ifnTableDelete(addCompany, holdCompanies):
    for n in range(len(addCompany)):
        addCompany = fetchNewCompanies(addCompany)
        holdCompanies = fetchHoldCompanies()
        print("\n1 =", n)
        data = yahooFinance.Ticker(addCompany[n])
        print("\n", data.info['shortName'])
        print("\n", data.info['marketCap'])
        cap = None
        if data.info['marketCap'] != None:
            cap = data.info['marketCap']
            cap = numerize.numerize(cap)
        print(cap) 
        if data.info['longName'] == None:
            print(addCompany[n] + " is not a valid ticker")
        else:
            print(addCompany[n] + " is a valid ticker")
            insideDB = False
            if len(holdCompanies) > 0:
                for i in range(len(holdCompanies)):
                    if addCompany[n] == holdCompanies[i][2]:
                        print(addCompany[n] + " Already in table")
                        insideDB = True
                        break
            if insideDB == False:
                print(addCompany[n] + " added to table")
                cursor.execute("INSERT INTO company (name, ticker, marketCap, flag) VALUES (%s, %s, %s, %s)", (data.info['longName'], data.info['symbol'], cap, 0))
                db.commit()
    #delete all items in newcompany table
    cursor.execute("DELETE FROM newcompany")
    db.commit()
    return fetchNewCompanies(addCompany)    



# Connect to the database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="5662",
    database="swingtraderdb"
)
cursor = db.cursor()
cursor.execute("ALTER TABLE company AUTO_INCREMENT = 1")
cursor.execute("SELECT * FROM newcompany")
addCompany = cursor.fetchall()
cursor.execute("SELECT * FROM company")
holdCompanies = cursor.fetchall()

addCompany = fetchNewCompanies(addCompany)
print(addCompany)

# if addCompany[n] is in the 3rd element (ticker) of holdCompanies, delete it from addCompany
addCompany = ifnTableDelete(addCompany, holdCompanies)
addCompany = fetchNewCompanies(addCompany)



holdCompanies = fetchCompany(holdCompanies)
print(holdCompanies)

# cursor.execute("CREATE TABLE stockdata (id INT AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(10) NOT NULL, date DATETIME NOT NULL, open FLOAT NOT NULL, close FLOAT NOT NULL, high FLOAT NOT NULL, low FLOAT NOT NULL, volume INT NOT NULL, dividends FLOAT(4,3) NOT NULL, stocksplit INT(3) NOT NULL)")
# db.commit()

#delete all itemss from the stockdata table
# cursor.execute("DELETE FROM stockdata")
# db.commit()
# print("stockdata table cleared")
cursor.execute("ALTER TABLE stockdata AUTO_INCREMENT = 1")

today = getToday()
yesterday = getYesterday(90)

#insert data into stockdata table if data is not in the table check if data['Open'] is not nan and if it is not nan, insert data into the table
for n in range(len(holdCompanies)):
    ticker = yahooFinance.Ticker(holdCompanies[n][2])
    data = ticker.history(start=yesterday, end=today)
    for i in range(len(data)):
        cursor.execute("SELECT * FROM stockdata WHERE symbol = %s AND date = %s", (holdCompanies[n][2], data.index[i]))
        check = cursor.fetchall()
        validData = math.isnan(data['Open'][i])
        if len(check) == 0 and validData == False:
            # print(holdCompanies[n][2] + " " + str(data.index[i]) + str(data['Open'][i]) + " " + str(data['Close'][i]) + " " + str(data['High'][i]) + " " + str(data['Low'][i]) + " " + str(data['Volume'][i]) + " " + str(data['Dividends'][i]) + " " + str(data['Stock Splits'][i]))
            cursor.execute("INSERT INTO stockdata (symbol, date, open, close, high, low, volume, dividends, stocksplit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (holdCompanies[n][2], data.index[i], data['Open'][i], data['Close'][i], data['High'][i], data['Low'][i], int(data['Volume'][i]), float(data['Dividends'][i]), int(data['Stock Splits'][i])))
            db.commit()
            print(holdCompanies[n][2] + " " + str(data.index[i].strftime('%Y-%m-%d')) + " record inserted.")
        else:
            if validData == True:
                print("data is " + str(not validData))
            else:
                print(holdCompanies[n][2] + " " + str(data.index[i].strftime('%Y-%m-%d')) + " already in table")


#close the connection
cursor.close()
db.close()












#db stuct

# CREATE TABLE `company` (
#   `ID` int unsigned NOT NULL AUTO_INCREMENT,
#   `Name` varchar(100) NOT NULL,
#   `Ticker` varchar(50) NOT NULL,
#   `MarketCap` varchar(25) DEFAULT NULL,
#   `Flag` tinyint unsigned NOT NULL,
#   UNIQUE KEY `Name` (`Name`),
#   UNIQUE KEY `Ticker` (`Ticker`),
#   UNIQUE KEY `ID` (`ID`)
# ) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


# CREATE TABLE `newcompany` (
#   `Ticker` varchar(50) NOT NULL,
#   UNIQUE KEY `Ticker` (`Ticker`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


# CREATE TABLE `stockdata` (
#   `id` int NOT NULL AUTO_INCREMENT,
#   `symbol` varchar(10) NOT NULL,
#   `date` datetime NOT NULL,
#   `open` float NOT NULL,
#   `close` float NOT NULL,
#   `high` float NOT NULL,
#   `low` float NOT NULL,
#   `volume` int NOT NULL,
#   `dividends` float(4,3) NOT NULL,
#   `stocksplit` int NOT NULL,
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB AUTO_INCREMENT=550 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

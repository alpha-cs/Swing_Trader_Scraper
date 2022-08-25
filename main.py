import yfinance as yahooFinance
import mysql.connector
from numerize import numerize

#split addCompany into a list of tuples
#required to manipulate the data
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

#make a functions that executes the sql query
def fetchNewCompanies(addCompany):
    cursor.execute("SELECT * FROM newcompany")
    addCompany = cursor.fetchall()
    return getCompanyData(addCompany)

def fetchCompany(addCompany):
    cursor.execute("SELECT * FROM company")
    addCompany = cursor.fetchall()
    return addCompany

#make a function that deletes the nth element in the database newcompany
def deleteNewCompany(addCompany, n):
    cursor.execute("DELETE FROM newcompany WHERE Ticker = %s", (addCompany[n],))
    db.commit()

# function if addCompany[n] is in the 3rd element of holdCompanies, delete it from addCompany
def ifnTableDelete(addCompany, holdCompanies):
    for n in range(len(addCompany)):
        for i in range(len(holdCompanies)):
            if addCompany[n] == holdCompanies[i][2]:
                print(addCompany[n] + " Already in table")
                deleteNewCompany(addCompany, n)
    addCompany = fetchNewCompanies(addCompany)
    for n in range(len(addCompany)):
        data = yahooFinance.Ticker(addCompany[n])
        if data.info['longName'] == None:
            print(addCompany[n] + " is not a valid ticker")
            deleteNewCompany(addCompany, n)
    return addCompany    


# Connect to the database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="5662",
    database="swingtraderdb"
)
cursor = db.cursor()
#alter table company auto_increment = 1;
cursor.execute("ALTER TABLE company AUTO_INCREMENT = 1")
#get data from newcompany table
cursor.execute("SELECT * FROM newcompany")
addCompany = cursor.fetchall()

#get data from company table
cursor.execute("SELECT * FROM company")
holdCompanies = cursor.fetchall()

#convert addCompany to a list of tuples
addCompany = fetchNewCompanies(addCompany)
print(addCompany)

#if addCompany[n] is in the 3rd element of holdCompanies, delete it from addCompany
addCompany = ifnTableDelete(addCompany, holdCompanies)
addCompany = fetchNewCompanies(addCompany)

#check if addCompany is empty or not and if it is not empty, add it to the company table and delete it from the newcompany table
if len(addCompany) != 0:
    for n in range(len(addCompany)):
        data = yahooFinance.Ticker(addCompany[n])
        print(data.info['longName'])
        cap = numerize.numerize(data.info['marketCap'])
        print(cap)
        cursor.execute("INSERT INTO company (name, ticker, marketCap, flag) VALUES (%s, %s, %s, %s)", (data.info['longName'], data.info['symbol'], cap, 0))
        db.commit()
        deleteNewCompany(addCompany, n)
        print(data.info['longName'], "record inserted.")

addCompany = fetchNewCompanies(addCompany)
print(addCompany)

holdCompanies = fetchCompany(holdCompanies)


# cursor.execute("CREATE TABLE stockdata (id INT AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(10) NOT NULL, date DATETIME NOT NULL, open FLOAT NOT NULL, close FLOAT NOT NULL, high FLOAT NOT NULL, low FLOAT NOT NULL, volume INT NOT NULL, dividends FLOAT(4,3) NOT NULL, stocksplit INT(3) NOT NULL)")
# db.commit()


#insert data into stockdata table
# for n in range(len(holdCompanies)):
#     ticker = yahooFinance.Ticker(holdCompanies[n][2])
#     data = ticker.history(start="2021-12-15", end="2021-12-25")
#     for i in range(len(data)):
#         cursor.execute("INSERT INTO stockdata (symbol, date, open, close, high, low, volume, dividends, stocksplit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (holdCompanies[n][2], data.index[i], data['Open'][i], data['Close'][i], data['High'][i], data['Low'][i], int(data['Volume'][i]), float(data['Dividends'][i]), int(data['Stock Splits'][i])))
#         db.commit()
#         print(data.index[1], "record inserted.")




#close the connection
cursor.close()
db.close()
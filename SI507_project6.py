# Import statements
import psycopg2
from Secret import * 
import csv
import psycopg2.extras

# Write code / functions to set up database connection and cursor here.
try:
    conn = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password)) # No password on the databases yet -- wouldn't want to save that in plain text, anyway
    print("Success connecting to database")
except:
    print("Unable to connect to the database. Check server and credentials.")
    sys.exit(1) # Stop running program if there's no db connection.
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Write code / functions to create tables with the columns you want and all database setup here.
cur.execute("""DROP TABLE IF EXISTS "Sites" """)
cur.execute("""DROP TABLE IF EXISTS "States" """)


cur.execute("""CREATE TABLE IF NOT EXISTS "States"(
	"ID" SERIAL PRIMARY KEY,
	"Name" VARCHAR(40) NOT NULL UNIQUE)""")

cur.execute("""CREATE TABLE IF NOT EXISTS "Sites"(
	"ID" SERIAL PRIMARY KEY,
	"Name" VARCHAR(128) NOT NULL UNIQUE,
	"Type" VARCHAR(128),
	"State_ID" INTEGER REFERENCES "States"("ID"),
	"Location" VARCHAR(255),
	"Description" TEXT)""")



# Write code / functions to deal with CSV files and insert data into the database here.

# Make sure to commit your database changes with .commit() on the database connection.

# Write code to be invoked here (e.g. invoking any functions you wrote above)


def insert_States(states_name, conn, cur):
    sql = """INSERT INTO "States"("Name") VALUES(%s) RETURNING  "ID" """
    cur.execute(sql,(states_name,)) # Must be tuple or list here, "," makes () be a tuple rather than list
    # print("Artist name", artist_name)
    conn.commit()
    rec = cur.fetchone()
    #  print (rec)
    return rec['ID']


def insert_Sites(sites_name, sites_type, sites_state_id, sites_location, sites_description, states_name, conn, cur):
    """Inserts an artist and returns name, None if unsuccessful"""
    sql = """INSERT INTO "Sites"("Name", "Type", "State_ID", "Location", "Description") VALUES(%s, %s, %s, %s, %s)"""
    cur.execute(sql,(sites_name, sites_type, sites_state_id, sites_location, sites_description))
    # print("Artist name", artist_name)
    conn.commit()
    return True

states_Name_list = ["arkansas","california","michigan"]

for n in range (len(states_Name_list)):
	with open(states_Name_list[n]+ '.csv', 'r', encoding = "utf-8") as f:
		reader = csv.reader(f)
		next(reader)
		stateID = insert_States(states_Name_list[n], conn, cur)
		for row in reader:
			# print (row[0], row[2], stateID, row[1], row[4])
			insert_Sites(row[0], row[2], stateID, row[1], row[4], states_Name_list[n], conn, cur)

# Write code to make queries and save data in variables here.

def execute_return(query):
	cur.execute(query)
	rec = cur.fetchall()
	return rec

all_locations = execute_return("""SELECT "Location" FROM "Sites" """)
# print (all_locations)
beautiful_sites = execute_return("""SELECT "Name" FROM "Sites" WHERE "Description" ILIKE '%beautiful%' """)
# print (beautiful_sites)
natl_lakeshores = execute_return("""SELECT COUNT(*) FROM "Sites" WHERE "Type"='National Lakeshore' """)
# print (natl_lakeshores)
michigan_names = execute_return("""SELECT "Sites"."Name" FROM "Sites" INNER JOIN "States" ON "Sites"."State_ID" = "States"."ID" WHERE "States"."Name" = 'michigan'  """)
# print (michigan_names)
total_number_arkansas = execute_return("""SELECT COUNT(*) FROM "Sites" INNER JOIN "States" ON "Sites"."State_ID" = "States"."ID" WHERE "States"."Name" = 'arkansas' """)
# print (total_number_arkansas)
# We have not provided any tests, but you could write your own in this file or another file, if you want.

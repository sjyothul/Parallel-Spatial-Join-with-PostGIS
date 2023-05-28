#
# Tester
#

from subprocess import call
import os
import shutil
import psycopg2
import sys
import Assignment2_Interface as Assignment2

DATABASE_NAME = 'dds_assignment2'

def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

def loadPoints(pointstablename, pointsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS " + pointstablename)

    cur.execute("CREATE TABLE " + pointstablename+" (longitude REAL,  latitude REAL, geom geometry)")

    f = open(pointsfilepath,'r')
    cur.copy_from(f, pointstablename, sep = ',', columns=('longitude','latitude'))
    cur.execute("UPDATE " + pointstablename + " SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")

    cur.close()
    openconnection.commit()

def loadRectangles(rectstablename, rectsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS " + rectstablename)

    cur.execute("CREATE TABLE " + rectstablename+" (longitude1 REAL,  latitude1 REAL, longitude2 REAL,  latitude2 REAL, geom geometry)")

    f = open(rectsfilepath,'r')
    cur.copy_from(f, rectstablename, sep = ',', columns=('longitude1', 'latitude1', 'longitude2', 'latitude2'))
    cur.execute("UPDATE " + rectstablename + " SET geom = ST_MakeEnvelope(longitude1, latitude1, longitude2, latitude2, 4326);")

    cur.close()
    openconnection.commit()

def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print("Creating Database named as dds_assignment2")
        createDB(DATABASE_NAME);

        # Getting connection to the database
        print("Getting connection from the dds_assignment2 database")
        con = getOpenConnection(dbname = DATABASE_NAME)
        
        try:
            cur = con.cursor()
            cur.execute("CREATE EXTENSION postgis;") # Add PostGIS extension
        except:
            pass
        finally:
            cur.close()
            con.commit()

        #Loading two tables points and rectangles
        loadPoints('points', 'points.csv', con)
        loadRectangles('rectangles', 'rectangles.csv', con)
        print("Points and rectangles data loaded successfully")

        # Calling ParallelJoin
        print("Performing Parallel Join")
        Assignment2.parallelJoin('points', 'rectangles', 'parallelJoinOutputTable', 'output_part_a.txt', con);

        if con:
            con.close()

    except Exception as detail:
        print("Something bad has happened!!! This is the error ==> ", detail)

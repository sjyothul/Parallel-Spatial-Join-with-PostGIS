#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading
# Do not close the connection inside this file i.e. do not perform openConnection.close()

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.
    with openConnection.cursor() as cur:
        cur.execute("drop table if exists point1")
        cur.execute("create table point1(longitude real,latitude real,geom geometry)")
        cur.execute("insert into point1 select * from "+ pointsTable +" where latitude <= 40.560000 AND longitude <= -75.0000")

        cur.execute("drop table if exists point2")
        cur.execute("create table point2(longitude real,latitude real,geom geometry)")
        cur.execute("insert into point2 select * from "+ pointsTable +" where latitude > 40.560000 AND longitude > -75.0000")

        cur.execute("drop table if exists point3")
        cur.execute("create table point3(longitude real,latitude real,geom geometry)")
        cur.execute("insert into point3 select * from "+ pointsTable +" where latitude <= 40.560000 AND longitude > -75.0000")

        cur.execute("drop table if exists point4")
        cur.execute("create table point4(longitude real,latitude real,geom geometry)")
        cur.execute("insert into point4 select * from "+ pointsTable +" where latitude > 40.560000 AND longitude <= -75.0000")

        cur.execute("drop table if exists rect1")
        cur.execute("create table rect1(longitude1 real,latitude1 real,longitude2 real,latitude2 real,geom geometry)")
        cur.execute("insert into rect1 select * from "+ rectsTable +" where latitude1 <= 40.560000 AND longitude1 <= -75.0000")

        cur.execute("drop table if exists rect2")
        cur.execute("create table rect2(longitude1 real,latitude1 real,longitude2 real,latitude2 real,geom geometry)")
        cur.execute("insert into rect2 select * from "+ rectsTable +" where latitude1 > 40.560000 AND longitude1 > -75.0000")
        
        cur.execute("drop table if exists rect3")
        cur.execute("create table rect3(longitude1 real,latitude1 real,longitude2 real,latitude2 real,geom geometry)")
        cur.execute("insert into rect3 select * from "+ rectsTable +" where latitude1 <= 40.560000 AND longitude1 > -75.0000")
        
        cur.execute("drop table if exists rect4")
        cur.execute("create table rect4(longitude1 real,latitude1 real,longitude2 real,latitude2 real,geom geometry)")
        cur.execute("insert into rect4 select * from "+ rectsTable +" where latitude1 > 40.560000 AND longitude1 <= -75.00000")

        cur.close()

    def thread1(rect1, point1):
        cur = openConnection.cursor()
        cur.execute("drop table if exists fragjoin1")
        cur.execute("select "+ rect1 +".geom as geom,count(*) as points_count into fragjoin1 from "+ rect1 +" join "+ point1 +
                   " on ST_Contains( " + rect1 + " .geom, " + point1 + " .geom) group by "+ rect1 +".geom order by count(*)")
        cur.close()

    def thread2(rect2, point2):
        cur = openConnection.cursor()
        cur.execute("drop table if exists fragjoin2")
        cur.execute("select "+ rect2 +".geom as geom,count(*) as points_count into fragjoin2 from "+ rect2 +" join "+ point2 +
                   " on ST_Contains( " + rect2 + " .geom, " + point2 + " .geom) group by "+ rect2 +".geom order by count(*)")
        cur.close()

    def thread3(rect3, point3):
        cur = openConnection.cursor()
        cur.execute("drop table if exists fragjoin3")
        cur.execute("select "+ rect3 +".geom as geom,count(*) as points_count into fragjoin3 from "+ rect3 +" join "+ point3 +
                   " on ST_Contains( " + rect3 + " .geom, " + point3 + " .geom) group by "+ rect3 +".geom order by count(*)")
        cur.close()

    def thread4(rect4, point4):
        cur = openConnection.cursor()
        cur.execute("drop table if exists fragjoin4")
        cur.execute("select "+ rect4 +".geom as geom,count(*) as points_count into fragjoin4 from "+ rect4 +" join "+ point4 +
                   " on ST_Contains( " + rect4 + " .geom, " + point4 + " .geom) group by "+ rect4 +".geom order by count(*)")
        cur.close()

    thread1 = threading.Thread(target=thread1, args=('rect1','point1'))
    thread1.start()
    thread2 = threading.Thread(target=thread2, args=('rect2','point2'))
    thread2.start()
    thread3 = threading.Thread(target=thread3, args=('rect3','point3'))
    thread3.start()
    thread4 = threading.Thread(target=thread4, args=('rect4','point4'))
    thread4.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    
    with openConnection.cursor() as cur:
        cur.execute("Drop table if exists " + outputTable + "")
        cur.execute("select geom, SUM(points_count) as points_count into " + outputTable +
         " from (select * from fragjoin1 union all select * from fragjoin2 union all select * from fragjoin3 union all select * from fragjoin4) as res group by geom order by SUM(points_count)")        
        cur.execute("select * from " + outputTable + "")

        sys.stdout = open(outputPath, 'wt')
        for output in cur.fetchall():
            print(output[1])
        
        cur.close()
    openConnection.commit()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
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
        cur.execute('CREATE DATABASE %s' % (dbname,))  # create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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
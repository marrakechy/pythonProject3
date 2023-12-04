import csv
from mysql.connector import connect, Error

def createScheduleTable(mydb):
    mycursor = mydb.cursor()
    query = """
    DROP TABLE IF EXISTS SCHEDULE;
    CREATE TABLE SCHEDULE (
        Department VARCHAR(255),
        CourseCode VARCHAR(255),
        Section VARCHAR(10),
        CourseTitle VARCHAR(255),
        Instructor VARCHAR(255),
        Days VARCHAR(10),
        BeginTime VARCHAR(25),
        EndTime VARCHAR(25),
        BuildingRoom VARCHAR(255),
        Credits DECIMAL(3, 2),
        Year INT,
        Term VARCHAR(50)
    );
    """
    try:
        mycursor.execute(query, multi=True)
        print("Table SCHEDULE created successfully.")
    except Error as e:
        print(e)

def insertScheduleRecord(mydb, department, course_code, course_title, instructor, days, begin_time, end_time, building_room, credits, year, term):
    # function implementation

    mycursor = mydb.cursor()
    query = """INSERT INTO SCHEDULE 
    (Department, CourseCode, CourseTitle, Instructor, Days, BeginTime, EndTime, BuildingRoom, Credits, Year, Term)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    values = (department, course_code, course_title, instructor, days, begin_time, end_time, building_room, credits, year, term)
    try:
        mycursor.execute(query, values)
        mydb.commit()
        #print(query)
    except Error as e:
        print(e)


def selectSchedule(mydb):
    mycursor = mydb.cursor()
    query = "SELECT * FROM SCHEDULE"
    try:
        mycursor.execute(query)
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def processCsvFile(mydb, filepath):
    department = None  # Keep track of the current department

    with open(filepath, newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        print(filepath)
        for row in csvreader:
            #print(len(row))
            # Check if the row is a department heading
            if len(row) == 1 or not any(row[1:]):
                department = row[0]
                continue

            if len(row) < 7:
                continue

            course_code_title = row[0].split(maxsplit=1)
            if len(course_code_title) < 2:
                continue  #skip if course code or title is missing

            course_code = course_code_title[0]
            course_title = course_code_title[1]
            print(course_title)
            insertScheduleRecord(mydb, department, course_code, course_title, row[1], row[2], row[3], row[4], row[5], row[6], 2021, 'Fall')



def main():
    try:
        mydb = connect(
            host="localhost",
            user="root",
            password="Moompopo00Z=",  # Replace with your password
            database="DS230"  # Replace with your database
        )

        #createScheduleTable(mydb)
        filepath = 'C:/Users/test1/PycharmProjects/9/5/pythonProject3/Course Schedule.csv'
        processCsvFile(mydb, filepath)
        selectSchedule(mydb)

    except Error as e:
        print(e)

main()

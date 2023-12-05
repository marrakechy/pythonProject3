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
        #print("Table SCHEDULE created successfully.")
    except Error as e:
        print(e)

def createStudentTable(mydb):
    mycursor = mydb.cursor()
    query = """
    DROP TABLE IF EXISTS STUDENT;
    CREATE TABLE STUDENT (
        StudentID INT AUTO_INCREMENT PRIMARY KEY,
        StudentNumber VARCHAR(255),
        Fname VARCHAR(255),
        Lname VARCHAR(255),
        ClassYear INT,
        Major1 VARCHAR(255),
        Major2 VARCHAR(255),
        Minor1 VARCHAR(255),
        Advisor VARCHAR(255)
    );
    """
    try:
        mycursor.execute(query, multi=True)
        print("Table STUDENT created successfully.")
    except Error as e:
        print(e)

def createEnrollmentTable(mydb):
    mycursor = mydb.cursor()
    query = """
    DROP TABLE IF EXISTS ENROLLMENT;
    CREATE TABLE ENROLLMENT (
        StudentID INT,
        CourseID VARCHAR(255),
        Status ENUM('Active', 'WaitList', 'Complete'),
        ClassSize INT DEFAULT 100,
        PRIMARY KEY (StudentID, CourseID),
        FOREIGN KEY (StudentID) REFERENCES STUDENT(StudentID),
        FOREIGN KEY (CourseID) REFERENCES SCHEDULE(CourseCode)
    );
    """
    try:
        mycursor.execute(query, multi=True)
        print("Table ENROLLMENT created successfully.")
    except Error as e:
        print(e)

def insertStudent(mydb, student_number, fname, lname, class_year, major1, major2, minor1, advisor):
    mycursor = mydb.cursor()
    query = """INSERT INTO STUDENT 
    (StudentNumber, Fname, Lname, ClassYear, Major1, Major2, Minor1, Advisor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
    values = (student_number, fname, lname, class_year, major1, major2, minor1, advisor)
    try:
        mycursor.execute(query, values)
        mydb.commit()
    except Error as e:
        print(e)

def insertEnrollment(mydb, student_id, course_id, status):
    mycursor = mydb.cursor()
    query = """INSERT INTO ENROLLMENT 
    (StudentID, CourseID, Status)
    VALUES (%s, %s, %s);"""
    values = (student_id, course_id, status)
    try:
        mycursor.execute(query, values)
        mydb.commit()
    except Error as e:
        print(e)

def populateSampleData(mydb):
    #let's insert sample students
    students = [
        ('1814624', 'Aasmin', 'Lama Tamang', 2023, 'Computer Science', 'Math', 'African American Studies', 'D Hughes'),

    ]
    for student in students:
        insertStudent(mydb, *student)

    #sample enrollments
    enrollments = [
        (1, 'CS220', 'Active'),
    ]
    for enrollment in enrollments:
        insertEnrollment(mydb, *enrollment)


def insertScheduleRecord(mydb, department, course_code, course_title, instructor, days, begin_time, end_time, building_room, credits, year, term):
    #function implementation

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
    department = None  #keep track of the current department

    with open(filepath, newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        print(filepath)
        for row in csvreader:
            #print(len(row))
            #check if the row is a department heading
            if len(row) == 1 or not any(row[1:]):
                department = row[0]
                continue

            if len(row) < 7:
                continue

            course_code_title = row[0].split(maxsplit=1)
            if len(course_code_title) < 2:
                continue  #skip the course code or title if missing
            course_code = course_code_title[0]
            course_title = course_code_title[1]
            print(course_title)
            insertScheduleRecord(mydb, department, course_code, course_title, row[1], row[2], row[3], row[4], row[5], row[6], 2021, 'Fall')


def main():
    try:
        mydb = connect(
            host="localhost",
            user="root",
            password="Moompopo00Z=",
            database="DS230"
        )

        #createScheduleTable(mydb)
        #createStudentTable(mydb)
        #createEnrollmentTable(mydb)

        filepath = 'C:/Users/test1/PycharmProjects/9/5/pythonProject3/Course Schedule.csv'
        processCsvFile(mydb, filepath)

        populateSampleData(mydb)
        selectSchedule(mydb)

    except Error as e:
        print(e)

main()

from mysql.connector import connect, Error
import csv
import pandas as pd
import random
import string

def createScheduleTable(mydb):
    mycursor = mydb.cursor()
    query = """
    DROP TABLE IF EXISTS ENROLLMENT;
    DROP TABLE IF EXISTS SCHEDULE;
    CREATE TABLE SCHEDULE (
        CourseID INT AUTO_INCREMENT PRIMARY KEY,
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
#PART II Simple queries with schedule
def queryCoursesByDepartment(mydb, department):
    """
    Query and return courses offered by a specific department from the SCHEDULE table.

    :param mydb: MySQL database connection object.
    :param department: The department to query courses for.
    :return: None, but prints the courses offered by the department.
    """
    mycursor = mydb.cursor()
    query = """
    SELECT CourseCode, CourseTitle, Instructor, Days, BeginTime, EndTime, BuildingRoom, Credits, Year, Term
    FROM SCHEDULE
    WHERE CourseCode = %s
    """
    try:
        mycursor.execute(query, (department,))
        courses = mycursor.fetchall()
        print("Table created successfully.")
        for course in courses:
            print(course)
    except Error as e:
        print(e)

def queryCoursesByTimeBlock(mydb, days, begin_time, end_time):
    """
    Query and return courses offered in a specific time block.

    :param mydb: MySQL database connection object.
    :param days: Days of the week (e.g., 'MWF', 'TR').
    :param begin_time: Start time of the time block.
    :param end_time: End time of the time block.
    :return: None, but prints the courses available in the time block.
    """
    mycursor = mydb.cursor()
    query = """
    SELECT CourseCode, CourseTitle, Instructor, Days, BeginTime, EndTime 
    FROM SCHEDULE 
    WHERE Days = %s AND BeginTime >= %s AND EndTime <= %s
    """
    try:
        mycursor.execute(query, (days, begin_time, end_time))
        courses = mycursor.fetchall()
        print("Table created successfully.")
        for course in courses:
            print(course)
    except Error as e:
        print(e)

def queryCoursesForGenEd(mydb, departments):
    """
    Query and return courses that satisfy a general education requirement.

    :param mydb: MySQL database connection object.
    :param departments: List of department codes for the general education requirement (e.g., ['ANT', 'ECO']).
    :return: None, but prints the courses that satisfy the requirement.
    """
    mycursor = mydb.cursor()
    format_strings = ','.join(['%s'] * len(departments))
    query = f"""
    SELECT CourseCode, CourseTitle, Department 
    FROM SCHEDULE 
    WHERE CourseCode IN ({format_strings})
    """
    #You have to change format_strings in mySQL to literal string CourseCodes
    try:
        mycursor.execute(query, tuple(departments))
        courses = mycursor.fetchall()
        print("Table created successfully.")
        for course in courses:
            print(course)
    except Error as e:
        print(e)
def queryCoursesForDCP(mydb):
    """
    Query and return courses that satisfy the DCP requirement.

    :param mydb: MySQL database connection object.
    :return: None, but prints the courses that satisfy the DCP requirement.
    """
    mycursor = mydb.cursor()
    query = """
    SELECT CourseCode, CourseTitle, Department 
    FROM SCHEDULE 
    WHERE CourseCode LIKE '%6' OR CourseCode LIKE '%7' OR CourseCode LIKE '%8'
    """
    try:
        mycursor.execute(query)
        courses = mycursor.fetchall()
        print("Table created successfully.")
        for course in courses:
            print(course)
    except Error as e:
        print(e)

def queryCoursesByProfessor(mydb, professor_name):
    """
    Query and return courses offered by a specific professor.

    :param mydb: MySQL database connection object.
    :param professor_name: The name of the professor.
    :return: None, but prints the courses offered by the professor.
    """
    mycursor = mydb.cursor()
    query = """
    SELECT CourseCode, CourseTitle, Instructor 
    FROM SCHEDULE 
    WHERE Instructor = %s
    """
    try:
        mycursor.execute(query, (professor_name,))
        courses = mycursor.fetchall()
        print("Table Professor created successfully.")
        for course in courses:
            print(course)
    except Error as e:
        print(e)

#III- the Course Registration

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
        CourseID INT,
        Status ENUM('Active', 'WaitList', 'Complete'),
        ClassSize INT DEFAULT 100,
        PRIMARY KEY (StudentID, CourseID),
        FOREIGN KEY (CourseID) REFERENCES SCHEDULE(CourseID),
        FOREIGN KEY (StudentID) REFERENCES STUDENT(StudentID)
       
    );
    """
    try:
        mycursor.execute(query, multi=True)
        print("Table ENROLLMENT created successfully.")
    except Error as e:
        print(e)

def insertStudent(mydb, studentID, fname, lname, class_year, major1, major2, minor1, advisor):
    mycursor = mydb.cursor()
    query = """INSERT INTO STUDENT 
    (StudentNumber, Fname, Lname, ClassYear, Major1, Major2, Minor1, Advisor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE StudentID = StudentID;""" #this line is updated to anonymization function
    values = (studentID, fname, lname, class_year, major1, major2, minor1, advisor)
    try:
        mycursor.execute(query, values)
        mydb.commit()
    except Error as e:
        print(e)

def insertEnrollment(mydb, student_id, course_id, status):
    mycursor = mydb.cursor()
    query = """INSERT INTO ENROLLMENT 
    (StudentID, CourseID, Status, classSize)
    VALUES (%s, %s, %s, %s);"""
    values = (student_id, course_id, status)
    try:
        mycursor.execute(query, values)
        mydb.commit()
    except Error as e:
        print(e)

def populateSampleData(mydb):
    #let's insert sample students
    students = [
        ('1814624', 'Aasmin', 'Lama Tamang', 2027, 'Computer Science', 'Math', 'African American Studies', 'D Hughes'),
        ('1814628', 'Brahim', 'El Marrakechy', 2024, 'Computer Science', None, None, 'D Hughes'),
        ('1672830', 'Andrew', 'Tate', 2025, 'Mathematics', None, 'Computer Science', 'L McQueen'),
        ('1589372', 'Danny', 'Mullen', 2024, 'Economics', 'Data Science', None, 'L McQueen'),
        ('1766691', 'Mia', 'Daniel', 2025, 'Biology', None, None, 'CJ'),
    ]
    for student in students:
        insertStudent(mydb, *student)


    enrollments = [
        (1, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
        (2, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'WaitList'),
        (3, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
        (4, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
        (5, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
    ]
    for enrollment in enrollments:
        insertEnrollment(mydb, *enrollment)


def insertScheduleRecord(mydb, department, course_code, course_title, instructor, days, begin_time, end_time, building_room, credits, year, term):

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

#IV- Anonymize and load student data

def generate_random_id():
    return random.randint(10000000, 99999999)

def generate_random_name():
    first_names = ["Alice", "Bob", "Carol", "Dave"]
    last_names = ["Smith", "Johnson", "Lee", "Garcia"]
    return random.choice(first_names), random.choice(last_names)

#I had problems with this in terms of reading the file utf-8 !!


# Reading the registration data
#registration_df = pd.read_csv('Registration.csv')


# Anonymize the data
# unique_ids = registration_df['ID'].unique()
# id_map = {old_id: generate_random_id() for old_id in unique_ids}
# name_map = {old_id: generate_random_name() for old_id in unique_ids}
#
# registration_df['ID'] = registration_df['ID'].map(id_map)
# registration_df['F_Name'] = registration_df['ID'].map(lambda x: name_map[x][0])
# registration_df['L_Name'] = registration_df['ID'].map(lambda x: name_map[x][1])
#
# def insertAnonymizedEnrollmentData(mydb, registration_df):
#     for index, row in registration_df.iterrows():
#         insertStudent(mydb, row['ID'], row['F_Name'], row['L_Name'],
#                       row['Class'], row['Major 1'], row['Major 2'],
#                       row['Minor 1'], row['Advisor'])
#
#         enrollments = [
#             (1, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
#             (2, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'WaitList'),
#             (3, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
#             (4, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
#             (5, 'CS 220' 'CS 125' 'CS 135' 'CS 215', 'Active'),
#         ]
#         for enrollment in enrollments:
#             insertEnrollment(mydb, *enrollment)
#
#
#     print("Data inserted successfully.")


#V Complex Queries

def queryStudentsInCourse(mydb, course_code):
    mycursor = mydb.cursor()
    query = """
    SELECT s.StudentID, s.Fname, s.Lname
    FROM STUDENT s
    JOIN ENROLLMENT e ON s.StudentID = e.StudentID
    WHERE e.CourseID = (SELECT CourseID FROM SCHEDULE WHERE CourseCode = %s);
    """
    try:
        mycursor.execute(query, (course_code,))
        for student in mycursor.fetchall():
            print(student)
    except Error as e:
        print(e)

def queryAvgClassSizeByDepartment(mydb):
    mycursor = mydb.cursor()
    query = """
    SELECT d.Department, AVG(e.ClassSize) as AvgClassSize
    FROM SCHEDULE d
    JOIN ENROLLMENT e ON d.CourseID = e.CourseID
    GROUP BY d.Department;
    """
    try:
        mycursor.execute(query)
        for avg in mycursor.fetchall():
            print(avg)
    except Error as e:
        print(e)

def queryTopEnrolledCourses(mydb):
    mycursor = mydb.cursor()
    query = """
    SELECT c.CourseCode, c.CourseTitle, COUNT(e.StudentID) as Enrollment
    FROM SCHEDULE c
    JOIN ENROLLMENT e ON c.CourseID = e.CourseID
    GROUP BY c.CourseCode, c.CourseTitle
    ORDER BY Enrollment DESC
    LIMIT 5;
    """
    try:
        mycursor.execute(query)
        for course in mycursor.fetchall():
            print(course)
    except Error as e:
        print(e)

def queryStudentCountByYear(mydb):
    mycursor = mydb.cursor()
    query = """
    SELECT ClassYear, COUNT(*) as NumberOfStudents
    FROM STUDENT
    GROUP BY ClassYear;
    """
    try:
        mycursor.execute(query)
        for count in mycursor.fetchall():
            print(count)
    except Error as e:
        print(e)

def queryCoursesByTerm(mydb, term):
    mycursor = mydb.cursor()
    query = """
    SELECT CourseCode, CourseTitle
    FROM SCHEDULE
    WHERE Term = %s;
    """
    try:
        mycursor.execute(query, (term,))
        for course in mycursor.fetchall():
            print(course)
    except Error as e:
        print(e)

def queryWaitlistedStudents(mydb, course_code):
    mycursor = mydb.cursor()
    query = """
    SELECT s.StudentID, s.Fname, s.Lname
    FROM STUDENT s
    JOIN ENROLLMENT e ON s.StudentID = e.StudentID
    WHERE e.Status = 'WaitList' AND e.CourseID = (SELECT CourseID FROM SCHEDULE WHERE CourseCode = %s);
    """
    try:
        mycursor.execute(query, (course_code,))
        for student in mycursor.fetchall():
            print(student)
    except Error as e:
        print(e)



# VI- New functionality (pre requiisites)

def createPreRequisiteTable(mydb):
    mycursor = mydb.cursor()
    query = """
    DROP TABLE IF EXISTS PREREQUISITE;
    CREATE TABLE PREREQUISITE (
        CourseID INT,
        PrerequisiteID INT,
        PRIMARY KEY (CourseID, PrerequisiteID),
        FOREIGN KEY (CourseID) REFERENCES SCHEDULE(CourseID),
        FOREIGN KEY (PrerequisiteID) REFERENCES SCHEDULE(CourseID)
    );
    """
    try:
        mycursor.execute(query, multi=True)
        print("Table PREREQUISITE created successfully.")
    except Error as e:
        print(e)

def insertPreRequisite(mydb, course_id, prerequisite_id):
    mycursor = mydb.cursor()
    query = """INSERT INTO PREREQUISITE (CourseID, PrerequisiteID) VALUES (%s, %s);"""
    values = (course_id, prerequisite_id)
    try:
        mycursor.execute(query, values)
        mydb.commit()
        print(f"Pre-requisite for course {course_id} added successfully.")
    except Error as e:
        print(e)

def Enrollment(mydb, student_id, course_id, status):
    mycursor = mydb.cursor()
    query = """
    SELECT PrerequisiteID
    FROM PREREQUISITE
    WHERE CourseID = %s
    """
    try:
        mycursor.execute(query, (course_id,))
        prerequisites = mycursor.fetchall()

        for prereq in prerequisites:
            prereq_query = """
            SELECT *
            FROM ENROLLMENT
            WHERE StudentID = %s AND CourseID = %s AND Status = 'Complete'
            """
            mycursor.execute(prereq_query, (student_id, prereq[0]))
            if not mycursor.fetchone():
                print(f"Cannot enroll, prerequisite CourseID {prereq[0]} not met for CourseID {course_id}")
                return

        enrollment_query = """INSERT INTO ENROLLMENT (StudentID, CourseID, Status) VALUES (%s, %s, %s);"""
        mycursor.execute(enrollment_query, (student_id, course_id, status))
        mydb.commit()
        print("Enrollment successful.")
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

        #filepath = 'C:/Users/test1/PycharmProjects/9/5/pythonProject3/Course Schedule.csv'
        #processCsvFile(mydb, filepath)
        #populateSampleData(mydb)
        #selectSchedule(mydb)

        #Query a: Courses in a specific deparment
        department_to_query = "ACC"
        print(f"Courses in the {department_to_query} department:")
        queryCoursesByDepartment(mydb, department_to_query)

        # Query b: Courses available at a given time-block
        begin_time = "9:00 AM"
        end_time = "9:50 AM"
        days = "MWF"
        print(f"\nCourses available at {begin_time}:")
        #queryCoursesByTimeBlock(mydb, days, begin_time, end_time)

        # Query c: Courses for General Education Requirement
        gen_ed_departments = ['ANT', 'ECO', 'POL', 'PSY', 'SOC']
        print("\nCourses for Social Sciences general education requirement:")
        #queryCoursesForGenEd(mydb, gen_ed_departments)

        # Query d: Courses for DCP Requirement
        print("\nCourses for DCP Requirement:")
        #queryCoursesForDCP(mydb)

        # Query e: Courses offered by a favorite professor
        favorite_professor = "C Johnson"
        print(f"\nCourses offered by {favorite_professor}:")
        #queryCoursesByProfessor(mydb, favorite_professor)

        #queryStudentsInCourse(mydb, "CS135")

        #registration_df = pd.read_csv('Registration.csv')
        #anonymize data as shown above
        #insertAnonymizedEnrollmentData(mydb, registration_df)

        #queryStudentsInCourse(mydb, "CS135")
        #queryAvgClassSizeByDepartment(mydb)
        #queryTopEnrolledCourses(mydb)
        #queryStudentCountByYear(mydb)
        #queryCoursesByTerm(mydb, "Fall 2023")
        #queryWaitlistedStudents(mydb, "CS125")

        #createPreRequisiteTable(mydb)
        #adding pre-requisites (use actual CourseIDs)
        #insertPreRequisite(mydb, course_id=135, prerequisite_id=125)

        #enrolling a student in a course with and without meeting pre-requisites
        #Enrollment(mydb, student_id=1, course_id=135, status='Active')




    except Error as e:
        print(e)

main()


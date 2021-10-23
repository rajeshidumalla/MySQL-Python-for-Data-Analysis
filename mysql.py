#!/usr/bin/env python
# coding: utf-8

# # MySQL + Python for Data Analysts
# 
# Using [MySQL Connector](https://dev.mysql.com/doc/connector-python/en/) and Python to implement a database on MySQL Server, and to create, read, update and delete data in that database.
# 
# 
# 
# ## Introduction
# 
# The notebook takes the reader step-by-step through all the processes involved with using Python and the [MySQL Connector](https://dev.mysql.com/doc/connector-python/en/) to perform the standard [CRUD functions](https://stackify.com/what-are-crud-operations/) on a database running on [MySQL Server](https://dev.mysql.com/downloads/mysql/).
# 
# 
# I will be implementing the same code to build the below database for the International Language School via Python using MySQL Connector.
# 
# 
# [![Entity Relationship Diagram for Database][erd]][link1]
# 
# [erd]: ./img/ERD.png
# [link1]: https://towardsdatascience.com/designing-a-relational-database-and-creating-an-entity-relationship-diagram-89c1c19320b2
# 
# -----------------
# 
# 
# ### Methods used
# * Defining functions in Python
# * Database Implementation
# * Creating, Reading, Updating and Deleting data using SQL and Python
# 
# 
# ### Technologies used
# * [MySQL Community Server](https://dev.mysql.com/downloads/mysql/)
# * [MySQL Python Connector](https://dev.mysql.com/doc/connector-python/en/)
# * [PopSQL](https://popsql.com/)
# * [Jupyter Notebook](https://jupyter.org/)
# * [pandas](https://pandas.pydata.org/)
# 
# ---------------------
# ### 1. Import Libraries
# 
# ##### 1.1 - Import Libraries
# 
# The first step is to import [MySQL Connector](https://dev.mysql.com/doc/connector-python/en/) and [pandas](https://pandas.pydata.org/).

# In[1]:


import mysql.connector
from mysql.connector import Error
import pandas as pd


# -------------------
# 
# ### 2. Connect to Server and Create Database
# 
# ##### 2.1 - Define Server Connection Function 
# 
# Next I am going to define a function in python which connects to MySQL Server. To do this, I am going to use the [mysql.connector.connect()](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysql-connector-connect.html) method.
# 
# **N.B. Update the variable 'pw' with the root password for your MySQL Server! Otherwise the connection cannot be made.**

# In[2]:


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

pw = "Tlord422" # IMPORTANT! MySQL Terminal password here.
db = "school" # This is the name of the database we will create in the next step - call it whatever you like.

connection = create_server_connection("localhost", "root", pw)


# ##### 2.2 - Create a New Database
# 
# Now defining a function to create a new database on the server. Here I am using [cursor.execute()](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html) to execute a [CREATE DATABASE](https://dev.mysql.com/doc/refman/8.0/en/creating-database.html) SQL command.

# In[3]:


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

create_database_query = "CREATE DATABASE school"
create_database(connection, create_database_query)


# ##### 2.3 - Modify Server Connection Function, Create Database Connection Function
# 
# Now that I've created a DB, let's modify the create_server_connection function to create a new function for connecting directly to that DB. This will prove more useful than just connecting to the server.

# In[4]:


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# ##### 2.4 - Define Query Execution Function
# 
# The final step of this stage is to create a function which will allow us to execute queries written in SQL. This is going to be extremely useful!
# 
# Again, we use [cursor.execute()](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html) to execute our commands.

# In[5]:


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# -------------------
# 
# ### 3. Creating Tables
# 
# ##### 3.1 - Create Teacher Table
# 
# Now let's create our first table inside our DB, using our newly defined functions.

# In[6]:


# Assign our SQL command to a python variable using triple quotes to create a multi-line string
create_teacher_table = """
CREATE TABLE teacher (
  teacher_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  language_1 VARCHAR(3) NOT NULL,
  language_2 VARCHAR(3),
  dob DATE,
  tax_id INT UNIQUE,
  phone_no VARCHAR(20)
  );
 """

connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
execute_query(connection, create_teacher_table) # Execute our defined query


# If we check in our MySQL terminal using SHOW TABLES; we see the following:
# 
# ![MySQL Terminal Screen confirming table creation](./img/01.jpg)
# 
# Success! The teacher table has been created.
# 
# ##### 3.2 - Create Remaining Tables
# 
# Now let's create the rest of our tables.

# In[7]:


create_client_table = """
CREATE TABLE client (
  client_id INT PRIMARY KEY,
  client_name VARCHAR(40) NOT NULL,
  address VARCHAR(60) NOT NULL,
  industry VARCHAR(20)
);
 """

create_participant_table = """
CREATE TABLE participant (
  participant_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  phone_no VARCHAR(20),
  client INT
);
"""

create_course_table = """
CREATE TABLE course (
  course_id INT PRIMARY KEY,
  course_name VARCHAR(40) NOT NULL,
  language VARCHAR(3) NOT NULL,
  level VARCHAR(2),
  course_length_weeks INT,
  start_date DATE,
  in_school BOOLEAN,
  teacher INT,
  client INT
);
"""


connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, create_client_table)
execute_query(connection, create_participant_table)
execute_query(connection, create_course_table)


# ##### 3.3 - Define Foreign Key Relationships
# 
# Now altering the tables to create Foreign Key relationships (see the accompanying [SQL Tutorial Series](https://towardsdatascience.com/tagged/sql-series) on [Towards Data Science](https://towardsdatascience.com/) for the background on all of this), and creating our final table, takes_course

# In[8]:


alter_participant = """
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

alter_course = """
ALTER TABLE course
ADD FOREIGN KEY(teacher)
REFERENCES teacher(teacher_id)
ON DELETE SET NULL;
"""

alter_course_again = """
ALTER TABLE course
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

create_takescourse_table = """
CREATE TABLE takes_course (
  participant_id INT,
  course_id INT,
  PRIMARY KEY(participant_id, course_id),
  FOREIGN KEY(participant_id) REFERENCES participant(participant_id) ON DELETE CASCADE, -- it makes no sense to keep this rtelation when a participant or course is no longer in the system, hence why CASCADE this time
  FOREIGN KEY(course_id) REFERENCES course(course_id) ON DELETE CASCADE
);
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, alter_participant)
execute_query(connection, alter_course)
execute_query(connection, alter_course_again)
execute_query(connection, create_takescourse_table)


# -----------------
# 
# ### 4. Populate Tables
# 
# The next step is to populate our tables with data, starting with the teacher table
# 
# ##### 4.1 - Populate Teacher Table
# 
# Here we again assign a multi-line string with our SQL command to a variable, and then call our create_db_connection and execute_query functions.

# In[9]:


pop_teacher = """
INSERT INTO teacher VALUES
(1,  'James', 'Smith', 'ENG', NULL, '1985-04-20', 12345, '+491774553676'),
(2, 'Stefanie',  'Martin',  'FRA', NULL,  '1970-02-17', 23456, '+491234567890'), 
(3, 'Steve', 'Wang',  'MAN', 'ENG', '1990-11-12', 34567, '+447840921333'),
(4, 'Friederike',  'Müller-Rossi', 'DEU', 'ITA', '1987-07-07',  45678, '+492345678901'),
(5, 'Isobel', 'Ivanova', 'RUS', 'ENG', '1963-05-30',  56789, '+491772635467'),
(6, 'Niamh', 'Murphy', 'ENG', 'IRI', '1995-09-08',  67890, '+491231231232');
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, pop_teacher)


# Let's take a look in MySQL terminal:
# 
# ![MySQL Terminal Screen confirming table population](./img/02.jpg)
# 
# Success! We can see the table has been properly filled with all of our data.
# 
# ##### 4.2 - Populate Remaining Tables
# 
# Now, let's populate the remaining tables.

# In[10]:


pop_client = """
INSERT INTO client VALUES
(101, 'Big Business Federation', '123 Falschungstraße, 10999 Berlin', 'NGO'),
(102, 'eCommerce GmbH', '27 Ersatz Allee, 10317 Berlin', 'Retail'),
(103, 'AutoMaker AG',  '20 Künstlichstraße, 10023 Berlin', 'Auto'),
(104, 'Banko Bank',  '12 Betrugstraße, 12345 Berlin', 'Banking'),
(105, 'WeMoveIt GmbH', '138 Arglistweg, 10065 Berlin', 'Logistics');
"""

pop_participant = """
INSERT INTO participant VALUES
(101, 'Marina', 'Berg','491635558182', 101),
(102, 'Andrea', 'Duerr', '49159555740', 101),
(103, 'Philipp', 'Probst',  '49155555692', 102),
(104, 'René',  'Brandt',  '4916355546',  102),
(105, 'Susanne', 'Shuster', '49155555779', 102),
(106, 'Christian', 'Schreiner', '49162555375', 101),
(107, 'Harry', 'Kim', '49177555633', 101),
(108, 'Jan', 'Nowak', '49151555824', 101),
(109, 'Pablo', 'Garcia',  '49162555176', 101),
(110, 'Melanie', 'Dreschler', '49151555527', 103),
(111, 'Dieter', 'Durr',  '49178555311', 103),
(112, 'Max', 'Mustermann', '49152555195', 104),
(113, 'Maxine', 'Mustermann', '49177555355', 104),
(114, 'Heiko', 'Fleischer', '49155555581', 105);
"""

pop_course = """
INSERT INTO course VALUES
(12, 'English for Logistics', 'ENG', 'A1', 10, '2020-02-01', TRUE,  1, 105),
(13, 'Beginner English', 'ENG', 'A2', 40, '2019-11-12',  FALSE, 6, 101),
(14, 'Intermediate English', 'ENG', 'B2', 40, '2019-11-12', FALSE, 6, 101),
(15, 'Advanced English', 'ENG', 'C1', 40, '2019-11-12', FALSE, 6, 101),
(16, 'Mandarin für Autoindustrie', 'MAN', 'B1', 15, '2020-01-15', TRUE, 3, 103),
(17, 'Français intermédiaire', 'FRA', 'B1',  18, '2020-04-03', FALSE, 2, 101),
(18, 'Deutsch für Anfänger', 'DEU', 'A2', 8, '2020-02-14', TRUE, 4, 102),
(19, 'Intermediate English', 'ENG', 'B2', 10, '2020-03-29', FALSE, 1, 104),
(20, 'Fortgeschrittenes Russisch', 'RUS', 'C1',  4, '2020-04-08',  FALSE, 5, 103);
"""

pop_takescourse = """
INSERT INTO takes_course VALUES
(101, 15),
(101, 17),
(102, 17),
(103, 18),
(104, 18),
(105, 18),
(106, 13),
(107, 13),
(108, 13),
(109, 14),
(109, 15),
(110, 16),
(110, 20),
(111, 16),
(114, 12),
(112, 19),
(113, 19);
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, pop_client)
execute_query(connection, pop_participant)
execute_query(connection, pop_course)
execute_query(connection, pop_takescourse)


# --------------
# 
# ### 5. Reading Data
# 
# ##### 5.1 - Define Data Reading Function
# 
# Now that we have populated our tables, it's time to start creating read queries. To do this, we will need a new function.

# In[11]:


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


# ##### 5.2 - Read Data from Database
# 
# Let's try this with a simple query to begin with.

# In[12]:


q1 = """
SELECT *
FROM teacher;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

for result in results:
  print(result)


# And here are some more queries to try.

# In[13]:


q2 = """
SELECT last_name, dob
FROM teacher;
"""

q3 = """
SELECT *
FROM course
WHERE language = 'ENG'
ORDER BY start_date DESC;
"""

q4 = """
SELECT first_name, last_name, phone_no
FROM teacher
WHERE dob < '1990-01-01';
"""

q5 = """
SELECT course.course_id, course.course_name, course.language, client.client_name, client.address
FROM course
JOIN client
ON course.client = client.client_id
WHERE course.in_school = FALSE;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q5)

for result in results:
  print(result)


# ##### 5.3 - Formatting Output into a List
# 
# Now we can assign the results to a list, to use further in our python applications or scripts.
# 
# The following code returns the results of our query as a list of tuples.

# In[14]:


#Initialise empty list
from_db = []

# Loop over the results and append them into our list, different styles

# Returns a list of tuples
for result in results:
  result = result
  from_db.append(result)
    
print(from_db)


# ##### 5.4 - Formatting Output into a List of Lists
# 
# If we want to, we can make this return a list of lists instead, like so:

# In[15]:


# Returns a list of lists
from_db = []

for result in results:
  result = list(result)
  from_db.append(result)
    
print(from_db)


# ##### 5.5 - Formatting Output into a pandas [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)
# 
# With a little more work (creating a list with our column names), we can create a pandas DataFrame like so:

# In[16]:


# Returns a list of lists and then creates a pandas DataFrame
from_db = []

for result in results:
  result = list(result)
  from_db.append(result)


columns = ["course_id", "course_name", "language", "client_name", "address"]
df = pd.DataFrame(from_db, columns=columns)

display(df)


# ### 6. Updating Records
# 
# Sometimes we will need to update our Database. We can do this very easily using our execute_query function alongside the SQL [UPDATE](https://dev.mysql.com/doc/refman/8.0/en/update.html) statement.
# 
# ##### 6.1 - Updating Client Address
# 
# The School receives notification that the Big Business Federation has moved office, and now they are located at 23 Fingiertweg, 14534 Berlin. We can change that in our database like so:

# In[17]:


update = """
UPDATE client 
SET address = '23 Fingiertweg, 14534 Berlin' 
WHERE client_id = 101;
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, update)


# Let's see if that worked.

# In[18]:


q1 = """
SELECT *
FROM client
WHERE client_id = 101;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

for result in results:
  print(result)


# Excellent!
# 
# Notice that as well as using "WHERE client_id = 101" in the UPDATE query, we could also have used "WHERE client_name = 'Big Business Federation'" or "WHERE address = '123 Falschungstraße, 10999 Berlin'" (or even "WHERE address LIKE '%Falschung%'"). The important thing is that the WHERE clause allows us to uniquely identify the record we want to update. 
# 
# Running the query without a WHERE clause would update all address records in the table, which is very much not what we wanted to do.
# 
# -----------------
# 
# ### 7. Deleting Records
# 
# ##### 7.1 - Deleting a Course
# 
# We can also use our execute_query function to delete records, by using DELETE FROM.
# 
# Let's try this with our course table. First let's remind ourselves of the courses contained in the table.

# In[19]:


q1 = """
SELECT *
FROM course;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)


# Let's delete the course with course_id 20 - 'Fortgeschrittenes Russisch'. For this we will use the [DELETE](https://dev.mysql.com/doc/refman/8.0/en/delete.html) SQL command.

# In[20]:


delete_course = """
DELETE FROM course WHERE course_id = 20;
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, delete_course)


# Let's confirm that the course is gone.

# In[21]:


q1 = """
SELECT *
FROM course;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)


# It's gone! Good work.
# 
# ##### 7.2 - Restoring the Course
# 
# Let's put that course back - it's a perfectly good course.

# In[22]:


restore_russian = """
INSERT INTO course VALUES
(20, 'Fortgeschrittenes Russisch', 'RUS', 'C1',  4, '2020-04-08',  FALSE, 5, 103);
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, restore_russian)


q1 = """
SELECT *
FROM course;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)


# Excellent.
# 
# This also works with DROP TABLE if we want to get rid of a whole table at once. We won't do that in this tutorial, but feel free to try it yourself. In this notebook we can always create and populate the table again - in a real production environment we will need to be much more careful with the DELETE and DROP operations. 
# 
# And always remember to back up our database!
# 
# ----------------
# 
# ### 8. Creating Records from Lists
# 
# We saw in Section 4 that we can use the SQL [INSERT](https://dev.mysql.com/doc/refman/8.0/en/insert.html) command in our execute_query function to insert records into our tables.
# 
# MySQL Connector also gives us a way to do this in a more 'pythonic' fashion, using a list of tuples as our input, where each tuple contains the data we wish to insert into our table. This is extremely useful for updating our database with data which may have been generated by an application we have written in Python, such as logs of user activity on a social media app, for example.
# 
# To do this, we will use the [executemany()](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-executemany.html) method, instead of the simpler [execute()](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html) method we have been using thus far.
# 
# This method is also more secure if our database is open to our users at any point, as it helps to prevent against [SQL Injection](https://en.wikipedia.org/wiki/SQL_injection) attacks, which can damage or even destroy our whole database.
# 
# ##### 8.1 - Create Execute List Query Function
# 
# To see how we can do this, let's add a couple of new teachers to our teacher table.
# 
# Fist let's modify our execute_query function to use [executemany()](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-executemany.html) and to accept one more argument.

# In[23]:


def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# ##### 8.2 - Add New Teachers
# 
# Now let's create a list containing the data for our new teachers (each stored within a [tuple](https://www.w3schools.com/python/python_tuples.asp)), and the SQL command to perform our action.
# 
# Notice that the SQL command requires a '%s' placeholder for each of the columns we wish to act upon, so in this case we need 8 for the 8 columns we wish to add values for.

# In[24]:


sql = '''
    INSERT INTO teacher (teacher_id, first_name, last_name, language_1, language_2, dob, tax_id, phone_no) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
val = [
    (7, 'Hank', 'Dodson', 'ENG', None, '1991-12-23', 11111, '+491772345678'), 
    (8, 'Sue', 'Perkins', 'MAN', 'ENG', '1976-02-02', 22222, '+491443456432')
]


connection = create_db_connection("localhost", "root", pw, db)
execute_list_query(connection, sql, val)


# In[25]:


q1 = """
SELECT *
FROM teacher;
"""

connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)


# Welcome to the ILS, Hank and Sue!
# 
# This method can allow us to create new records in our database (or read, update or delete existing records) using a python list as our input. It is difficult to overstate how useful this can be when we are working with Python and SQL together.
# 
# --------------------
# 
# ### 9. Conclusion
# 
# ##### 9.1 - Conclusion
# 
# From using Python and MySQL Connector to create an entirely new database in MySQL Server, creating tables, defining their relationships to one another and populating them with data. We have covered how to [Create, Read, Update and Delete](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete) data in our database.
# 
# We have looked at how to extract data from existing databases and load them into pandas DataFrames, ready for analysis and further work taking advantage of all the possibilities offered by the [PyData stack](https://www.pluralsight.com/guides/a-lap-around-the-pydata-stack). Going the other direction, we have also learned how to take data generated by our Python scripts and applications, and write those into a database where they can be safely stored for later retrieval.
# 
# I hope it is clear just how powerful each of these programming languages can be for Data Analysts. Using them together makes them even stronger.
# 
# ----------------------------

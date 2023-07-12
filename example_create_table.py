#===========Import and set up zone===============
import os
import ydb
import ydb.iam
from faker import Faker

point = 'grpcs://ydb.serverless.yandexcloud.net:2135'                    # Define the YDB endpoint.
base = '/ru-central1/b1gsuefilguhob2709ps/etn011j4atu9f6amk794'          # Define the YDB database path. 
path = '/ru-central1/b1gsuefilguhob2709ps/etn011j4atu9f6amk794/test_dir' # Define the path of the project directory within YDB.


#======Driver Initialization zone===========
driver = ydb.Driver(                                                     # Initialize the YDB driver.
  endpoint=point,                                                        # Set the endpoint.
  database=base,                                                         # Set the database path.
  credentials=ydb.iam.MetadataUrlCredentials(),                          # Set the credentials for authentication.
)

driver.wait(fail_fast=True, timeout=5)                                   # Launch the driver with a specified timeout.


#==========Session create table inincialization zone==============
session = driver.table_client.session().create()                                             # Create a new session using the driver's table client.

session.create_table(                                                                        # Start the creation of a new table.
        os.path.join(path, 'people'),                                                        # Specify the table path by joining the base path and the table name 'people'.
        ydb.TableDescription()                                                               # Begin describing the table structure.
        .with_column(ydb.Column('people_id', ydb.PrimitiveType.Uint64))                      # Define a non-nullable column 'people_id' of Uint64 type.
        .with_column(ydb.Column('name', ydb.OptionalType(ydb.PrimitiveType.Utf8)))           # Define a nullable column 'name' of Utf8 type.
        .with_column(ydb.Column('username', ydb.OptionalType(ydb.PrimitiveType.Utf8)))       # Define a nullable column 'username' of Utf8 type.
        .with_column(ydb.Column('job', ydb.OptionalType(ydb.PrimitiveType.Utf8)))            # Define a nullable column 'job' of Utf8 type.
        .with_column(ydb.Column('sex', ydb.OptionalType(ydb.PrimitiveType.Utf8)))            # Define a nullable column 'sex' of Utf8 type.
        .with_column(ydb.Column('birthdate', ydb.OptionalType(ydb.PrimitiveType.Datetime)))  # Define a nullable column 'birthdate' of Datetime type.
        .with_column(ydb.Column('residence', ydb.OptionalType(ydb.PrimitiveType.Utf8)))      # Define a nullable column 'residence' of Utf8 type.
        .with_column(ydb.Column('ssn', ydb.OptionalType(ydb.PrimitiveType.Utf8)))            # Define a nullable column 'ssn' of Utf8 type.
        .with_column(ydb.Column('company', ydb.OptionalType(ydb.PrimitiveType.Utf8)))        # Define a nullable column 'company' of Utf8 type.
        .with_column(ydb.Column('address', ydb.OptionalType(ydb.PrimitiveType.Utf8)))        # Define a nullable column 'address' of Utf8 type.
        .with_column(ydb.Column('mail', ydb.OptionalType(ydb.PrimitiveType.Utf8)))           # Define a nullable column 'mail' of Utf8 type.
        .with_column(ydb.Column('blood_group', ydb.OptionalType(ydb.PrimitiveType.Utf8)))    # Define a nullable column 'blood_group' of Utf8 type.
        .with_primary_key('people_id')                                                       # Set 'people_id' as the primary key of the table.
    )  

#=======Fake data generation zone==============
fake_data = []                       # Initialize an empty list to store the fake data.
for _ in range(5):                   # Generate fake data for 5 instances.
    fake = Faker()                   # Create an instance of the Faker class.
    fake_data.append(fake.profile()) # Generate a fake profile and append it to the 'fake_data' list.

#==========Creating query to upload data zone==========#
values_str = [                            # Initialize list to hold formatted data.
    f"({i}, '{data['name']}', '{data['username']}', '{data['job']}', '{data['sex']}', {data['birthdate']}, '{data['residence']}', '{data['ssn']}', '{data['company']}', '{data['address']}', '{data['mail']}', '{data['blood_group']}')" 
    for i, data in enumerate(fake_data)]  # Create formatted string for each fake profile in fake_data.

values_str = ', '.join(values_str)        # Join all formatted strings into one string separated by commas.


# Define SQL query.
query = f"""                              
        PRAGMA TablePathPrefix("{path}");
        UPSERT INTO people (people_id, name, username, job, sex, birthdate, residence, ssn, company, address, mail, blood_group) VALUES {values_str};
        """

session.transaction().execute(query, commit_tx=True)  # Execute SQL query in a transaction, commit upon completion.


#==========Creating query to read data from table zone==========#
# Define SQL query.
req_read = f"""
        PRAGMA TablePathPrefix("{path}");
        SELECT name, mail
        FROM people;
            """

result = session.transaction(ydb.SerializableReadWrite()).execute(req_read, commit_tx=True) # Execute SQL query in a transaction with serializable read/write isolation level, commit upon completion.

#===========Print data zone==================#
for row in result[0].rows: # Iterate over each row in the result set.
    print(row) # Print the content of each row.      

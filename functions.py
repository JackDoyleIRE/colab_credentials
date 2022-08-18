# Function to get access credentials
def get_credentials(read: bool, db_creds: dict = uploaded) -> str:
  '''Gets credential for db access from uploaded json file'''

  cred_file = next(iter(db_creds.values()))
  data = json.loads(cred_file.decode())

  if read == True:
    user = data["user1"]
    pass_word = data["pass1"]
  elif read == False:
    user = data["user2"]
    pass_word = data["pass2"]
  credentials = user + ":" + pass_word

  return credentials

# Function to connect to a database
def connect(server: str, db_name: str, read: bool) -> str:
  '''Creates connection string for accessing db '''
  #Calling the get_credentials function
  creds = get_credentials(read)
  conn_string = 'mysql://' + creds + server + '/' + db_name
  return conn_string

# Reading from a MySQL db
def read_db(server: str, db_name: str, query: str) -> pd.DataFrame:
  ''' Reads from DB using server DB name and query '''
  # Calling the connect function which will call the get_credentials function
  conn_string = connect(server, db_name, read=True)
  engine = create_engine(conn_string)

  db_connection = engine.connect()
  # Read to frame
  frame = pd.read_sql(query, db_connection)

  db_connection.close()

  return frame

# Writing to a MySQL db
def write_db(server: str, db_name: str, frame_to_write: pd.DataFrame, table_name: str): 
  ''' When called this function writes a dataframe to a MySQL db'''

  # Calling the connect function which will call the get_credentials function
  conn_string = connect(server, db_name, read=False)
  engine = create_engine(conn_string)

  db_connection = engine.connect()
  # Write frame to DB
  frame_to_write.to_sql(name=table_name, con=db_connection, index=False, if_exists ='replace')

  db_connection.close()

# verson 1.1
import snowflake.connector
from snowflake.connector import DictCursor
import traceback
import datetime
import time

#Credentials ~ snowflakes
ACCOUNT = ''
USER = ''
PASSWORD = ''

class snowCredentials:
    
    __account = None
    __username = None
    __password = None

    __warehouse = None
    __database = None
    __schema = None



    def __init__(self, account, username, password, **kw):
        self.__account = account
        self.__username = username
        self.__password = password

        if 'warehouse' in kw:
            self.__warehouse = kw["warehouse"]
        
        if 'database' in kw:
            self.__database = kw["database"]

        if 'schema' in kw:
            self.__schema = kw["schema"]



    def __repr__(self):
        return "Account: {} \nUser: {} \nWarehouse: {} \nDatabase: {} \nSchema: {}".format(self.__account, self.__username, self.__warehouse, self.__database, self.__schema)


    def account(self, *arg):
        if len(arg) == 0:
            return self.__account

        elif len(arg) == 1:
            self.__account = arg[0]


    def username(self, *arg):
        if len(arg) == 0:
            return self.__username

        elif len(arg) == 1:
            self.__username = arg[0]

    
    def password(self, *arg):
        if len(arg) == 0:
            return self.__password

        elif len(arg) == 1:
            self.__password = arg[0]


    def warehouse(self, *arg):
        if len(arg) == 0:
            return self.__warehouse

        elif len(arg) == 1:
            self.__warehouse = arg[0]

    
    def database(self, *arg):
        if len(arg) == 0:
            return self.__database

        elif len(arg) == 1:
            self.__database = arg[0]


    def schema(self, *arg):
        if len(arg) == 0:
            return self.__schema

        elif len(arg) == 1:
            self.__schema = arg[0]

class snowInterface:
    # global cursors
    __cnx = None
    __dictcnx = None
    # credentials
    __credentials = snowCredentials(None, None, None)


    def __init__(self, credentials):
        self.openInterface(credentials)
        

    def snowConnect(self, warehouse, database, schema):
        try:
            self.__cnx.cursor().execute("USE WAREHOUSE {}".format(warehouse))
            self.__cnx.cursor().execute("USE DATABASE {}".format(database))
            self.__cnx.cursor().execute("USE SCHEMA {}".format(schema))
        except Exception as e:
            print ("There was an error. Make sure the user has permissions for the warehouse: {}, database: {}, and schema: {}. Exception {}".format(warehouse, database, schema, e))
            print (traceback.format_exc())


    def openInterface(self, credentials):
        print('Opening Snowflake Interface')
        if credentials.account() == None or credentials.username() == None or credentials.password() == None:
            raise LookupError('There was an error in the credentials wrapper. Make sure to add account, username, and password')

        self.__cnx = snowflake.connector.connect(
                                                user = credentials.username(),
                                                password = credentials.password(),
                                                account = credentials.account()
        )

        self.__dictcnx = self.__cnx.cursor(DictCursor)

        if credentials.warehouse() != None and credentials.database() != None and credentials.schema() != None:
            self.snowConnect(credentials.warehouse(), credentials.database(), credentials.schema())

        # print("Connected With Credentials: {}".format(credentials))


    def closeInterface(self):
        self.__cnx.close()
        print ('Connection has been closed. Reconnect to use functionality.')
    
    
    def setWarehouse(self, warehouse):

        if not warehouse:
            raise ValueError("Warehouse value passed is Null or Blank.")
        
        self.__credentials.warehouse(warehouse)

        try:
            self.__cnx.cursor().execute("USE WAREHOUSE {}".format(warehouse))
        except Exception as e:
            print ("Error in setting the warehouse withing snowflake. Exception: {}".format(e))
            print (traceback.format_exc())
            self.closeInterface()


    def setDatabase(self, database):
        if not database:
            raise ValueError("Database value passed is Null or Blank.")
        
        self.__credentials.database(database)

        try:
            self.__cnx.cursor().execute("USE DATABASE {}".format(database))
        except Exception as e:
            print ("Error in setting the database withing snowflake. Exception: {}".format(e))
            print (traceback.format_exc())
            self.closeInterface()


    def setSchema(self, schema):
        if not schema:
            raise ValueError("Schema value passed is Null or Blank.")
        
        self.__credentials.schema(schema)

        try:
            self.__cnx.cursor().execute("USE SCHEMA {}".format(schema))
        except Exception as e:
            print ("Error in setting the schema withing snowflake. \n{}".format(e))
            print (traceback.format_exc())
            self.closeInterface()

    def __describeTable(self, tableName):
        cursor = self.__dictcnx

        try:
            cursor.execute("DESCRIBE TABLE {}".format(tableName))
        except Exception as e:
            print ("An error Occured when describing the given table. Exception {}".format(e))
            print (traceback.format_exc())
            self.closeInterface()

        return cursor

    def executeBlock(self, statement):
        try:
            self.__cnx.cursor().execute(statement)
        except Exception as e:
            print("An error occured while executing block. \n{}".format(e))
            print(traceback.format_exc())

    #returns List [colName, colName]
    def getOrderedTabelHeaders(self, tableName):
        headerList = []

        cursor = self.__describeTable(tableName)

        for record in cursor:
            headerList.append(str(record['name']))

        return headerList

    # returns map {colName:colType}
    def getTableHeaders(self, tableName):
        # Map of ColName : ColType of a specified Table
        headersMap = {}
        cursor = self.__describeTable(tableName)

        for record in cursor:
            headersMap[str(record['name'])] = str(record['type'])

        return headersMap


    # Creates a temporary table with timestamp
    @staticmethod
    def __createTempTableName(tableFunction):
        return 'temp{}Table{}'.format(tableFunction, str(datetime.datetime.now()).replace(' ','_').replace('-','_').replace(':','_').replace('.','_'))


    # returns true if all headers exist
    @staticmethod
    def compareTableHeaders(headerMapTarget, headerMapSource):
        for col in headerMapSource:
            name = str(col).upper()
            if name not in headerMapTarget:
                print("Missing Column {} in target table.".format(name))
                return False
        
        return True
    

    # Adds columns to table
    def addColumn(self, tableName, columnMap):
        createdColumns = []
        tableHeaders = self.getTableHeaders(tableName)
        try:
            for col in columnMap.keys():

                if col not in tableHeaders.keys() and 'NULL' not in columnMap[col]:

                    self.__cnx.cursor().execute("ALTER TABLE {} ADD COLUMN {}".format(tableName, col + ' ' + columnMap[col]))

                    createdColumns.append(columnMap[col])
        except Exception as e:
            print ("Columns Created: {}".format(', '.join(createdColumns)))
            print ("Exception occured when adding a new column to table {}. Exception: {}".format(tableName, e))
            print (traceback.format_exc())


    # Input for Snow Upsert. Upserts Data to a target table.
    # String, 
    # {headers:{col1:VARCHAR,...}, data:[{col1:val1, col2: val2,...}, ...]}
    # Tuple (targetCol, sourceCol)
    # matchCondidtions = {
                    #     'WHEN MATCHED': [
                    #                         {
                    #                         'condition' : 'AND |TBL1|.<Col_Name> != |TBL2|.<Col_Name>', 
                    #                         'action' : 'UPDATE ALL'
                    #                         },
                    #                         {
                    #                         'condition' : 'AND |TBL1|.<Col_Name>!= |TBL2|.<Col_Name>', 
                    #                         'action' : '<tbl1Col1> = |TBL2|.<tbl2Col>, <tbl1Col2> = 5'
                    #                         }
                    #                     ],
                    #     'WHEN NOT MATCHED':[
                    #                             {
                    #                             'condition' : '',
                    #                             'action' : 'INSERT ALL'
                    #                             },
                    #                             {               
            # must have value even if blank ->    'condition' : '',
                    #                             'action' : '(<tbl1Col1>, <tbl1Col2>) VALUES (|TBL2|.<tbl2Col1>, |TBL2|.<tbl2Col2>)'
                    #                             }
                    #                         ]
                    # }
    # Set autoCreateMissingColumns false to NOT create missing columns from source table onto target table. Default: false   
    def snowUpsert(self, targetTbl, source, joinExp, upsertConditions, autoCreateMissingColumns=False):
        
        print ('snowUpsert')

        joinExpression = ''
        conditionString = ''
        tempMergeTableName = self.__createTempTableName('Merge_TO_{}'.format(targetTbl))
        # Check all incoming parameters
        if not targetTbl or not source or not joinExp or not upsertConditions:
            raise ValueError('Provide all parameters.')

        if not self.snowTableCheck(targetTbl):
            raise Exception('Target table does not exsist. Create Target table first.')

        if type(joinExp) is not tuple:
            raise TypeError('Join parameter must be of type tuple.')

        if len(upsertConditions) == 0:
            raise ValueError("Provide at least one condition and associated update expression for a matched case. Provide at least one condition and insert expression for unmatched case.")

        # Get the table headers of the target table.
        headersMap = self.getTableHeaders(targetTbl)

        # Get records from source table
        data = source['data']

        # Check that headers and data are populated
        if type(data) is not list:
            raise TypeError("The records must be in a single array comprised of objects that are of header value map.")

        if len(headersMap) == 0:
            raise ValueError("There are no headers found in the target table.")

        # Check if additional columns need to be created in target table
        headerCompare = self.compareTableHeaders(headersMap, source['headers'])
        if headerCompare == False and autoCreateMissingColumns == True:
            self.addColumn(targetTbl, source['headers'])
        elif headerCompare == False:
            raise Exception("Columns in Target table {} are missing in source.".format(targetTbl))
            
        # Generate the join on Expression for merge. if one arg is passed will use that col for both tables. ex. foo.fookey = bar.barkey
        if len(joinExp) == 2:
            joinExpression = '{}.{} = {}.{}'.format(targetTbl, joinExp[0], tempMergeTableName, joinExp[1])
        elif len(joinExp) == 1:
            joinExpression = '{}.{} = {}.{}'.format(targetTbl, joinExp[0], tempMergeTableName, joinExp[0])
        else:
            raise ValueError("Join expressin must have a target column and source column to join on.")

        # Generate the conditions for matched and unmatched if the action on the condition is Update All.
        for condition in upsertConditions:
            conditionDetailList = upsertConditions[condition]

            for additional in conditionDetailList:
                if 'action' in additional and 'condition' in additional:
                    additional['condition'] = additional['condition'].replace('|TBL1|', targetTbl).replace('|TBL1|', tempMergeTableName)
                    additional['action'] = additional['action'].replace('|TBL1|', targetTbl).replace('|TBL1|', tempMergeTableName)
                    conditionString = self.__upsertHelper(condition, additional, tempMergeTableName, headersMap)
                else:
                    raise KeyError("action and condtion must be present in the match conditions.")

        # Create a temporary table and insert data
        self.snowCreateTable(tempMergeTableName, self.createColumnString(headersMap))
        try:
            self.insertRows(tempMergeTableName, data)
        except Exception as e:
            # if insert fails remove temp table and close connection
            print ("Insertion of records failed. Deleting temp table.")
            self.snowDropTable(tempMergeTableName)
            self.closeInterface()
            print (traceback.format_exc())
            raise Exception("Error occured while inserting rows. Connection terminated.")      

        try:
            self.__cnx.cursor().execute("MERGE INTO {} USING {} ON {} {}".format(targetTbl, tempMergeTableName, joinExpression, conditionString))
            self.snowDropTable(tempMergeTableName)
        except Exception as e:
            print ("An error occured when Merging tables. Exception: {}".format(e))
            print (traceback.format_exc())
            self.snowDropTable(tempMergeTableName)
            self.closeInterface()
            raise Exception("Error occured while inserting rows. Connection terminated.")
        print("Upsert Completed.")      


    def __upsertHelper(self,condition, detail, tempMergeTableName, headersMap):
        conditionString = ''

        if 'DELETE' in detail['action']:
            raise Exception('Cannot have a delete in upsert expression.')
        
        if detail['action'] == 'UPDATE ALL' and 'WHEN MATCHED' in condition:

            if detail['condition']:
                condition = condition + ' ' + detail['condition']
            
            # String format -> When Matched (and tbl.col3 = 5) then update set col1 = tempMergeTable.col1, col2...
            conditionString = conditionString + ' ' + condition + ' THEN UPDATE SET {}'.format(','.join('{colName} = {tempTable}.{colName}'.format(tempTable = tempMergeTableName, colName = col) for col in headersMap.keys()))
                
        elif detail['action'] == 'INSERT ALL' and 'WHEN NOT MATCHED' in condition:

            if detail['condition']:
                condition = condition + ' ' + detail['condition']

            # String format -> When not Matched (and tbl.col3 = 5) then insert (col1, col2, col3, ...) values (tempMergeTable.col1, tempMergeTable.col2, ...)
            conditionString = conditionString + ' ' + condition + ' THEN INSERT ' + '({})'.format(','.join(headersMap.keys())) + ' VALUES ({})'.format(','.join('{tempTable}.{colName}'.format(tempTable = tempMergeTableName, colName = col) for col in headersMap.keys()))

        elif 'WHEN MATCHED' in condition: 

            if detail['condition']:
                condition = condition + ' ' + detail['condition']

            conditionString = conditionString + condition + ' THEN UPDATE SET ' + detail['action']
        elif 'WHEN NOT MATCHED' in condition: 

            if detail['condition']:
                condition = condition + ' ' + detail['condition']

            conditionString = conditionString + condition + ' THEN INSERT ' + detail['action']

        return conditionString


    def snowTableCheck(self, tableName):
        try:
            self.__cnx.cursor().execute("SELECT * FROM {}".format(tableName))
            return True
        except Exception as e:
            return False

    
    def snowCreateTable(self, tableName, columnString):
        print('Creating Table. {}'.format(str(datetime.datetime.now())))
        if not tableName:
            raise ValueError("Enter a Valid table name.")
        
        if self.snowTableCheck(tableName):
            raise Exception('Table with the same name already exists.')

        if not columnString and type(columnString) is not str:
            raise TypeError("Enter a valid Column String. Use the createColumnString function.")

        try:
            self.__cnx.cursor().execute("CREATE OR REPLACE TABLE {tblName}({cols})".format(tblName = tableName, cols = columnString))
            self.__cnx.cursor().execute("GRANT SELECT ON TABLE {} TO ROLE LOOKER_ROLE".format(tableName))
            self.__cnx.cursor().execute("GRANT ALL PRIVILEGES ON TABLE {} TO ROLE INTEGRATIONS".format(tableName))
        except Exception as e:
            print ("Error in Creating a new table. \n{}".format(e))
            print (traceback.format_exc())
            self.closeInterface()
    
    
    # Creates a string with column name and type. ex: 'column1 VARCHAR, column2 INTEGER, column3 STRING'
    #takes in colMap {colName:coltype,...}
    #Optional order takes a column list [colName, colName2, ...]
    @staticmethod
    def createColumnString(colMap, order=None):
        if order==None:
            return ','.join(['{name} {cType}'.format(name = col, cType = colMap[col]) for col in colMap])
        
        return ','.join(['{name} {cType}'.format(name = col, cType = colMap[col]) for col in order])

    

    def snowDropTable(self, tableName):
        print('Dropping Table.')
        try:
            self.__cnx.cursor().execute("DROP TABLE IF EXISTS {}".format(tableName))
        except Exception as e:
            print ("An exception occured when dropping the table. Exception {}".format(e))
            self.closeInterface()

    def __rowInsertValidator(self, recordArray, headerMap):
        recordStr = ''
        for record in recordArray:
            recordStr = ''
            for head in headerMap.keys():
                val = None
                if head in record:
                    val = record[head]
                else:
                    continue
                
                if val == None or val == '':
                    recordStr = '{}NULL'.format(recordStr)
                elif isinstance(val, str) == False:
                    recordStr = '{}{}'.format(recordStr, str(val))
                elif val.find("$") >= 0:
                    val.removeInvalidChar(val)
                    val = "'{}'".format(val)
                else:
                    val = "$${}$$".format(val)
                    recordStr = '{}{}'.format(recordStr, val)

            yield recordStr


    # recordArray shoud be [{col1:val1,col2:val2,...},...]
    def insertRows(self, tableName, recordArray):
        singleExecuteRowLimit = 15000
        print ('Inserting Rows')
        if not tableName:
            raise ValueError("Table name was Null or blank")

        if not recordArray or type(recordArray) is not list or len(recordArray) == 0:
            raise ValueError("Error with record Array. Make sure type is list and has elements.")

        if not self.snowTableCheck(tableName):
            raise Exception("Table does not exist, Make sure table is created.")

        headersMap = self.getTableHeaders(tableName)

        executionStr = ''
        keyStr = '({})'.format(','.join(headersMap.keys()))

        rows = self.__rowInsertValidator(recordArray, headersMap)

        for i,row in enumerate(rows):
            
            if (i+1)%singleExecuteRowLimit == 0:
                try:
                    insertStr = "INSERT INTO {} {} VALUES {}".format(tableName, keyStr, executionStr)
                    self.__cnx.cursor().execute(insertStr)
                    print('\t Inserted Batch')
                except Exception as e:
                    print ("An error occured when inserting records. \n {}".format(e))
                    print (traceback.format_exc())
                    self.closeInterface()
                time.sleep(.05)
                executionStr = ''

            executionStr = ','.join([executionStr, '({})'.join(row)])

            
    def queryRecords(self, query):
        print("Querying Records")   

        cursor = self.__dictcnx

        try:
            cursor.execute(query)
        except Exception as e:
            print ("An error Occured when querying warehouse. Exception {}".format(e))
            print (traceback.format_exc())
            self.closeInterface()
        
        return cursor

def getDataType(data):
    if data == None or data == '':
        return 'VARCHAR'
    elif isinstance(data, int):
        return 'NUMBER'
    elif isinstance(data, float):
        return 'FLOAT'
    elif isinstance(data, datetime.date):
        return 'DATE'
    elif isinstance(data, datetime.time):
        return 'TIME'
    elif isinstance(data, datetime.datetime):
        return 'DATETIME'
    else:
        return 'VARCHAR'

def removeInvalidChar(data):
    strVal = data.replace("'","\\")
    strVal = strVal.replace('"','\\"')
    strVal = strVal.replace("\\", "\\\\")
    strVal = strVal.replace("\\b", "\\\\b")
    strVal = strVal.replace("\\f", "\\\\f")
    strVal = strVal.replace("\\n", "\\\\n")
    strVal = strVal.replace("\\r", "\\\\r")
    strVal = strVal.replace("\\t", "\\\\t")
    return strVal


        # TODO create a flag for rolling back temp table creations on error
        # TODO re-evaluate when to close connections. sometimes better to catch error and leave connection open. 
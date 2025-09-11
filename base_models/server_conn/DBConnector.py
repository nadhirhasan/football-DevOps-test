import mysql.connector

class DBConnector(object):

    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host, 
            user=user, 
            password=password, 
            database=database
        )
    

    def execute(self, query, args=None):
        """Runs write queries"""
        cursor = self.connection.cursor()
        cursor.execute(query, args or ())
        last_pk_id = cursor.lastrowid
        self.connection.commit()
        cursor.close()
        return last_pk_id
    
    
    def execute_many(self, query, args=None):
        """Runs multiple write queries"""
        cursor = self.connection.cursor()
        cursor.executemany(query, args or ())
        self.connection.commit()
        cursor.close()
    

    def execute_and_read(self, query, args=None):
        """Runs read queries and returns output"""
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, args or ())
        output = cursor.fetchall()
        cursor.close()
        return output

import pymysql

class DB:
    def __init__(self, host: str, port: int, user: str, password: str, database: str) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
    
    def connect(self) -> None: 
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4"
            )
            self.cursor = conn.cursor()
        except Exception as e:
            self.conn = None
            self.cursor = None
            raise e
        
class query(DB): # 查询类，支持基本查询和多表连接查询
    def __init__(self, host: str, port: int, user: str, password: str, database: str, table_name: str) -> None:
        super().__init__(host, port, user, password, database)
        self.table_name = table_name
        self.connect()

    def db_query(self, table: str, cols: str, where=None, ** kwargs) -> list: # 支持基本查询
        sql = f"SELECT {cols} FROM {table}"
        if where:
            sql += f" WHERE {where}"
              
        if order_by := kwargs.get("order_by"):
            sql += f" ORDER BY {order_by}"
            
        if limit := kwargs.get("limit"):
            sql += f" LIMIT {limit}"
        
        if self.cursor is not None:
            self.cursor.execute(sql)
            return list(self.cursor.fetchall())
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_join_query(self, tables: list, cols: str, on: str, where=None, ** kwargs) -> list: # 支持多表连接查询
        sql = f"SELECT {cols} FROM {' JOIN '.join(tables)} ON {on}"
        if where:
            sql += f" WHERE {where}"
        
        if order_by := kwargs.get("order_by"):
            sql += f" ORDER BY {order_by}"
            
        if limit := kwargs.get("limit"):
            sql += f" LIMIT {limit}"
        
        if self.cursor is not None:
            self.cursor.execute(sql)
            return list(self.cursor.fetchall())
        else:
            raise RuntimeError("Database connection is not established.")
        
class DML(DB): # 支持增删改操作
    def __init__(self, host: str, port: int, user: str, password: str, database: str, table_name: str) -> None:
        super().__init__(host, port, user, password, database)
        self.table_name = table_name
        self.connect()

    def db_insert(self, table: str, cols: list, values: list) -> None:
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(values))})"
        if self.cursor is not None:
            self.cursor.execute(sql, values)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_update(self, table: str, set_clause: str, where=None) -> None:
        sql = f"UPDATE {table} SET {set_clause}"
        if where:
            sql += f" WHERE {where}"
        
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_delete(self, table: str, where=None) -> None:
        sql = f"DELETE FROM {table}"
        if where:
            sql += f" WHERE {where}"
        
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
class DDL(DB): # 支持表结构和数据库管理操作，如建表、删表、修改表结构、建库、删库等
    def __init__(self, host: str, port: int, user: str, password: str, database: str) -> None:
        super().__init__(host, port, user, password, database)
        self.connect()

    def db_create_table(self, table: str, schema: str) -> None: # 支持表创建操作，schema参数为表结构定义字符串，如"id INT PRIMARY KEY, name VARCHAR(100), age INT"等
        sql = f"CREATE TABLE IF NOT EXISTS {table} ({schema})"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_drop_table(self, table: str) -> None: # 支持表删除操作
        sql = f"DROP TABLE IF EXISTS {table}"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_truncate_table(self, table: str) -> None: # 支持表数据清空操作
        sql = f"TRUNCATE TABLE {table}"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_rename_table(self, old_name: str, new_name: str) -> None: # 支持表重命名操作
        sql = f"RENAME TABLE {old_name} TO {new_name}"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_alter_table(self, table: str, alteration: str, **kwargs) -> None: # 支持表结构修改，增加列、删除列、修改列、重命名列等操作
        sql = f"ALTER TABLE {table} {alteration}"
        
        if add_col := kwargs.get("add_col"):
            sql += f" ADD COLUMN {add_col}"
            
        if drop_col := kwargs.get("drop_col"):
            sql += f" DROP COLUMN {drop_col}"
            
        if modify_col := kwargs.get("modify_col"):
            sql += f" MODIFY COLUMN {modify_col}"
            
        if rename_col := kwargs.get("rename_col"):
            sql += f" RENAME COLUMN {rename_col}"
        
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_create_database(self, database: str) -> None: # 支持数据库创建操作
        sql = f"CREATE DATABASE IF NOT EXISTS {database}"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_drop_database(self, database: str) -> None: # 支持数据库删除操作
        sql = f"DROP DATABASE IF EXISTS {database}"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
class DCL(DB): # 支持数据库连接管理操作，如连接建立、连接关闭、连接池管理等
    def __init__(self, host: str, port: int, user: str, password: str, database: str) -> None:
        super().__init__(host, port, user, password, database)
        self.connect()
    
    def db_grant_privileges(self, user: str, privileges: str, database: str = "*", table: str = "*") -> None: # 支持权限授予操作
        sql = f"GRANT {privileges} ON {database}.{table} TO '{user}'@'%'"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_revoke_privileges(self, user: str, privileges: str, database: str = "*", table: str = "*") -> None: # 支持权限撤销操作
        sql = f"REVOKE {privileges} ON {database}.{table} FROM '{user}'@'%'"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_create_user(self, user: str, password: str) -> None: # 支持用户创建操作
        sql = f"CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{password}'"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_drop_user(self, user: str) -> None: # 支持用户删除操作
        sql = f"DROP USER IF EXISTS '{user}'@'%'"
        if self.cursor is not None:
            self.cursor.execute(sql)
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
        
    def db_close(self) -> None: # 支持连接关闭操作
        if self.cursor is not None:
            self.cursor.connection.close()
            self.cursor = None
        else:
            raise RuntimeError("Database connection is not established.")
        
class TCL(DB): # 支持事务管理操作，如事务开始、事务提交、事务回滚等
    def __init__(self, host: str, port: int, user: str, password: str, database: str) -> None:
        super().__init__(host, port, user, password, database)
        self.connect()
    
    def db_begin_transaction(self) -> None: # 支持事务开始操作
        if self.cursor is not None:
            self.cursor.execute("START TRANSACTION")
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_commit_transaction(self) -> None: # 支持事务提交操作
        if self.cursor is not None:
            self.cursor.connection.commit()
        else:
            raise RuntimeError("Database connection is not established.")
    
    def db_rollback_transaction(self) -> None: # 支持事务回滚操作
        if self.cursor is not None:
            self.cursor.connection.rollback()
        else:
            raise RuntimeError("Database connection is not established.")
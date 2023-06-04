from database import column
from database import table
from database import database
import json
import os


class fileStream:
    def __init__(self, _db=None):
        self.db = _db

    def newDb(self, _db_name):
        if self.db != None:
            self.closeDb()
        self.db = database(_db_name)
        self.saveDb()
        print('创建成功，当前数据库为：', self.db.name, sep='')

    def openDb(self, _dir):
        cd = os.getcwd()
        directory = os.path.join(_dir)
        if os.path.exists(directory):
            self.db = database(_dir)
            filenames = []
            for filename in os.listdir(directory):
                if os.path.isfile(os.path.join(directory, filename)):
                    file_name_without_extension = os.path.splitext(filename)[0]
                    filenames.append(file_name_without_extension)
            self.db.table = {i: self.readTable(i) for i in filenames}
            print('读取成功，当前数据库为：', self.db.name, sep='')
        else:
            print('数据库不存在，读取失败！')
        pass

    def closeDb(self):
        self.saveDb()
        self.db = None

    def readTable(self, _table):
        os.chdir('./'+self.db.name)
        fileName = _table + '.json'
        with open(fileName) as f:
            dic = json.load(f)
        os.chdir('..')
        tb = table(_table)
        tb.name = dic['name']
        tb.column = [column(
            i['name'],
            i['type'],
            i['maxSize'],
            _pk=i['pk'],
            _fk=i['fk']
        )
            for i in dic['column']]
        tb.data = dic['data']
        return tb
    
    def delTable(self, _table):
        os.chdir('./'+self.db.name)
        fileName = _table + '.json'
        os.remove(fileName)
        os.chdir('..')

    def saveDb(self):
        if self.db != None:
            directory = self.db.name
            directory = os.path.join(directory)
            if not os.path.exists(directory):
                os.mkdir(directory)
            for i in self.db.table.values():
                self.writeTable(i)

    def toDict(self, _col):
        return [{'name': i.name, 'type': i.type, 'maxSize': i.maxSize, 'pk': i.pk, 'fk': i.fk} for i in _col]

    def writeTable(self, _table):
        os.chdir('./'+self.db.name)
        dic = {
            'name': _table.name,
            'column': self.toDict(_table.column),
            'data': _table.data
        }
        fileName = _table.name + '.json'
        with open(fileName, 'w') as f:
            json.dump(dic, f)
        os.chdir('..')

    def decodeSQL(self, sql):
        dec = decoder(self.db)
        dec.decode(sql)
        self.saveDb()

class decoder:
    def __init__(self, _database):
        self.database = _database
    
    def decode(self, sql):
        sql = sql.strip().lower()  # 将SQL语句转换为小写并去除首尾空格
        if sql.startswith("create table"):
            self.parse_create_table(sql)
        elif sql.startswith("drop table"):
            self.parse_drop_table(sql)
        elif sql.startswith("select"):
            self.parse_select(sql)
        elif sql.startswith("insert into"):
            self.parse_insert(sql)
        elif sql.startswith("delete"):
            self.parse_delete(sql)
        elif sql.startswith("update"):
            self.parse_update(sql)
        else:
            print("SQL语句无效！")
        
    def parse_create_table(self, sql):
        # 解析CREATE TABLE语句
        table_name = sql[12:].strip().split('(')[0].strip()
        idx = sql.index('(')

        lists = [column.strip() for column in sql[idx+1:-1].split(',')]
        col_set = []
        for col in lists:
            tmplst = [i.strip() for i in col.split()]
            col_name = tmplst[0]
            col_type = tmplst[1]
            col_maxSize = 5
            if col_type.startswith('varchar'):
                i = col_type.index('(')
                j = col_type.index(')')
                col_maxSize = eval(col_type[i+1:j])
                col_type = col_type[:i]
            col_pk = False
            if 'primary key' in col:
                col_pk = True
            col_fk = {}
            if 'foreign key' in col:
                i = tmplst.index('references')
                kv = tmplst[i+1]
                j = kv.index('(')
                k = kv.index(')')
                kv_k = kv[:j]
                kv_v = kv[j+1:k]
                col_fk[kv_k] = kv_v
            col_set.append(column(col_name, col_type, _maxSize=col_maxSize, _pk=col_pk, _fk=col_fk))
        if self.database.createTable(table_name, col_set):
            print("已创建表:", table_name)
        else:
            print("该表已存在")

    def parse_drop_table(self, sql):
        # 解析DROP TABLE语句
        table_name = sql[10:].strip()
        if self.database.dropTable(table_name):
            f = fileStream(self.database)
            f.delTable(table_name)
            print("已删除表:", table_name)
        else:
            print("该表不存在")

    def parse_select(self, sql):
        # 解析SELECT语句
        table_name = sql.split('from')[1].strip()
        if 'where' in table_name:
            table_name = table_name.split('where')[0].strip()
        tb = self.database.table[table_name]
        columns = sql.split('select')[1].split('from')[0].strip()
        if columns == '*':
            columns = []
        else:
            column_names = [col.strip() for col in columns.split(',')]
            columns = [i for i in tb.column if i.name in column_names]
        cond = None
        if 'where' in sql:
            cond = []
            condstr = sql.split('where')[1].strip()
            if '<' in condstr:
                cond.append([condstr.split('<')[0].strip(), condstr.split('<')[1].strip(), '<'])
            elif '<=' in condstr:
                cond.append([condstr.split('<=')[0].strip(), condstr.split('<=')[1].strip(), '<='])
            elif '=' in condstr:
                cond.append([condstr.split('=')[0].strip(), condstr.split('=')[1].strip(), '='])
            elif '!=' in condstr:
                cond.append([condstr.split('!=')[0].strip(), condstr.split('!=')[1].strip(), '!='])
            elif '>=' in condstr:
                cond.append([condstr.split('>=')[0].strip(), condstr.split('>=')[1].strip(), '>='])
            elif '>' in condstr:
                cond.append([condstr.split('>')[0].strip(), condstr.split('>')[1].strip(), '>'])
        result = tb.selectFrom(columns, tb.where(cond) if cond != None else None)
        print("选择结果:")
        for i in result:
            print(i)

    def parse_insert(self, sql):
        # 解析INSERT INTO语句
        table_name = sql.split('into')[1].split('values')[0].strip()
        tb = self.database.table[table_name]
        values = [i.strip() for i in sql.split('(')[1].split(')')[0].strip().split(',')]
        for index in range(len(values)):
            if values[index].startswith("\'") and values[index].endswith("\'") or values[index].startswith('\"') and values[index].endswith('\"'):
                values[index] = values[index][1:-1]
        data = [value.strip() for value in values]
        tb.insertData(data)
        print("已将数据添加到:", table_name)

    def parse_delete(self, sql):
        # 解析DELETE语句
        table_name = sql.split('from')[1].strip()
        if 'where' in table_name:
            table_name = table_name.split('where')[0].strip()
        tb = self.database.table[table_name]
        cond = None
        if 'where' in sql:
            cond = []
            condstr = sql.split('where')[1].strip()
            if '<' in condstr:
                cond.append([condstr.split('<')[0].strip(), condstr.split('<')[1].strip(), '<'])
            elif '<=' in condstr:
                cond.append([condstr.split('<=')[0].strip(), condstr.split('<=')[1].strip(), '<='])
            elif '=' in condstr:
                cond.append([condstr.split('=')[0].strip(), condstr.split('=')[1].strip(), '='])
            elif '!=' in condstr:
                cond.append([condstr.split('!=')[0].strip(), condstr.split('!=')[1].strip(), '!='])
            elif '>=' in condstr:
                cond.append([condstr.split('>=')[0].strip(), condstr.split('>=')[1].strip(), '>='])
            elif '>' in condstr:
                cond.append([condstr.split('>')[0].strip(), condstr.split('>')[1].strip(), '>'])
        tb.deleteFrom(tb.where(cond) if cond != None else None)
        print("已将数据从表中删除:", table_name)

    def parse_update(self, sql):
        # 解析UPDATE语句
        table_name = sql.split('update')[1].split('set')[0].strip()
        tb = self.database.table[table_name]
        column_names = sql.split('set')[1].split('=')[0].strip()
        value = sql.split('=')[1].split('where')[0].strip() if 'where' in sql else sql.split('=')[1].strip()
        cond = None
        if 'where' in sql:
            cond = []
            condstr = sql.split('where')[1].strip()
            if '<' in condstr:
                cond.append([condstr.split('<')[0].strip(), condstr.split('<')[1].strip(), '<'])
            elif '<=' in condstr:
                cond.append([condstr.split('<=')[0].strip(), condstr.split('<=')[1].strip(), '<='])
            elif '=' in condstr:
                cond.append([condstr.split('=')[0].strip(), condstr.split('=')[1].strip(), '='])
            elif '!=' in condstr:
                cond.append([condstr.split('!=')[0].strip(), condstr.split('!=')[1].strip(), '!='])
            elif '>=' in condstr:
                cond.append([condstr.split('>=')[0].strip(), condstr.split('>=')[1].strip(), '>='])
            elif '>' in condstr:
                cond.append([condstr.split('>')[0].strip(), condstr.split('>')[1].strip(), '>'])
        tb.update(column_names, value, tb.where(cond) if cond != None else None)
        print("数据已更新:", table_name)
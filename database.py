class column:
    def __init__(self, _name, _type, _maxSize=5, _pk=False, _fk={}):
        self.name = _name
        self.type = _type
        self.maxSize = _maxSize
        self.pk = _pk
        self.fk = _fk


class table:
    def __init__(self, _name):
        self.name = _name
        self.column = list()
        self.data = list()

    def insertColumn(self, _column):
        self.column.append(_column)

    def insertData(self, _data):
        cnt = 0
        tmpdict = {}
        for i in self.column:
            columnName = i.name
            tmpdict[columnName] = _data[cnt]
            cnt += 1
        self.data.append(tmpdict)

    def where(self, _cond):
        mask = [False for x in self.data]
        for i in _cond:
            cnt = 0
            for j in self.data:
                if i[2] == '<':
                    if j[i[0]] < i[1]:
                        mask[cnt] = True
                elif i[2] == '<=':
                    if j[i[0]] <= i[1]:
                        mask[cnt] = True
                elif i[2] == '=':
                    if j[i[0]] == i[1]:
                        mask[cnt] = True
                elif i[2] == '!=':
                    if j[i[0]] != i[1]:
                        mask[cnt] = True
                elif i[2] == '>=':
                    if j[i[0]] >= i[1]:
                        mask[cnt] = True
                elif i[2] == '>':
                    if j[i[0]] > i[1]:
                        mask[cnt] = True
                cnt += 1
        condition_index = [index for index,
                           element in enumerate(mask) if element == True]
        return condition_index

    def deleteFrom(self, _index=None):
        if _index != None:
            self.data = list(
                filter(lambda x: self.data.index(x) not in _index, self.data))
        else:
            self.data = []

    def selectFrom(self, _column=[], _index=None):
        if _index != None:
            table_copy = list(
                filter(lambda x: self.data.index(x) in _index, self.data))
        else:
            table_copy = self.data
        if _column == []:
            table_copy_keys = [i.name for i in self.column]
            table_copy_values = [list(x.values()) for x in table_copy]
        else:
            table_copy_keys = [i.name for i in [
                x for x in self.column if x in _column]]
            table_copy_values = [list({key: value for key, value in list(
                x.items()) if key in table_copy_keys}.values()) for x in table_copy]
        table_copy_values.insert(0, table_copy_keys)
        return table_copy_values

    def update(self, _column, _value, _index=None):
        if _index == None:
            for i in self.data:
                i[_column] = _value
        else:
            for i in _index:
                self.data[i][_column] = _value


class database:
    def __init__(self, _name):
        self.name = _name
        self.table = dict()

    def createTable(self, _name, _columnSet):
        if _name in self.table.keys():
            return 0
        else:
            self.table[_name] = table(_name)
            for i in _columnSet:
                self.table[_name].insertColumn(i)
            return 1

    def dropTable(self, _name):
        if _name not in self.table.keys():
            return 0
        else:
            del self.table[_name]
            return 1

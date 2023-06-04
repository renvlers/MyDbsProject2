import executor
import os
import sys

def clear_console():
    if sys.platform.startswith('win'):
        os.system('cls')
    else:
        os.system('clear')

if __name__ == '__main__':
    fs = executor.fileStream()
    while True:
        clear_console()
        print('---------------欢迎使用SQL执行器---------------')
        print('当前未打开数据库' if fs.db == None else f'当前已打开数据库：{fs.db.name}')
        print('-----------------------------------------------')
        print('请选择您需要进行的操作：')
        print('1. 新建数据库', '2. 打开数据库', '3. 执行SQL语句', '4. 关闭数据库', '5. 退出程序', sep='\n')
        ipt = input()
        if ipt == '1':
            clear_console()
            print('请输入数据库名称：')
            dbName = input()
            fs.newDb(dbName)
            input('请按回车键返回……')
        elif ipt == '2':
            clear_console()
            if fs.db != None:
                fs.closeDb()
            print('请输入数据库名称：')
            dbName = input()
            fs.openDb(dbName)
            input('请按回车键返回……')
        elif ipt == '3':
            clear_console()
            if fs.db == None:
                print('未打开数据库！')
            else:
                print('请输入SQL语句，以\"\\\"结尾：')
                lines = []
                while True:
                    line = input()
                    line = line.rstrip()  # 去除末尾的空格和换行符
                    if line.endswith('\\'):
                        line = line[:-1]  # 去除最后一个字符'\\'
                        lines.append(line)
                        break
                    else:
                        lines.append(line)
                # 将多行字符串连接起来
                input_string = ' '.join(lines)
                sql = [i.strip() for i in input_string.split(';') if i.strip() != '']
                for i in sql:
                    fs.decodeSQL(i)
            input('请按回车键返回……')
        elif ipt == '4':
            clear_console()
            if fs.db == None:
                print('当前未打开数据库')
            else:
                fs.closeDb()
                print("已关闭数据库")
            input('请按回车键返回……')
        elif ipt == '5':
            clear_console()
            if fs.db != None:
                fs.saveDb()
            exit()
        else:
            clear_console()
            print('输入无效，请重新输入！')
            input('请按回车键返回……')

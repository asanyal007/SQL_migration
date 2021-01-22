import pandas as pd
import re
import difflib
import function_convert
import keyword_maps
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Parenthesis, Comparison, Where
import ntpath

class covert_function:
    def __init__(self, sql_as_string):
        self.sql_as_string = sql_as_string

    def get_func(self):
        '''Returns the list of all present functins within the given source SQL'''
        open_br = 0
        closed_br = 0
        list_of_function = []
        list_of_fun_name_new = []
        i = 0
        list_of_func_name = re.findall("[aA-zZ_]+\(+", self.sql_as_string)
        list_of_func_name = {a.split("(")[0]: len(a.split("(")[0]) for a in list_of_func_name}
        for s in list_of_func_name.keys():
            p = re.compile(r"\b{}\(".format(s))
            for a in p.finditer(self.sql_as_string):
                start_pos = a.start()
                for c in self.sql_as_string[start_pos:len(self.sql_as_string)]:
                    i = i + 1
                    if "(" in c:
                        open_br = open_br + 1
                    elif ")" in c:
                        closed_br = closed_br + 1
                    list_of_function.append(c)
                    if open_br == closed_br and closed_br > 0 and open_br > 0:
                        if not 'WHEN' in ''.join(list_of_function).upper():
                            list_of_fun_name_new.append(''.join(list_of_function))
                        open_br = 0
                        closed_br = 0
                        list_of_function = []
                        break
        return list_of_fun_name_new

    @staticmethod
    def create_map(all_functions, dict_transformed):
        ''' Return a dataframe of Source function and target function'''
        print("all functions: ", all_functions)
        for matches in all_functions:
            '''balances the opening and closing brackets'''
            open_br = matches.count("(")
            closing_br = matches.count(")")
            diff_count = closing_br - open_br
            if diff_count>0:
                for i in range(diff_count):
                    matches = matches.replace(")","")
            elif diff_count<0:
                for i in range(diff_count):
                    matches = matches.replace("(","")
            try:
                dict_transformed[matches] = function_convert.map_function(matches)
            except:
                dict_transformed["-"] = function_convert.map_function(matches)

        df_converted_func = pd.DataFrame.from_dict(dict_transformed, orient='index').reset_index()
        df_converted_func = df_converted_func.rename(columns={'index': 'SQL_Functions', 0: 'Converted_Functions'},
                                                     inplace=False)

        return df_converted_func

    @staticmethod
    def convert(sql_as_string,file_path, ROOT_DIR):
        '''Converts the functions based on mapping dataframe & reurns the converted SQL'''
        sql_file = sql_as_string
        file_name = ntpath.basename(file_path).split('.')[0]
        map = pd.read_csv(ROOT_DIR+"/func_maps/{}.csv".format(file_name))
        sql_as_string = sql_file
        map["complexity"] = map["SQL_Functions"].str.count("\(")
        map.sort_values(by=['complexity'], inplace=True, ascending=False)

        for s in map['SQL_Functions']:
            replace_str = map[map['SQL_Functions'] == s]['Converted_Functions'].iloc[0]

            if replace_str != "Not Available":
                sql_as_string = re.sub(r"\b"+re.escape('{}'.format(s)), replace_str, sql_as_string)
        return sql_as_string

    @staticmethod
    def diff(new_sql_as_string,old_sql_as_string,diff_file_path,file_name):
        '''Creates git like difference between source sql and target sql'''
        f = open(diff_file_path +file_name + "_diff.sql", "w+")
        for line in difflib.unified_diff(old_sql_as_string.strip().splitlines(), new_sql_as_string.strip().splitlines(),
                                         fromfile='old_file', tofile='sql_as_string', lineterm=''):
            f.write("\n"+line)
            f.truncate()
        f.close()

    @staticmethod
    def save_file(sql_as_string,out_file_path,file_name):
        '''Saves the converted SQL file'''
        f = open(out_file_path+file_name+"_BigQ.sql", "w+")
        f.write(sql_as_string)
        f.truncate()
        f.close()

    @staticmethod
    def convert_tables(sql_as_string, map):
        '''Converts table names'''
        for key, val in map.items():
            sql_as_string = sql_as_string.replace("cs_supp.","") # change the project name based on your use case
            sql_as_string = re.sub(r'\b{}\b'.format(key.lower()),val, sql_as_string)
        return sql_as_string

    @staticmethod
    def  convert_reference(sql):
        regxp_alias = "[aA-zA_]+[(']+\w.*?\)+[ ]as[ ][aA-zZ]+|[aA-zA_]+[(']+\w.*?\)+[ ]AS[ ][aA-zZ]+"
        selections = re.findall(regxp_alias, sql)
        map ={}
        ''' Creating map for alias'''
        for s in selections:
            print("splitting: ", s)
            column = re.split('\)[ ]+as[ ]+|\)[ ]+AS[ ][^A-Z]+', s)[0]
            alias = re.split('\)[ ]+as[ ]+|\)[ ]+AS[ ][^A-Z]+', s)[1]
            print("col : ",column,"and alias:",alias)
            open_br = column.count("(")
            closed_br = column.count(")")
            if open_br>closed_br:
                column = column+")"
            map[alias] = column

        ''' Replacing the alias with function'''
        function_regxp = "[(']+\w.*?\) "
        all_parenthesis = re.findall(function_regxp, sql)
        conversion_map = {}
        for p in all_parenthesis:
            for k,v in map.items():
                if k in p:
                    converted = re.sub(r'[^.]{}\b'.format(k), v, p)
                    conversion_map[p] = converted


    @staticmethod
    def re_try(sql_as_string, func, file_name):
        '''Retries to convert if found syntax error in API check'''
        print("retrying conversion: ",func)
        file_name = file_name.split(r"/")[-1].split('.')[0]
        map = pd.read_csv(r"venv\\Func_Dict\\{}.csv".format(file_name))

        replace_str = map[map['SQL_Functions'] == func]['Converted_Functions'].iloc[0]
        print("Changing {} with {}".format(func, replace_str))
        if replace_str != "Not Available":
            sql_as_string = sql_as_string.replace('{}'.format(func), replace_str)
        return sql_as_string


class preProcess:
    def __init__(self, sql_as_string):
        self.sql_as_string = sql_as_string

    def doublecolon_to_standard_cast(self):
        '''Converts :: type cast to target type cast'''
        keywords_map = keyword_maps.keywords_map
        new_ptr = ['\s*([a-zA-Z_]\w*[(](\s*[a-zA-Z_]\w*[(]|[^()]+[)]|[)])::[a-z_]+)',
                   '[a-zA-Z_.][^ (\n)]*::.[a-zA-Z]+\w+']
        new_match = []
        for pat in new_ptr:
            new_match.extend(re.findall(pat, self.sql_as_string))
        all_list = []
        for n in new_match:
            if len(n) == 2:
                all_list.append(n[0])
            else:
                all_list.append(n)
        final = {}
        for each in all_list:
            if '(' in each:
                x = function_convert.map_function(re.findall('[^::]+', each)[0])
            else:
                x = re.findall('[^::]+', each)[0]
            if re.findall('[^::]+', each)[1] in keywords_map.keys():
                y = keywords_map[re.findall('[^::]+', each)[1]]
            else:
                y = re.findall('[^::]+', each)[1]

            final[each] = "CAST({} AS {})".format(x, y)
        for k, v in final.items():
            self.sql_as_string = self.sql_as_string.replace(k, v)
        return self.sql_as_string, final

    @staticmethod
    def regex_replace(str_sql, regex):
        regex_map = {}
        for k, v in regex.items():
            all_finds = re.findall(k, str_sql)
            for f in all_finds:
                regex_map[f] = f.replace(v[0], v[1])
        df_regex_map = pd.DataFrame(list(regex_map.items()), columns=['SQL_Functions', 'Converted_Functions'])
        return df_regex_map



class handleUnion:
    def __init__(self, sql_as_string, client, job_config, project_id):
        self.sql_as_string = sql_as_string
        self.client = client
        self.job_config = job_config
        self.project_id = project_id
    def run_script(self, sql_as_string):
        query_job = self.client.query(("{}".format(sql_as_string)), job_config= self.job_config)  # Make an API request.
        return query_job

    def extract_table_identifiers(self, token_stream, list_of_columns):
        for item in token_stream:
            if isinstance(item, Identifier) or isinstance(item, Parenthesis) and 'SELECT' in item.value.upper():
                print("recurse")
                return list_of_columns
            elif isinstance(item, Where):
                self.extract_table_identifiers(item.tokens)
            elif isinstance(item, IdentifierList):
                for ident in item.get_identifiers():
                    try:
                        alias = ident.get_alias()
                        real_name = str(ident).split(' as ')[0]
                        # print("real name: {} alias: {}".format(real_name,alias))
                        list_of_columns.append([real_name, alias])
                    except AttributeError:
                        continue

    def process_union(self):
        parsed = sqlparse.parse(self.sql_as_string)[0]
        for tok in parsed.tokens:
            if isinstance(tok, Identifier) and 'select' in tok.value.lower():
                sub_select_with_union = tok

        b = ""
        closing_br = 0
        open_br = 0
        for a in sub_select_with_union.tokens:
            open_br = open_br + a.value.count("(")
            closing_br = closing_br + a.value.count(")")

            b = b + a.value

            if open_br == closing_br and (open_br > 0 and closing_br > 0):
                # print(b)
                break

        new_union_str = b[1:len(b) - 1]

        list_of_unions = new_union_str.split("UNION ALL")

        # create temp table
        sql = '''CREATE or REPLACE VIEW `{}.temp.new`
        OPTIONS(
            expiration_timestamp=TIMESTAMP_ADD(
                CURRENT_TIMESTAMP(), INTERVAL 48 HOUR),
            friendly_name="new_view",
            description="a view that expires in 2 days",
            labels=[("org_unit", "development")]
        )
        AS {} LIMIT 1'''

        get_view_sql = '''SELECT column_name,data_type FROM temp.INFORMATION_SCHEMA.COLUMNS
                            where table_name = 'new' ;'''
        i = 0

        dict_rows = []
        for un in list_of_unions:
            list_of_columns = []

            if i == 0:
                pass
            else:
                print("---------------------- {} ----------------------------".format(i))
                job = self.run_script(sql.format(self.project_id, un))
                job.result()
                jb = self.run_script(get_view_sql)

                cols = self.extract_table_identifiers(sqlparse.parse(un)[0].tokens, list_of_columns)
                g = 0
                f = 0
                for a in jb.result():
                    cols[g].append(list(a)[1])
                    print(cols[g], g)

                    g = g + 1
                dict_rows.append(cols)
            i = i + 1

        df_columns = pd.DataFrame(dict_rows)
        null_list = []
        not_null_list = []
        list_of_unions = sub_select_with_union.value.split("UNION ALL")
        for i in range(df_columns.shape[1]):
            # print(i)
            null_list = []
            not_null_list = []
            for s in df_columns[i]:
                if re.search(r'\bnull\b', s[0]) and 'INT64' in s[2]:
                    null_list.append(s)
                elif not re.search(r'\bnull\b', s[0]) and 'INT64' not in s[2]:
                    not_null_list.append(s[2])
            if null_list and not_null_list:
                for n in null_list:
                    g = 0
                    for a in list_of_unions:
                        source = "{}\s+as\s+{}".format(n[0], n[1])
                        target = "CAST({} AS {}) as {}".format(n[0], list(set(not_null_list))[0], n[1])
                        # print(source)
                        list_of_unions[g] = re.sub(source, target, a)
                        # print(list_of_unions[g])
                        g = g + 1
        SQL_STRING = ""
        for tok in parsed.tokens:
            if isinstance(tok, Identifier) and 'select' in tok.value.lower():
                # print(' UNION ALL '.join(list_of_unions))
                SQL_STRING = SQL_STRING + ' UNION ALL '.join(list_of_unions)
            else:
                # print(tok)
                SQL_STRING = SQL_STRING + tok.value

        return SQL_STRING

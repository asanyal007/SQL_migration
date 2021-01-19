import pandas as pd
from os import listdir
from os.path import isfile, join
import keyword_maps
import function_convert
import convert_sql
import API_Check
import logging
logging.basicConfig(filename='app.log', level=logging.INFO)
from tkinter import *
import configparser
import pandas as pd


parser = configparser.ConfigParser()
parser.read('conf/ETLConfig.ini')

gcp_project_id = parser['GCP']['PROJECT_ID']

json_file = parser['GCP']['OENVIRON']



# importing askopenfile function
# from class filedialog
'''from tkinter.filedialog import askopenfile

root = Tk()
root.geometry('200x100')
file = askopenfile(filetypes=[('SQL Files', '*.sql')])
'''

file_path = 'C:/Workspace/SQL_To_BigQ/Objects_Complexity/Complex_SQL/'
out_file_path = r"C:\\Workspace\\SQL_To_BigQ\\Objects_Complexity\\Output_Complex\\"
diff_file_path = r"C:\\Workspace\\SQL_To_BigQ\\Objects_Complexity\\diffs\\Complex_SQL\\"
#onlyfiles = [file_path+f for f in listdir(file_path) if isfile(join(file_path, f))]


main_ptrn = "[aA-zA_]+[(']+\w.*?\)+"
fallback_ptrn = "\s*([a-zA-Z_]\w*[(](\s*[a-zA-Z_]\w*[(]|[^()]+)[)])"
ptr = "[^aA-zZ][^0-9 \W]+"



def run(file):
    #file = str(file.name)
    print("Reading {}".format(file))
    sql_file = open(file)
    sql_as_string = sql_file.read()



    '''replace double colon'''
    pre_proc = convert_sql.preProcess(sql_as_string)
    sql_as_string, dict_transaformed = pre_proc.doublecolon_to_standard_cast()

    df_regex_map = pre_proc.regex_replace(sql_as_string, keyword_maps.regex_map)


    file_name = file.split("\\")[-1].split('.')[0]
    ''' generaing list of functions in SQL'''
    cov_sql = convert_sql.covert_function(sql_as_string)
    all_functions = cov_sql.get_func()
    ''' create list of functions conversons to be made'''
    df_converted_func = cov_sql.create_map(all_functions, dict_transaformed)


    gcp_client = API_Check.gcpClient(gcp_project_id, json_file , dryRun=False, use_query_cache=False)
    client, jobconfig = gcp_client.api_config()
    gcp_api = API_Check.gcpAPIChecks(client, jobconfig)
    ''' Covert tables'''
    sql_as_string= cov_sql.convert_tables(sql_as_string, gcp_api.get_schema())
    ''' Direct conversions'''
    df_direct_conv = function_convert.map_direct(keyword_maps.direct_conversion)
    new_df = pd.concat([df_converted_func, df_direct_conv, df_regex_map ]).reset_index()
    new_df.to_csv(r"func_maps\\{}.csv".format(file_name))



    ''' Creating converted SQL files based on the list'''
    new_sql_as_string = cov_sql.convert(sql_as_string,file)



    ''' convert ref'''
    #new_sql_as_string = convert_sql.convert_reference(new_sql_as_string)

    '''saving the convertedfile'''
    cov_sql.save_file(new_sql_as_string,out_file_path, file_name)

    #print(API_Check.API_check(new_sql_as_string))
    '''Checking with API'''

    file_log = open("log/run_log/{}.log".format(file_name), "w+")

    gcp_client = API_Check.gcpClient(gcp_project_id, json_file, dryRun=True, use_query_cache=False)
    client, jobconfig = gcp_client.api_config()
    gcp_api = API_Check.gcpAPIChecks(client, jobconfig)

    # get error report
    result, err = gcp_api.API_check(new_sql_as_string)
    if isinstance(result, list):
        errors = result[0]['message']
        if "argument types:" in errors:
            print(file_name +": "+ str(errors))
            logging.error(file +": "+ str(errors))
            for er in str(err):
                file_log.writelines(er)

        else:
            print(file_name +": "+"There is no syntax errors")
            logging.info(file+ ": There is no syntax errors")
            # get stats
            job_detail = gcp_api.get_stats(new_sql_as_string)
            if job_detail:
                val = job_detail.total_bytes_processed
            else:
                val = 0
            logging.info(file+ ": Total bytes processed {}".format(val))
            file_log.write(file+ ": There is no syntax errors")
    file_log.close()
    ''' Creating diff'''
    cov_sql.diff(new_sql_as_string,sql_as_string,diff_file_path, file_name)

    cov_sql_final = convert_sql.covert_function(new_sql_as_string)
    final_function_list = cov_sql_final.get_func()


    function_log = open("log/function_log/success/function_{}.log".format(file_name), "w+")
    function_err_log = open("log/function_log/error/function_error_{}.log".format(file_name), "w+")

    gcp_client = API_Check.gcpClient(gcp_project_id, json_file, dryRun=True, use_query_cache=False)
    client, jobconfig = gcp_client.api_config()
    gcp_api = API_Check.gcpAPIChecks(client, jobconfig)
    status = []
    for f in list(set(final_function_list)):
        result, err = gcp_api.API_check("select {} ".format(f))
        if isinstance(result, list):
            errors = result[0]['message']
            status.append("error")
            if "Function not found:" in errors:
                print(f + ": " + str(errors))
                function_err_log.write(f + ": " + str(errors)+"\n")
                new_sql_as_string = cov_sql.re_try(sql_as_string, f, file_name)
            else:
                status.append("No error")
                print(f + ": " + "There is no syntax errors")
                function_log.write(f + ": " + "There is no syntax errors\n")
    function_log.close()
    function_err_log.close()

    # Post_process

    gcp_client = API_Check.gcpClient(gcp_project_id, json_file, False, False)
    client, jobconfig = gcp_client.api_config()

    postproc = convert_sql.handleUnion(new_sql_as_string, client=client, job_config=jobconfig, project_id='onyx-sequencer-297412')

    new_sql_as_string = postproc.process_union()

    cov_sql.save_file(new_sql_as_string,out_file_path, file_name)

    return list(set(status))


if __name__ == '__main__':
    import ntpath
    liat_dir1 = []
    liat_dir2 = []
    files_status_dict = {}

    i=0
    for k, file_path in parser['SOURCE'].items():
        file_name = ntpath.basename(file_path).split('.')[0]
        file_name = ntpath.basename(file_path).split('.')[0]
        files_status_dict['File_Name'] = file_name
        files_status_dict['Status'] = "In Progress"
        liat_dir1.append(files_status_dict)

    df_stat = pd.DataFrame(liat_dir1)
    df_stat.to_csv("Run_stat.csv", index=False)

    for k, file_path in parser['SOURCE'].items():
        file_name = ntpath.basename(file_path).split('.')[0]
        files_status_dict['File_Name'] = file_name
        files_status_dict['Status'] = "In Progress"
        status = run(file_path)
        if "Error" in status:
            files_status_dict['Status'] = "There were Errors"
            files_status_dict['Error_path'] = "log/function_log/error/function_error_{}.log".format(file_name)
        else:
            files_status_dict['Status'] = "Completed"
        i+=1
        liat_dir2.append(files_status_dict)
        df_stat= pd.DataFrame(liat_dir2)
        df_stat.to_csv("Run_stat.csv", index=False)








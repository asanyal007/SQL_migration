from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import ClientError


class gcpClient:
    def __init__(self, project_id, credentials, dryRun, use_query_cache):
        self.project_id = project_id
        self.credentials_json = credentials
        self.dryRun = dryRun
        self.use_query_cache = use_query_cache

    def api_config(self):
        credentials = service_account.Credentials.from_service_account_file(self.credentials_json)
        client = bigquery.Client(credentials=credentials, project=self.project_id)
        job_config = bigquery.QueryJobConfig(dry_run=self.dryRun, use_query_cache=self.use_query_cache, autodetect=True)
        return client, job_config

class gcpAPIChecks:
    def __init__(self, client, job_config):
        self.client = client
        self.job_config = job_config

    def API_check(self, str_sql):
        try:
            query_job = self.client.query(str_sql, job_config=self.job_config, retry=None)
            results = query_job.result()  # Wait for the job to complete.
            return results
        except ClientError as err:
            error_dic = err.__dict__['_errors']
            return error_dic,err


    def get_stats(self, sql_str):
        # Start the query, passing in the extra configuration.
        try:
            query_job = self.client.query(("{}".format(sql_str)),job_config=self.job_config,)  # Make an API request.
            return query_job
        except:
            return False

    def get_schema(self):
        sql_str = '''SELECT concat(table_schema,".",table_name) as tablename,concat("`",table_catalog,".",table_schema,".",table_name,"`") as schema   
        FROM {}.INFORMATION_SCHEMA.TABLES'''
        datasets = list(self.client.list_datasets())
        # Start the query, passing in the extra configuration.
        dict_result = {}
        try:
            for dataset in datasets:
                sql_str_new = sql_str.format(dataset.dataset_id)
                query_job = self.client.query(("{}".format(sql_str_new)), job_config= self.job_config, )  # Make an API request.
                for a in query_job.result():
                    dict_result[a[0]] = a[1]
            return dict_result
        except:
            return False

    def final_check(self, function):
        sql_str = '''SELECT {}'''.format(function)
        # Start the query, passing in the extra configuration.
        dict_result = {}
        try:
            query_job = self.client.query(("{}".format(sql_str)), job_config= self.job_config, )  # Make an API request.
            for a in query_job.result():
                # print(a[0].lower(), a[1])
                dict_result[a[0].lower()] = a[1]
            return dict_result
        except:
            return False
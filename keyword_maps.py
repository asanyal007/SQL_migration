keywords_map = {'DATEADD(1_, 2_, 3_)' : 'date_add(3_ ,INTERVAL 2_ 1_)',
                'date_part(1_, 2_)' : 'EXTRACT(1_ FROM 2_)',
                'datediff(1_, 2_, 3_)': 'date_diff(CAST(2_ as DATE), 3_, 1_)',
                'current_timestamp' : 'CURRENT_TIMESTAMP()',
                "'month'" : 'MONTH',
                "'dd'" : 'DAY',
                'convert_timezone' : 'DATE(2_,1_)',
                'dateadd(1_, 2_, 3_)' : 'date_add(3_,INTERVAL 2_ 1_)',
                'to_date(1_)' : 'CAST(1_ AS DATE)',
                "'hour'" : 'HOUR',
                "'year'" : 'YEAR',
                'contains(1_,2_)' : 'REGEXP_CONTAINS(1_,2_)',
                'replace(1_,2_)' : 'REPLACE(1_,2_,"''")',
                'date_trunc(1_,2_)' :'TIMESTAMP_TRUNC(2_, 1_)',
                'timestamp_ltz' : 'TIMESTAMP',
                'timestamp_ntz' : 'TIMESTAMP',
                'varchar()' : 'String',
                'VARCHAR()' : 'String',
                'TIMESTAMP_TZ' : 'TIMESTAMP',
                'TIMESTAMP_NTZ' : 'TIMESTAMP',
                'nvl(1_,2_)' : 'ifnull(1_,2_)',
                'TO_NUMBER(1_,2_,3_)' : 'CAST(1_ AS NUMERIC)',
                'to_date(1_,2_)' : 'CAST(1_ AS DATE)',
                'TO_DATE(1_,2_)' : 'CAST(1_ AS DATE)',
                'NUMBER()' : 'NUMERIC',

                }
direct_conversion = {
                        'integer':'INT64',
                        'double' : 'FLOAT64',
                        'USE ' : '--USE ',
                        'GRANT ': '--GRANT ',
                        'union all' : 'UNION',
                        'UNION' : 'UNION ALL',
                        'contains(' : 'REGEXP_CONTAINS(',
                        'cs_supp.' : ' ',
                        "(dow" : "(DAYOFWEEK"
}

datatype = {'VARCHAR' : 'String',
            'NUMBER' : 'NUMERIC',
            'TIMESTAMP_TZ' : 'TIMESTAMP',
            'TIMESTAMP_NTZ' : 'TIMESTAMP'




}

regex_map = {
    'AS "[aA-zZ_].*' : ['"',''],
    "[aA-zZ]\'\'" : ["''", r"\'"]
}
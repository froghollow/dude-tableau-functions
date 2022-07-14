"""
## tableau_functions.py -- Common code module for DUDE Tableau Intergration
#  (by RAMyers, DAAB.BSSD@fiscal.treasury.gov, 04/20/2022)
"""
import tableauserverclient as TSC
import json
import os

if 'AWS_DEFAULT_REGION' not in os.environ.keys():
    os.environ['AWS_DEFAULT_REGION'] = 'us-gov-west-1'
if 'TdsxOutpath' not in os.environ.keys():
    os.environ['TdsxOutpath'] = '/tmp/tdsx/'
if 'GlueLocationUri' not in os.environ.keys():
    os.environ['GlueLocationUri'] = 's3://wc2h-dtl-prd-transient/TDSX/'

import batch_functions as bat

token = json.loads(bat.get_ssm_parm(os.environ['TableauAuthTokenName']))
tableau_auth = TSC.PersonalAccessTokenAuth(token['token_name'], token['token_secret'], os.environ['TableauSiteName'] )


server = TSC.Server(os.environ['TableauServerUrl'], use_server_version=True )
print(server.version)

def get_tableau_project_by_name(project_name):
    ''' Get a Tableau project by its Name '''
    got_project = None
    with server.auth.sign_in(tableau_auth):
        projects, pagination_item = server.projects.get()
        for project in projects:
            if project.name == project_name:
                print(project.name, project.id)
                got_project = project
                break
    return got_project

def get_tds_by_name(datasource_name):
    ''' Get a Tableau Data Set (tds) by its Name
        ( +ToDo filter by project for Dev/Prod isolation? )
    '''
    tds = None
    with server.auth.sign_in(tableau_auth):
        all_datasources, pagination_item = server.datasources.get()
        for datasource in all_datasources:
            if datasource.name == datasource_name:
                print(datasource.name, datasource.id)
                datasource_id = datasource.id
                tds = datasource
                break
    return tds

def convert_hyper_to_s3(tds_id, tdsx_filename):
    ''' Download a Tableau Data Set Hyper Extract (tdsx), Convert to CSV Stored on S3  '''
    import zipfile

    # tableauhyperapi requires Python 3.7 64-bit ...
    from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode

    #outpath = '/mnt/efs/tdsx/'  # pending EFS access point
    outpath = '/tmp/tdsx/'
    outpath = os.environ['TdsxOutpath']

    download_path  = outpath + 'downloaded/{}'.format(tdsx_filename)
    extracted_path = outpath + 'extracted/{}'.format(tdsx_filename)
    hyperfile_path = outpath + 'extracted/{}/Data/Extracts/{}'
    tdsx_outpath   = outpath + 'processed/{}/D{}.Extract/'.format(tdsx_filename[14:].upper(),tdsx_filename[:6])

    try:
        os.makedirs(tdsx_outpath)
        os.makedirs(outpath + 'extracted/')
        os.makedirs(outpath + 'downloaded/')
    except:
        # ToDo:  test 'not already exists'
        pass

    tdsx_zipfile = None
    with server.auth.sign_in(tableau_auth):
        tdsx_zipfile = server.datasources.download(tds_id, filepath=download_path, include_extract=True)

    # if tdsx_zipfile:
    with zipfile.ZipFile(tdsx_zipfile, mode="r") as archive:
        archive.printdir()
        archive.extractall(path=extracted_path )

    for hyperfile in os.listdir(hyperfile_path.format(tdsx_filename,'')) :
        print(hyperfile)
        with HyperProcess(True, parameters={'log_config': ''}) as hyper:
            with Connection(hyper.endpoint, hyperfile_path.format(tdsx_filename, hyperfile)) as connection:
                schema_names = connection.catalog.get_schema_names()
                print(schema_names)
                table_names = connection.catalog.get_table_names(schema='Extract')
                print(table_names)

                for table_name in table_names:
                    table_definition = connection.catalog.get_table_definition(name=table_name)
                    col_names = []
                    tds_columns = []
                    for column in table_definition.columns:
                        print(f"Column {column.name} has type={column.type}") # and nullability={column.nullability}")
                        tds_column = {
                            "Name" : str(column.name).lower().replace(' ','_').replace('"',''),
                            "Type" : str(column.type).lower(),
                            "Comment" : "",
                            "Parameters" : { 'tdsColName' : str(column.name) }
                        }
                        col_names.append( tds_column["Name"])
                        tds_columns.append(tds_column)
                    print("")
                    with connection.execute_query(query=f"SELECT * FROM {table_name} " ) as result:
                        rows = list(result)

        import csv
        csvfilename = tdsx_outpath + hyperfile.replace('.hyper', '.csv')
        with open(csvfilename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerow( col_names)
            writer.writerows( rows )

        '''
        # ToDo -- gzip output file(s) use streaming to support HUGE files
        import gzip
        import shutil
        with open(csvfilename, 'rb') as f_in:
            with gzip.open(csvfilename + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        glue_table_input['TableInput']['Parameters']['compressionType'] = 'gzip'
        '''
        # save column metadata
        bat.put_file( "file://" + extracted_path + "/tds_columns.json" , json.dumps(tds_columns) )
        #glue_table_input['TableInput']['StorageDescriptor']['Columns'] = tds_columns

        # copy output to S3
        s3urlname = csvfilename.replace(outpath + 'processed', os.environ['GlueLocationUri'] + 'PROCESSED')
        bat.copy_file( 'file://' + csvfilename , s3urlname )

    print( os.listdir(tdsx_outpath) )

    return s3urlname, tds_columns

'''
### SAMPLE DRIVER
import sys
sys.path.insert( 0, os.path.abspath(os.getcwd() + '/common/batch_functions/python') )

os.environ['TableauAuthTokenName'] = 'dtl-prd-tableau'
os.environ['TableauServerUrl'] = "https://tableau.wc2h.treasury.gov/"
os.environ['TableauSiteName'] = "FiscalService"

get_tableau_project_by_name('Production')
tds = get_tds_by_name ('Analytics Dispute Dashboard Extract')
'''

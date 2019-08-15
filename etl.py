import configparser
import psycopg2
import boto3
from sql_queries import  staging_songs_copy, copy_table_queries, insert_table_queries

                              

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        if isinstance(query,(list,)):
            for subquery in query:
                cur.execute(subquery)
                conn.commit()
        else:
            cur.execute(query)
            conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()



def copy_staging_songs (rootpath):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY                         = config.get('AWS','KEY')
    SECRET                      = config.get('AWS','SECRET')
    client                      = boto3.client('s3',
                                    region_name="us-west-2",
                                    aws_access_key_id=KEY,
                                   aws_secret_access_key=SECRET
                                )
    cnt = 0
    all_files = []
    all_folders = []

    # Get list of folders with json files
    
    print('Scanning folders with json files.....')
    foldersL1 = client.list_objects_v2(Bucket='udacity-dend', Prefix='song_data/', Delimiter='/')
    for folder1 in foldersL1.get('CommonPrefixes'):
        #print(folder1.get('Prefix'))
        foldersL2=client.list_objects_v2(Bucket='udacity-dend', Prefix=folder1.get('Prefix'), Delimiter='/')
        for folder2 in foldersL2.get('CommonPrefixes'):
            #print(folder2.get('Prefix'))
            foldersL3=client.list_objects_v2(Bucket='udacity-dend', Prefix=folder2.get('Prefix'), Delimiter='/')
            for folder3 in foldersL3.get('CommonPrefixes'):
                foldername = 's3://udacity-dend/' + folder3.get('Prefix')
                all_folders.append(foldername)
                print(foldername)
                cnt = cnt + 1
    
    if len(all_folders)>0:
        print('{} folders with json files found. First folder: {}.  Last folder: {}'.format(len(all_folders), all_folders[0], all_folders[len(all_folders)-1]))  
    
      
    # Generate queries to copy songs data into staging tables
    print('Generating queries to copy songs into staging tables....')
    del staging_songs_copy[:]   # clear array contents
    for folder in all_folders:
        staging_songs_copy.append('copy staging_songs from \'' + folder + '\' iam_role ' + config.get('IAM_ROLE','ARN') + ' region  \'us-west-2\' COMPUPDATE OFF STATUPDATE OFF  JSON  \'auto\' ;')


        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
        
    copy_staging_songs(config.get('S3','SONG_DATA'))
    
    print('Loading tables ...')
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('Data load completed successfully.')
    conn.close()


if __name__ == "__main__":
    main()
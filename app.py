from flask import Flask,request,jsonify,send_from_directory,abort,after_this_request,Response
import pandas as pd
from elasticsearch import Elasticsearch,ElasticsearchException
import time
import os


app = Flask(__name__)

# create 3 requests
# Check the connection to host.
# 1.If connection is fine get indices.
# 2.Once the search is made get the search response.
# 3. To download data.
# clean up:
# 1. add better logging
# 2. exception handling with appropriate error codes and messages.


@app.route('/indices',methods=['POST'])
def getIndices():
    req=request.get_json()
    host=req['host']
    try:
      es=Elasticsearch(host)
      indices=[i for i in es.indices.get_alias('*')]
    except ElasticsearchException as ex:
      return jsonify({'error':'Connection not found'})
    cleanup()

    
    return jsonify({'indices':indices})

def cleanup():
  try:
    os.remove('./static/csv/result.csv')
  except FileNotFoundError as ex:
    print('file not found')
  try:
    os.remove('./static/excel/result.xlsx')
  except FileNotFoundError as ex:
    print('file not found')

@app.route('/search',methods=['POST'])
def getSearchResponse():
  req=request.get_json()
  host=req['host']
  query=req['query']
  index=req['index']
  try:
      es=Elasticsearch(host)
      res=es.search(body=query,index=index)
  except ElasticsearchException as ex:
    return jsonify({'error':'Connection not found'})
    
  return jsonify(res)

@app.route('/download',methods=['POST'])
def downloadFile():
  req=request.get_json()
  host=req['host']
  indexName=req['index']
  bd=req['query']
  fileformat=req['format']
  downloadsize=req['downloadsize']
  try:
      es=Elasticsearch(host)
      s1=time.time()
      for i in range(0,downloadsize):    
          if i!=0:
              bd['search_after']=[]
              bd['search_after'].append(test0.loc[len(test0)-1]['_id'])
              test0=test0.append(pd.DataFrame([i for i in es.search(index=indexName,body=bd,size=10000)['hits']['hits']]),ignore_index=True)
          else:
              test0=pd.DataFrame([i for i in es.search(index=indexName,body=bd,size=10000)['hits']['hits']])
      s2=time.time()
      print(s2-s1)
      s3=time.time()
      finaldf=pd.DataFrame(list(test0._source))
      s4=time.time()
      print(s4-s3)
      print(finaldf.shape)
      
      path=None
      if fileformat=='csv':
        s5=time.time()
        finaldf.to_csv('./static/csv/result.csv')
        s6=time.time()
        print(s6-s5)
        path='./static/csv/'
        filename='result.csv'
      elif fileformat=='excel':
        s5=time.time()
        finaldf.to_excel('./static/excel/result.xlsx')
        s6=time.time()
        print(s6-s5)
        path='./static/excel/'
        filename='result.xlsx'
      return send_from_directory(path,filename,as_attachment=True)
  except ConnectionError as ex:
    return jsonify({'error':'Connection not found'})
  except ElasticsearchException as ex:
    print(ex)
    return jsonify({'error':'Connection not found'})
  except FileNotFoundError as filenotfound:
    return abort(500,'filecould not be created')
  except Exception as ex:
    print(ex)
    return jsonify({'error':'Connection not found'})


if __name__=='__main__':
  app.run(host='0.0.0.0',debug=True)

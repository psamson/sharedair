import flask
from flask import request, jsonify
import datetime
import re
from datetime import datetime, timedelta
import os.path
import dateutil.parser
import pymysql

conn = pymysql.connect(host='psamson.engin.umich.edu', port=3306, user='qtbaReader', passwd='2rVe2aVUazsnQ7*?', db='415798_trajectory')

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return '''<h1>Trajectory Archive</h1>
<p>A prototype API for reading site-specific trajectories.</p>'''

@app.route('/site', methods=['GET'])
def api_all():
    if 'siteId' in request.args:
        siteId = str(request.args['siteId'])
    else:
        return "Error: No siteId field provided. Please specify a siteId."
    latlons = {}
    tableId = "a"+siteId
    #print ("Reading from ",tableId)
    sqlall = "SELECT date, "
    count = 1
    for i in range(1,37,1):
        inti1 = str(i)
        inti2 = str(i+1)
        if (i == 1):
            count0 = str(count)
            count1 = str(count+1)
            sqlall = sqlall + "SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count0+"),':',-1) as i"+inti1+", SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count1+"),':',-1) as lat"+inti1+" , SUBSTRING_INDEX(SUBSTRING_INDEX(lon_array,';',"+count1+"),':',-1) as lon"+inti1
        else:
            count += 2
            count0 = str(count)
            count1 = str(count+1)
            sqlall = sqlall + ", SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count0+"),':',-1) as i"+inti1+", SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count1+"),':',-1) as lat"+inti1+" , SUBSTRING_INDEX(SUBSTRING_INDEX(lon_array,';',"+count1+"),':',-1) as lon"+inti1
    #print ("Reading from ",tableId)
    sqlall = sqlall+" FROM "+tableId
    #print ("Reading from ",tableId)
    #sqlall = "SELECT date, lat_array, lon_array FROM "+tableId
    curall = conn.cursor()
    curall.execute (sqlall)
    numall = curall.rowcount
    #print(numall)
    datall = curall.fetchall()
    if (numall > 0):
        countId = 0
        #Loop through trajectories
        for rowall in datall:
            thisDate       = rowall[0]
            
            latlons[countId] = [rowall[0], rowall[1], rowall[2], rowall[3], rowall[4], rowall[5], rowall[6], rowall[7], rowall[8], rowall[9], rowall[10], rowall[11], rowall[12], rowall[13], rowall[14], rowall[15], rowall[16], rowall[17], rowall[18], rowall[19], rowall[20], rowall[21], rowall[22], rowall[23], rowall[24], rowall[25], rowall[26], rowall[27], rowall[28], rowall[29], rowall[30], rowall[31], rowall[32], rowall[33], rowall[34], rowall[35], rowall[36], rowall[37], rowall[38], rowall[39], rowall[40], rowall[41], rowall[42], rowall[43], rowall[44], rowall[45], rowall[46], rowall[47], rowall[48], rowall[49], rowall[50], rowall[51], rowall[52], rowall[53], rowall[54], rowall[55], rowall[56], rowall[57], rowall[58], rowall[59], rowall[60], rowall[61], rowall[62], rowall[63], rowall[64], rowall[65], rowall[66], rowall[67], rowall[68], rowall[69], rowall[70], rowall[71], rowall[72]]
            countId += 1
            
    return jsonify(latlons)

@app.route('/site/date', methods=['GET'])
def api_id():
    if 'siteId' in request.args:
        siteId = str(request.args['siteId'])
    else:
        return "Error: No siteId field provided. Please specify a siteId."
    latlons = {}
    if 'd' in request.args:
        dateraw = request.args['d']
        dateparts = dateraw.split("-")
        yearraw  = dateparts[0]
        year  = yearraw[-2:]
        month = dateparts[1]
        day   = dateparts[2]
    else:
        return "Error: No date field provided. Please specify a date in format 'YYYY-MM-DD'."
        
    tableId = "a"+siteId
    #sqldate = "SELECT date, lat_array, lon_array FROM "+tableId+" WHERE yr = '"+year+"' and mn='"+month+"' and dy='"+day+"'" 
    sqldate = "SELECT date, "
    count = 1
    for i in range(1,37,1):
        inti1 = str(i)
        inti2 = str(i+1)
        if (i == 1):
            count0 = str(count)
            count1 = str(count+1)
            sqldate = sqldate + "SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count0+"),':',-1) as i"+inti1+", SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count1+"),':',-1) as lat"+inti1+" , SUBSTRING_INDEX(SUBSTRING_INDEX(lon_array,';',"+count1+"),':',-1) as lon"+inti1
        else:
            count += 2
            count0 = str(count)
            count1 = str(count+1)
            sqldate = sqldate + ", SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count0+"),':',-1) as i"+inti1+", SUBSTRING_INDEX(SUBSTRING_INDEX(lat_array,';',"+count1+"),':',-1) as lat"+inti1+" , SUBSTRING_INDEX(SUBSTRING_INDEX(lon_array,';',"+count1+"),':',-1) as lon"+inti1
    #print ("Reading from ",tableId)
    sqldate = sqldate+" FROM "+tableId+" WHERE yr = '"+year+"' and mn='"+month+"' and dy='"+day+"'"
    curdate = conn.cursor()
    curdate.execute (sqldate)
    numdate = curdate.rowcount
    datdate = curdate.fetchall()
    #print (numdate, datdate)
    if (numdate > 0):
        countId = 0
        #datdate = datdate.replace('"','')
        for rowdate in datdate:
            thisDate       = rowdate[0]
            print("Processing ",thisDate," for site '",siteId,"'")
            #print(rowdate)
            latlons[countId] = [rowdate[0], int(rowdate[1]), float(rowdate[2].replace("\"","")), float(rowdate[3].replace("\"","")), int(rowdate[4]), float(rowdate[5].replace("\"","")), float(rowdate[6].replace("\"","")), int(rowdate[7]), float(rowdate[8].replace("\"","")), float(rowdate[9].replace("\"","")), int(rowdate[10]), float(rowdate[11].replace("\"","")), float(rowdate[12].replace("\"","")), int(rowdate[13]), float(rowdate[14].replace("\"","")), float(rowdate[15].replace("\"","")), int(rowdate[16]), float(rowdate[17].replace("\"","")), float(rowdate[18].replace("\"","")), int(rowdate[19]), float(rowdate[20].replace("\"","")), float(rowdate[21].replace("\"","")), int(rowdate[22]), float(rowdate[23].replace("\"","")), float(rowdate[24].replace("\"","")), int(rowdate[25]), float(rowdate[26].replace("\"","")), float(rowdate[27].replace("\"","")), int(rowdate[28]), float(rowdate[29].replace("\"","")), float(rowdate[30].replace("\"","")), int(rowdate[31]), float(rowdate[32].replace("\"","")), float(rowdate[33].replace("\"","")), int(rowdate[34]), float(rowdate[35].replace("\"","")), float(rowdate[36].replace("\"","")), int(rowdate[37]), float(rowdate[38].replace("\"","")), float(rowdate[39].replace("\"","")), int(rowdate[40]), float(rowdate[41].replace("\"","")), float(rowdate[42].replace("\"","")), int(rowdate[43]), float(rowdate[44].replace("\"","")), float(rowdate[45].replace("\"","")), int(rowdate[46]), float(rowdate[47].replace("\"","")), float(rowdate[48].replace("\"","")), int(rowdate[49]), float(rowdate[50].replace("\"","")), float(rowdate[51].replace("\"","")), int(rowdate[52]), float(rowdate[53].replace("\"","")), float(rowdate[54].replace("\"","")), int(rowdate[55]), float(rowdate[56].replace("\"","")), float(rowdate[57].replace("\"","")), int(rowdate[58]), float(rowdate[59].replace("\"","")), float(rowdate[60].replace("\"","")), int(rowdate[61]), float(rowdate[62].replace("\"","")), float(rowdate[63].replace("\"","")), int(rowdate[64]), float(rowdate[65].replace("\"","")), float(rowdate[66].replace("\"","")), int(rowdate[67]), float(rowdate[68].replace("\"","")), float(rowdate[69].replace("\"","")), int(rowdate[70]), float(rowdate[71].replace("\"","")), float(rowdate[72].replace("\"",""))]
            countId += 6
    else:
        latlons[0] = [dateraw, year, month, day, tableId]
     
    #return rowdate   
    return jsonify(latlons)

app.run(host='0.0.0.0', port=80)
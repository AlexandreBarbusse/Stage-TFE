# -*- coding: utf-8 -*-
"""
/***************************************************************************
 
                              -------------------
        begin                : 2017-03-24
        git sha              : $Format:%H$
        copyright            : (C) 2017 by Alexandre Barbusse
        email                : alexandre.barbusse@gmail.com
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""




import requests
from owslib.sos import SensorObservationService
from owslib.etree import etree
import datetime
import dateutil.parser
import decimal
import time

import owslib.swe.observation.sos200
import owslib.swe.observation.sos100
import pandas as pd
from pandas import Series
from osgeo import ogr, osr
import pyproj


import logging
owslib_log = logging.getLogger('owslib')
# Add formatting and handlers as needed
owslib_log.setLevel(logging.DEBUG)


logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True



##                          LIST OF SOS 2.0 SERVERS 
SOS200serverList = []

SOS200serverList.append('http://insitu.webservice-energy.org/52n-sos-webapp/sos')
SOS200serverList.append('http://localhost:8080/52n-sos-webapp_4.3.7/sos')
SOS200serverList.append('http://gin.gw-info.net/GinService/sos/gw')
SOS200serverList.append('https://cida.usgs.gov/ngwmn_cache/sos')
SOS200serverList.append('http://vesk.ve.ismar.cnr.it/observations/sos/kvp')
SOS200serverList.append('http://sk.ise.cnr.it/observations/sos/kvp')
SOS200serverList.append('http://geonodenodc.ogs.trieste.it/observations/sos/kvp')
SOS200serverList.append('http://geo.irceline.be/sos/service')






def getCapabilitiesSOS200(sos_url):
    
    response = requests.get(sos_url + '?REQUEST=GetCapabilities&SERVICE=SOS&ACCEPTVERSIONS=2.0.0')
    sos = SensorObservationService(None,xml=response.content)
    
    ##  Return information
    #
    #Service information
    #
    # Service Identification
    print("\n\n")
    print("Service Identification")
    print("\n")
    id = sos.identification
    print("\n")
    print("Title")
    print("\n")
    print id.title
    print("\n")
    print("Abstract")
    print("\n")
    print id.abstract
    print("\n")
    print("Keywords")
    print("\n")
    print id.keywords
    print("\n")
    
    
    # Service Provider
    print("\n\n\n")
    print("Service Provider")
    p = sos.provider
    print("\n")
    print("Name")
    print("\n")
    print(p.name)
    print("\n")
    print("Site")
    print("\n")
    print(p.url)
    print("\n")
    print("Contact phone")
    sc = p.contact
    print("\n")
    print(sc.phone)
    print("\n")
    print("Contact email")
    print("\n")
    print(sc.email)
    print("\n")
    print("Adress")
    print("\n")
    print(sc.address)
    print(sc.city)
    print(sc.region)
    print(sc.postcode)
    print(sc.country)
    
    




def WGS84conversion(off):
    try:
        former_srs = pyproj.Proj(init=off.bbox_srs.getcode())
        wgs84 = pyproj.Proj(init='EPSG:4326')
        
        former_left=off.bbox[0]
        former_bottom=off.bbox[1]
        former_right=off.bbox[2] 
        former_top=off.bbox[3]
        
        
        wgs84_left, wgs84_bottom = pyproj.transform(former_srs, wgs84, former_left, former_bottom)
        wgs84_right, wgs84_top = pyproj.transform(former_srs, wgs84, former_right, former_top)
        
        return (wgs84_left, wgs84_bottom, wgs84_right, wgs84_top)
    except:
        return off.bbox




class GetOfferingsList:
    def __init__(self, sos, WGS84bbox):
        self.offering_list = []
        for off in sos.offerings:
            if WGS84conversion(off) == WGS84bbox:
                self.offering_list.append(off)
                




def printStations(sos_url):
    get_cap_resp1 = requests.get(sos_url + '?REQUEST=GetCapabilities&SERVICE=SOS&ACCEPTVERSIONS=2.0.0', stream=True)
    sos1 = SensorObservationService(None,xml=get_cap_resp1.content, version="2.0.0")
    WGS84bbox_set=set(WGS84conversion(off) for off in sos1.offerings)
    WGS84bbox_list=list(WGS84bbox_set)
    
    
    print "\n\n\n"
    print ">>> >>> List of station locations for SOS server", sos_url, " <<< <<<"
    print "\n\n"
    
    for i, s in enumerate(WGS84bbox_list):
        print ">> station [%d]: "%i, s
        
    print "\n\n\n"


def printOfferings(sos_url, station_number):
    get_cap_resp1 = requests.get(sos_url + '?REQUEST=GetCapabilities&SERVICE=SOS&ACCEPTVERSIONS=2.0.0', stream=True)
    sos1 = SensorObservationService(None,xml=get_cap_resp1.content, version="2.0.0")
    WGS84bbox_set=set(WGS84conversion(off) for off in sos1.offerings)
    WGS84bbox_list=list(WGS84bbox_set)
    station = WGS84bbox_list[station_number]
    
    print "\n\n\n"
    print ">>> >>> Offering list for station located at", station, " <<< <<<"
    print "\n\n"
    
    for i, o in enumerate(GetOfferingsList(sos1, station).offering_list):
        print ">> offering [%d]: "%i, o.id
        
    print "\n\n\n"



def printObservedProperties(sos_url, station_number, offering_number):
    get_cap_resp1 = requests.get(sos_url + '?REQUEST=GetCapabilities&SERVICE=SOS&ACCEPTVERSIONS=2.0.0', stream=True)
    sos1 = SensorObservationService(None,xml=get_cap_resp1.content, version="2.0.0")
    WGS84bbox_set=set(WGS84conversion(off) for off in sos1.offerings)
    WGS84bbox_list=list(WGS84bbox_set)
    station = WGS84bbox_list[station_number]
    off = GetOfferingsList(sos1, station).offering_list[offering_number]
    
    print "\n\n\n"
    print ">>> >>> ObservedProperties list for offering", off.id, " <<< <<<"
    print "\n\n"
    
    for i, p in enumerate(off.observed_properties):
        print ">> property [%d]: "%i, p
        
    print "\n\n\n"
        
        
def printTimeBeginEndPositions(sos_url, station_number, offering_number):
    get_cap_resp1 = requests.get(sos_url + '?REQUEST=GetCapabilities&SERVICE=SOS&ACCEPTVERSIONS=2.0.0', stream=True)
    sos1 = SensorObservationService(None,xml=get_cap_resp1.content, version="2.0.0")
    WGS84bbox_set=set(WGS84conversion(off) for off in sos1.offerings)
    WGS84bbox_list=list(WGS84bbox_set)
    station = WGS84bbox_list[station_number]
    off = GetOfferingsList(sos1, station).offering_list[offering_number]
    
    print "\n\n\n"
    print ">>> >>> Time begin position and end position for offering", off.id, " <<< <<<"
    print "\n\n"
    
    print ">> Begin Position: ",sos1.contents[off.id].begin_position
    print ">> End Position: ", sos1.contents[off.id].end_position
    
    print "\n\n\n"




def getSeriesSOS200(sos_url, station_number, offering_number, property_number, starting_time_string, ending_time_string):
    
    dates = []
    values = []

    get_cap_resp1 = requests.get(sos_url + '?REQUEST=GetCapabilities&SERVICE=SOS&ACCEPTVERSIONS=2.0.0', stream=True)
    sos1 = SensorObservationService(None,xml=get_cap_resp1.content, version="2.0.0")
    
    #######
    #######                     DEFINING REQUEST PARAMETERS
    #######
    
    WGS84bbox_set=set(WGS84conversion(off) for off in sos1.offerings)
    WGS84bbox_list=list(WGS84bbox_set)
                    
    #                           selecting STATION
    station = WGS84bbox_list[station_number]
    
    
    #                           selecting OFFERINGS
    off = GetOfferingsList(sos1, station).offering_list[offering_number]
    offerings = [off.id]
    selected_offering = off.id
    
    #                           selecting OBSERVED PROPERTIES
    prop = off.observed_properties[property_number]
    observedProperties = [prop]
          
    #                           selecting FORMAT " 
    omFormat = 'http://www.opengis.net/om/2.0'
    waterml2Format = 'http://www.opengis.net/waterml/2.0'
    jsonFormat = 'application/json'
        
    #                           namespaces for request
    namespaces = 'xmlns(om,http://www.opengis.net/om/2.0)'
         
        
    #                           selecting TIMEPERIOD
    user_starting_time = dateutil.parser.parse(starting_time_string)
    user_ending_time = dateutil.parser.parse(ending_time_string)
        
    
    #######
    #######                     REQUEST PARAMETERS ARE NOW DEFINED
    #######
    
    
    
    period_starting_time = user_starting_time
    iso_period_starting_time = period_starting_time.isoformat()
    period_ending_time = period_starting_time + datetime.timedelta(seconds = 43200)
    unit = ''
    
    
    
    while period_ending_time <= user_ending_time:
        
        iso_period_starting_time = period_starting_time.isoformat()
        iso_period_ending_time = period_ending_time.isoformat()
        event_time = "om:phenomenonTime,"+str(iso_period_starting_time)+"/"+str(iso_period_ending_time)
        response1 = None
        
        while response1 is None:
            try:
                response1 = sos1.get_observation(offerings=offerings, responseFormat=omFormat, observedProperties=observedProperties, timeout=600, namespaces=namespaces, eventTime=event_time)
            except:
                pass
           
             
             
        xml_tree1 = etree.fromstring(response1)
        parsed_response1 = owslib.swe.observation.sos200.SOSGetObservationResponse(xml_tree1)
        unit = parsed_response1.observations[0].get_result().uom
            
        for i, obs in enumerate(parsed_response1.observations):
            if obs.resultTime is not None and obs.get_result().value is not None:
                dates.append(obs.resultTime)
                values.append(obs.get_result().value)
                #        print ">> obsr [%d]: "%i, obs.get_result().value, obs.get_result().uom, obs.resultTime, type(obs)
            else:
                pass
    
        period_starting_time += datetime.timedelta(seconds = 43200)
        period_ending_time += datetime.timedelta(seconds = 43200)
        print period_ending_time, user_ending_time
        
    return dates, values, selected_offering, prop, unit







#######
#######                     TIME SERIES PLOT FUNCTION

def plotSeries(dates, values, observedproperty, unit):
    
    print ">>> START TIME SERIE DISPLAY"
    print "\n\n"
                 
    series = pd.Series(values, index=dates)
    ax = series.plot(grid=True)
    ax.set_ylabel(observedproperty + " (" + unit +")" )
    
    print "\n\n"
    print ">>> END TIME SERIES DISPLAY"


#######
#######                     TIME SERIES ARRAY DISPLAY FUNCTION


def arraySeries(dates, values, observedproperty, unit):
    
    print ">>> START TIME SERIE DISPLAY"
    print "\n\n"
                 
    series = pd.Series(values, index=dates, name=observedproperty + " (" + unit +")" )
    print series
    
    print "\n\n"
    print ">>> END TIME SERIES DISPLAY"
    
    
#######
#######                     TIME SERIES EXPORT FUNCTION


def exportSeries(dates, values, observedproperty, unit, path):
    
    print ">>> START EXPORT"
    print "\n\n"
                 
    series = pd.Series(values, index=dates, name=observedproperty + " (" + unit +")" )
    series.to_csv(path, sep=';', header=True)
    
    print "\n\n"
    print ">>> END EXPORT"
    
    
    
#######
#######                     EXAMPLE AND BENCHMARKING
#######
#######

select_url = SOS200serverList[0]

#######
#######                     BENCHMARK I


###         Workflow from sos_url to time series plot :
start_time1 = decimal.Decimal(time.time()) 


dates, values, offering, observedproperty, unit = getSeriesSOS200(select_url, 16, 7, 0, '2004-01-01 00:00:00+00:00', '2004-02-01 00:00:00+00:00')
getCapabilitiesSOS200(select_url)
plotSeries(dates, values, observedproperty, unit)

end_time1 = decimal.Decimal(time.time()) 
###         benchmark is finished


print ">>> START BENCHMARK REQUEST DISPLAY"
print "\n\n"

timelapse = end_time1 - start_time1
print "Processing Time : %f" % timelapse


print "\n\n"
print ">>> END BENCHMARK REQUEST DISPLAY"



#######


#######
#######                     BENCHMARK II



###         Workflow from sos_url to time series array :
start_time2 = decimal.Decimal(time.time()) 


arraySeries(dates, values, observedproperty, unit)

end_time2 = decimal.Decimal(time.time()) 
###         benchmark is finished

print ">>> START BENCHMARK OPERATION DISPLAY"
print "\n\n"

timelapse = end_time2 - end_time1
print "Processing Time : %f" % timelapse


print "\n\n"
print ">>> END BENCHMARK OPERATION DISPLAY"



#######


#######
#######                     BENCHMARK III



###         Workflow from sos_url to time series export :
start_time3 = decimal.Decimal(time.time()) 

path = '/home/alexandre.barbusse/Bureau/series.csv'
exportSeries(dates, values, observedproperty, unit, path) 

end_time3 = decimal.Decimal(time.time()) 
###         benchmark is finished

print ">>> START BENCHMARK OPERATION DISPLAY"
print "\n\n"

timelapse = end_time3 - end_time3
print "Processing Time : %f" % timelapse


print "\n\n"
print ">>> END BENCHMARK OPERATION DISPLAY"



printStations(select_url)
printOfferings(select_url, 16)
printObservedProperties(select_url, 16, 7)
printTimeBeginEndPositions(select_url, 1, 7)

#######
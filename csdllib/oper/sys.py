"""
@author: Sergey.Vinogradov@noaa.gov
"""

import datetime
from configparser import ConfigParser
import io, sys

#==============================================================================
def timeStamp():
    ts = str(datetime.datetime.utcnow()) + ' UTC'
    msg('time', ts)
    return ts

#==============================================================================
def stampToDate (stamp):
    return datetime.datetime.strptime(stamp, "%Y%m%d")

#==============================================================================
def stampToTime (stamp):
    return datetime.datetime.strptime(str(int(stamp)), "%Y%m%d%H%M%S")

#==============================================================================
def dateToStamp (date):
    YYYY = str(date.year).zfill(4)
    MM   = str(date.month).zfill(2)
    DD   = str(date.day).zfill(2)
    return YYYY+MM+DD
#==============================================================================
def timeToStamp (date):
    YYYY = str(date.year).zfill(4)
    MM   = str(date.month).zfill(2)
    DD   = str(date.day).zfill(2)
    HH   = str(date.hour).zfill(2)
    MI   = str(date.minute).zfill(2)
    SE   = str(date.second).zfill(2)
    return YYYY+MM+DD+HH+MI+SE
#==============================================================================
def msg (msgType, msge, out=sys.stdout):
    '''
    Customize standard I/O here.
    '''
    if msgType.lower().startswith('i'):
        print ('[Info]: ' + msge,file=out, flush=True)
    elif msgType.lower().startswith('w'):
        print ('[Warn]: ' + msge,file=out, flush=True)
    elif msgType.lower().startswith('e'):
        print ('[Error]: '+ msge,file=out, flush=True)
    elif msgType.lower().startswith('t'):
        print ('[Time]: ' + msge,file=out, flush=True) 
    else:
        print ('[....]: ' + msge,file=out, flush=True)
    
#==============================================================================
def removeInlineComments(cfgparser, delimiter):
    '''
    Per https://stackoverflow.com/questions/9492430/inline-comments-for-configparser
    '''
    for section in cfgparser.sections():
        [cfgparser.set(section, item[0], item[1].split(delimiter)[0].strip()) for item in cfgparser.items(section)]
    return cfgparser

#==============================================================================
def config ( iniFile ): 
    '''
    Loads configuration file
    '''
    with open(iniFile) as f:
        sample_config = f.read()
        f.close()

    config = ConfigParser(allow_no_value=True)
    config.readfp( io.StringIO(sample_config) )
    config = removeInlineComments(config, '#') 

    dictionary = {}
    for section in config.sections():
        dictionary[section] = {}
        for option in config.options(section):
            val = config.get(section, option)
            try:
                dictionary[section][option] = float(val)
            except:
                dictionary[section][option] = val
        
    return dictionary

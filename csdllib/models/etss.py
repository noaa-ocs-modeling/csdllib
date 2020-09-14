"""
@author: Sergey.Vinogradov@noaa.gov
"""

import os, csv, shutil, glob
import tarfile
import numpy as np
from datetime import datetime
from csdllib import oper

#==============================================================================
def findLatestCycle (dirMask):
    
    dirs = glob.glob(dirMask+'*')
    latestDir = max(dirs, key=os.path.getctime)    
    D = os.path.basename(latestDir).split('.')[-1]

    files = glob.glob(latestDir + '/*.csv_tar')
    latestFile = max(files)

    F = os.path.basename(latestFile)
    latestCycle =  D + F[F.find('.t')+2:F.find('z.')]

    return latestCycle
    
#==============================================================================
def readStations (tarFile, verbose=1):
    """
    Reads content of tar file into the list of stations
    """    
    if verbose:
        oper.sys.msg('i', 'Reading ' + tarFile)
    if not os.path.exists (tarFile):
        oper.sys.msg('e', 'File ' + tarFile + ' is not found. Exiting.')
        return
    
    stations = []
    tar = tarfile.open(tarFile, "r:*")
    tmpDir = os.path.join(os.getcwd(),'tmp')
    try:
        shutil.rmtree(tmpDir)
    except:
        pass    
    os.mkdir(tmpDir)
    for member in tar.getmembers():
        if member.isreg():  # skip if the TarInfo is not files
            member.name = os.path.basename(member.name)
            tar.extract(member, tmpDir) # extract
            stations.append( readStation( os.path.join(tmpDir, member.name)) )
    tar.close()
    shutil.rmtree(tmpDir)
    
    return stations

#==============================================================================
def readStation (csvFile, verbose=1):
    """
    Reads one station data from csvFile
    Returns lists of dates and corresponding time series values
    Skips obs
    """
    if verbose:
        oper.sys.msg( 'i','Reading ' + csvFile)
    if not os.path.exists (csvFile):
        oper.sys.msg( 'e','File ' + csvFile + ' is not found. Exiting.')
        return

    nosid = os.path.splitext(os.path.basename(csvFile))[0]
    missingVal = 9999.
    dtime    = []
    tide     = []
    surge    = []
    bias     = []
    twl      = []
    with open( csvFile ) as csvfile:
        data = csv.reader(csvfile, delimiter=',')
        next(data, None)

        for row in data:
            row = [np.nan if float(x) == missingVal else x for x in row]
            TIME, TIDE, OB, SURGE, BIAS, TWL = row
            if TWL is np.nan:
                pass
            else:
                dtime.append   ( datetime.strptime(TIME,'%Y%m%d%H%M') )
                tide.append    ( float (TIDE) )
                surge.append   ( float(SURGE) )
                bias.append    ( float(BIAS) )
                twl.append     ( float(TWL) )
        csvfile.close()
    return  {'time'      : dtime, 
             'htp'       : tide,
             'swl'       : surge,
             'cwl'       : twl,
             'bias'      : bias,
             'nosid'     : nosid}        

        

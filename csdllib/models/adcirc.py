"""
@author: Sergey.Vinogradov@noaa.gov
"""

import os
import numpy as np
from datetime import datetime
from datetime import timedelta
import netCDF4
from csdllib.oper.sys import msg

#==============================================================================
def readGrid ( gridFile, verbose=1):
    """
    Reads ADCIRC grid file
    
    Args:
        gridFile (str): full path to fort.14 file
    Returns:
        grid (dict): field names according to ADCIRC internal variables:
    http://adcirc.org/home/documentation/users-manual-v50/
    input-file-descriptions/adcirc-grid-and-boundary-information-file-fort-14/
    """
    if verbose:
        msg( 'info', 'Reading the grid from ' + gridFile)
    if not os.path.exists (gridFile):
        msg( 'error', 'File ' + gridFile + ' does not exist.')
        return
        
    f  = open(gridFile)
    
    myDesc     = f.readline().rstrip()
    myNE, myNP = map(int, f.readline().split())    
    if verbose:
        msg( 'i','Grid description ' + myDesc + '.')
        msg( 'i','Grid size: NE= '   + str(myNE) + ', NP=' + str(myNP) + '.')

    myPoints   = np.zeros([myNP,3], dtype=float)
    myElements = np.zeros([myNE,3], dtype=int)
    
    if verbose:
        msg( 'i','Reading grid points...')

    for k in range(myNP):
        line            = f.readline().split()
        myPoints[k,0] = float(line[1])
        myPoints[k,1] = float(line[2])
        myPoints[k,2] = float(line[3])

    if verbose:
        msg( 'i','Reading grid elements...')

    for k in range(myNE):
        line              = f.readline().split()
        #myElements[k,0:2] = map(int, line[2:4])
        myElements[k,0] = int (line[2])
        myElements[k,1] = int (line[3])
        myElements[k,2] = int (line[4])
    
    myNOPE   = int(f.readline().split()[0])
    myNETA   = int(f.readline().split()[0])   
    myNVDLL  = np.zeros([myNOPE], dtype=int)
    myNBDV   = np.zeros([myNOPE, myNETA], dtype=int)
    
    if verbose:
        msg('i', 'Reading elevation-specified boundaries...')

    for k in range(myNOPE):
        myNVDLL [k] = int(f.readline().split()[0])
        for j in range(myNVDLL[k]):
            myNBDV[k,j] = int(f.readline().strip())

    myNBOU = int(f.readline().split()[0])
    myNVEL = int(f.readline().split()[0])   
    myNVELL      = np.zeros([myNBOU], dtype=int)
    myIBTYPE     = np.zeros([myNBOU], dtype=int)
    myNBVV       = np.zeros([myNBOU, myNVEL], dtype=int)
    myBARLANHT   = np.zeros([myNBOU, myNVEL], dtype=float)
    myBARLANCFSP = np.zeros([myNBOU, myNVEL], dtype=float)
    myIBCONN     = np.zeros([myNBOU, myNVEL], dtype=int)
    myBARINHT    = np.zeros([myNBOU, myNVEL], dtype=float)
    myBARINCFSB  = np.zeros([myNBOU, myNVEL], dtype=float)
    myBARINCFSP  = np.zeros([myNBOU, myNVEL], dtype=float)
    myPIPEHT     = np.zeros([myNBOU, myNVEL], dtype=float)
    myPIPECOEF   = np.zeros([myNBOU, myNVEL], dtype=float)
    myPIPEDIAM   = np.zeros([myNBOU, myNVEL], dtype=float)
    
    if verbose:
        msg('i', 'Reading normal flow-specified boundaries...')
    for k in range(myNBOU):
        line = f.readline().split()
        myNVELL[k]  = int(line[0])
        myIBTYPE[k] = int(line[1])
        
        for j in range(myNVELL[k]):
            line = f.readline().rstrip().split()            
            if myIBTYPE[k] in   [0,1,2,10,11,12,20,21,22,30]:
                myNBVV      [k,j] = int(line[0])
            elif myIBTYPE[k] in [3,13,23]:
                myNBVV      [k,j] = int  (line[0])
                myBARLANHT  [k,j] = float(line[1])
                myBARLANCFSP[k,j] = float(line[2])
            elif myIBTYPE[k] in [4,24]:
                myNBVV      [k,j] = int  (line[0])
                myIBCONN    [k,j] = int  (line[1])
                myBARINHT   [k,j] = float(line[2])
                myBARINCFSB [k,j] = float(line[3])
                myBARINCFSP [k,j] = float(line[4])
            elif myIBTYPE[k] in [5,25]:
                myNBVV      [k,j] = int  (line[0])
                myIBCONN    [k,j] = int  (line[1])
                myBARINHT   [k,j] = float(line[2])
                myBARINCFSB [k,j] = float(line[3])
                myBARINCFSP [k,j] = float(line[4])
                myPIPEHT    [k,j] = float(line[5])
                myPIPECOEF  [k,j] = float(line[6])
                myPIPEDIAM  [k,j] = float(line[7])

    f.close()
        
    return {'GridDescription'               : myDesc, 
            'NE'                            : myNE, 
            'NP'                            : myNP, 
            'lon'                           : np.squeeze(myPoints[:,0]),
            'lat'                           : np.squeeze(myPoints[:,1]), 
            'depth'                         : np.squeeze(myPoints[:,2]), 
            'Elements'                      : np.squeeze(myElements),
            'NETA'                          : myNETA, 
            'NOPE'                          : myNOPE,
            'ElevationBoundaries'           : np.squeeze(myNBDV), 
            'NormalFlowBoundaries'          : np.squeeze(myNBVV),
            'ExternalBarrierHeights'        : np.squeeze(myBARLANHT),
            'ExternalBarrierCFSPs'          : np.squeeze(myBARLANCFSP),
            'BackFaceNodeNormalFlow'        : np.squeeze(myIBCONN),
            'InternalBarrierHeights'        : np.squeeze(myBARINHT),
            'InternallBarrierCFSPs'         : np.squeeze(myBARINCFSP),
            'InternallBarrierCFSBs'         : np.squeeze(myBARINCFSB),            
            'CrossBarrierPipeHeights'       : np.squeeze(myPIPEHT),
            'BulkPipeFrictionFactors'       : np.squeeze(myPIPECOEF),            
            'CrossBarrierPipeDiameter'      : np.squeeze(myPIPEDIAM)
            }


#==============================================================================
def readTimeSeries (ncFile, ncVar = 'zeta', verbose=1):
    """
    Reads fort.61.nc-like file
    """
    if verbose:
        msg( 'i','Reading [' + ncVar + '] from ' + ncFile)
    if not os.path.exists (ncFile):
        msg( 'e','File ' + ncFile + ' does not exist.')
        return
    
    nc    = netCDF4.Dataset( ncFile )
    fld        = nc.variables[ncVar][:]
    missingVal = nc.variables[ncVar]._FillValue
    try:
        fld.unshare_mask()
    except:
        pass
    fld [np.where(fld == missingVal)] = np.nan
                          
    lon  = nc.variables['x'][:]
    lat  = nc.variables['y'][:]    
    tim  = nc.variables['time'][:]    
    nam  = nc.variables['station_name'][:]
    stations = netCDF4.chartostring(nam)  # Python3 requirement?

    ncTitle  = nc.getncattr('title')
    try:
        baseDate = datetime.strptime(nc.variables['time'].base_date[0:19], 
                                 '%Y-%m-%d %H:%M:%S')
    except: # when 00 sec is not written at all
        baseDate = datetime.strptime(nc.variables['time'].base_date[0:19], 
                                 '%Y-%m-%d %H:%M  ')

    realtime = np.array([baseDate + 
                         timedelta(seconds=int(tim[i])) 
                         for i in range(len(tim))])

    return  {'lat'       : lat, 
             'lon'       : lon, 
             'time'      : realtime, 
             'base_date' : baseDate, 
             'zeta'      : fld, 
             'stations'  : stations,
             'title'     : ncTitle}        
    
#==============================================================================
def readSurfaceField ( ncFile, ncVar = 'zeta_max', verbose=1 ):  
    """
    Reads specified variable from the ADCIRC 2D netCDF output
    and grid points along with validation time.
    Args:
        'ncFile' (str): full path to netCDF file
        'ncVar'  (str): name of netCDF field
    Returns:
        dict: 'lon', 'lat', 'time', 'base_date', 'value', 'path', 'variable'
    """
    
    if verbose:
        msg( 'i','Reading [' + ncVar + '] from ' + ncFile)

    if not os.path.exists (ncFile):
        msg( 'e','File ' + ncFile + ' does not exist.')
        return
           
    nc   = netCDF4.Dataset (ncFile)
    lon  = nc.variables['x'][:]
    lat  = nc.variables['y'][:]
    tim  = nc.variables['time'][:]
    fld  = nc.variables[ncVar][:] 

    missingVal = nc.variables[ncVar]._FillValue
    try:
        fld.unshare_mask()
    except:
        pass
    fld [fld==missingVal] = np.nan

    d = nc.variables['time'].base_date[0:19].strip()
    if len(d)==19:
        baseDate = datetime.strptime(d,'%Y-%m-%d %H:%M:%S')
    if len(d)==16:
        baseDate = datetime.strptime(d,'%Y-%m-%d %H:%M')

    realtime = np.array([baseDate +
                         timedelta(seconds=int(tim[i]))
                         for i in range(len(tim))])

    return { 'lon'      : lon, 
             'lat'      : lat, 
             'time'     : realtime, 
             'base_date': baseDate,
             'value'    : fld, 
             'path'     : ncFile,              
             'variable' : ncVar}

#==============================================================================
def readSurfaceField_ascii ( asciiFile, verbose=1 ):
    """
    Reads ADCIRC 2D output file (e.g. mmaxele)
    Args:
        'asciiFile' (str): full path to ADCIRC 2D file in ASCII format
    Returns:
        value (np.array [NP, NS]), where NP - number of grid points, 
                                     and NS - number of datasets
    """
    if verbose:
        msg( 'i','Reading ASCII file ' + asciiFile + '.')

    f  = open(asciiFile)
    
    myDesc = f.readline().strip()
    msg( 'i','Field description [' + myDesc + '].')
    line          = f.readline().split()    
    myNDSETSE     = int(line[0])
    myNP          = int(line[1])
#    myNSPOOLGE    = int(line[3])
#    myIRTYPE      = int(line[4])
#    dtdpXnspoolge = float(line[2])   
    line          = f.readline().split()
#    myTIME        = float(line[0])
#    myIT          = float(line[1])
    value = np.zeros([myNP,myNDSETSE], dtype=float)
    for s in range(myNDSETSE):
        for n in range(myNP):
            value[n,s] = float(f.readline().split()[1])    
    value = np.squeeze(value)
    
    fill_value = -99999.0
    value[value==fill_value]=np.nan
    
    f.close()
    
    return value 

#==============================================================================
def computeMax (fields):
    return np.amax(fields, axis=0)

"""
#==============================================================================
def computeMax (ncFile, ncVar='zeta'):
    nc = netCDF4.Dataset(ncFile)
    zeta  = nc.variables[ncVar][:]         
    fill_value = nc._FillValue    
    zeta = np.ma.masked_equal(zeta,fill_value)
    return {'lon'   : nc.variables['x'][:],
            'lat'   : nc.variables['y'][:],
            'value' : np.amax(zeta, axis=0)}
"""

#==============================================================================
def readFort14 ( fort14file ):
    """
    Reads ADCIRC fort.14 file
    """
    return readGrid (fort14file)

#==============================================================================
def readStationsList (fileName):
    """
    Reads fort15-like block of fort.61 stations (e.g.)
    4
    -92.1 32.03          9999991
    -92.5 32.11          9999992
    -91.8 31.12          9999993
    -90.3 32.45          9999994
    """
    f = open(fileName, 'r')
    nstations  = int(f.readline().split()[0])    
    stations = dict()
    
    stations['lon'] = []
    stations['lat'] = []
    stations['name'] = []
    for n in range(nstations):
        line = f.readline()
        stations['lon'].append(float(line.split()[0]))
        stations['lat'].append(float(line.split()[1]))
        stations['name'].append(line[20:])
    f.close()
    return stations

#==============================================================================
def readFort15 ( fort15file, verbose=1 ):
    """
    Reads ADCIRC fort.15 file according to: 
    http://adcirc.org/home/documentation/users-manual-v50/
    input-file-descriptions/
    model-parameter-and-periodic-boundary-condition-file-fort-15/
    """
    config = dict()
    tides  = dict()
    if verbose:
        msg( 'i','Reading fort.15 file ' + fort15file)

    f = open(fort15file,'r')
    config['mesh']        = f.readline().strip()
    config['description'] = f.readline().strip()
    config['NFOVER']      = int(f.readline().split()[0])
    config['NABOUT']      = int(f.readline().split()[0])
    config['NSCREEN']     = int(f.readline().split()[0])
    config['IHOT']        = int(f.readline().split()[0])
    config['ICS']         = int(f.readline().split()[0])
    config['IM']          = int(f.readline().split()[0])
    config['NOLIBF']      = int(f.readline().split()[0])
    config['NOLIFA']      = int(f.readline().split()[0])
    config['NOLICA']      = int(f.readline().split()[0])
    config['NOLICAT']     = int(f.readline().split()[0])
    config['NWP']         = int(f.readline().split()[0])
    config['node attrib'] = []
    for n in range(config['NWP']):
        config['node attrib'].append(f.readline().strip())
    config['NCOR']        = int(f.readline().split()[0])
    config['NTIP']        = int(f.readline().split()[0])
    config['NWS']         = int(f.readline().split()[0])
    config['NRAMP']       = int(f.readline().split()[0])
    config['G']           = float(f.readline().split()[0])
    config['TAU0']        = int(f.readline().split()[0])
    config['DT']          = float(f.readline().split()[0])
    config['STATIM']      = float(f.readline().split()[0])
    config['REFTIM']      = float(f.readline().split()[0])
    line = f.readline().split()
    config['WTIMINC_Year']        = int(line[0])
    config['WTIMINC_Month']       = int(line[1])
    config['WTIMINC_Day']         = int(line[2])
    config['WTIMINC_Param1']      = int(line[3])
    config['WTIMINC_Param2']      = float(line[4])
    config['WTIMINC_Param3']      = float(line[5])
    
    config['RNDAY']       = float(f.readline().split()[0])
    config['DRAMP']       = float(f.readline().split()[0])
    line = f.readline().split()
    config['TWF_GWCE_Param1']    = float(line[0])
    config['TWF_GWCE_Param2']    = float(line[1])
    config['TWF_GWCE_Param3']    = float(line[2])
    line = f.readline().split()
    config['H0']          = float(line[0])
    config['NODEDRYMIN']  = float(line[1])
    config['NODEWETRMP']  = float(line[2])
    config['VELMIN']      = float(line[3])
    line = f.readline().split()
    config['SLAM0']    = float(line[0])
    config['SFEA0']    = float(line[1])
    config['FFACTOR']     = float(f.readline().split()[0])
    config['ESL']         = float(f.readline().split()[0])
    config['CORI']        = float(f.readline().split()[0])    
    config['NTIF']        = int(f.readline().split()[0])
    
    tides['TIPOTAG_name']   = []    
    tides['TPK']   = []
    tides['AMIGT'] = []
    tides['ETRF']  = []
    tides['FFT']   = []
    tides['FACET'] = []
    
    for n in range(config['NTIF']):
        tides['TIPOTAG_name'].append(f.readline().strip())
        line = f.readline().split()
        tides['TPK'].append(float(line[0]))
        tides['AMIGT'].append(float(line[1]))
        tides['ETRF'].append(float(line[2]))
        tides['FFT'].append(float(line[3]))
        tides['FACET'].append(float(line[4]))
        
    config['NBFR']        = int(f.readline().split()[0])
    
    tides['BOUNDTAG_name'] = []
    tides['AMIG'] = []
    tides['FF']   = []
    tides['FACE'] = []
    
    for n in range(config['NBFR']):
        tides['BOUNDTAG_name'].append(f.readline().strip())
        line = f.readline().split()        
        tides['AMIG'].append(float(line[0]))
        tides['FF'].append(float(line[1]))
        tides['FACE'].append(float(line[2]))
        
    config['NETA'] = 0 # is undefined thank you very much!    
    # Finding NETA...
    f.readline() # k1
    count = 0
    stopReading = False
    while not stopReading:
        line = f.readline().split()
        if len(line)==2:
            count += 1
        else:  #o1
            config['NETA'] = count
            for m in range(config['NETA']): #o1
                f.readline()    
            stopReading = True                        
        
    for n in range(config['NBFR']-2): #p1
        f.readline()
        for m in range(config['NETA']): 
            f.readline()    
                
    config['ANGINN']    = float(f.readline().split()[0])    
    line = f.readline().split()    
    config['NOUTE']     = float(line[0])    
    config['TOUTSE']    = float(line[1])    
    config['TOUTFE']    = float(line[2])    
    config['NSPOOLE']   = int(line[3])    
    
    stations = readStationsList (f)
    
    config['NSTATIONS'] = len(stations['lon'])
    
    linelist = f.readlines()
    coldstart = linelist[len(linelist)-1].strip()
    f.close()
    
    return {'config' : config,
            'tides'  : tides,
            'stations' : stations,
            'coldstart' : coldstart
            }

#==============================================================================
def writeOffset63 ( val, offset63file, note=None):
    """
    Writes ADCIRC offset.63 file in ASCII format
    for use with pseudo pressure loading option
    Args:
        val (float)        : Array of gridded values 
        offset63file (str) : Full path to the output file
    Note:
        val should be the same size and order as your grid vectors
    """
    msg( 'i','Writing Offset63 file...')
    f = open(offset63file,'w')
    if note is None:
        f.write("# ADCIRC Offset file\n")
    else:
        f.write("# " + note + "\n")
    f.write("1.0\n")  # ADCIRC Version 55
    f.write("1.0\n")    
    for n in range(len(val)):
        f.write(str(n+1) + ' ' + str(val[n]) + '\n')
    f.close()
    return None

#==============================================================================
def readOffset63 ( offset63file):
    msg( 'e','readOffset63() is not yet implemented.')
    return None




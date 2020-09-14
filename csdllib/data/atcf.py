"""
@author: Sergey.Vinogradov@noaa.gov
"""

import sys
from datetime import datetime
from datetime import timedelta
import numpy as np
from csdllib import oper

#==============================================================================
def read ( atcfFile, product=None ):
    """
    Reads ATCF-formatted file (e.g. NHC/JTWC track, advisory, best track, or 
                               ADCIRC fort.22 file, for NWS options 19 or 20)    
    Args:
        'atcfFile': (str) - full path to the ATCF file
        'product' : (str) = 'BEST', 'OFCL', etc...        
    """
    
    oper.sys.msg( 'info','Reading ATCF file ' + atcfFile)
    
    fp = open(atcfFile)
    lines  = fp.readlines()
    fp.close()
    
    # Extract only lines that belong to the specified product
    plines = []
    pdates = []
    for line in lines:
        r = line.strip().split(',')
        p = r[4].strip()        
        d = datetime.strptime(r[2].strip(),'%Y%m%d%H')
        tau = int(r[5].strip())
        if tau:
            d = d + timedelta(hours=tau)
        if product is None or p in product:
            plines.append( line )
            pdates.append( d )
            
    myDates = np.unique( pdates )

    myLat     = [None] * len(myDates)
    myLon     = [None] * len(myDates)
    myVmax    = [None] * len(myDates)
    myMSLP    = [None] * len(myDates)   
    my34knots = [None] * len(myDates)
    my50knots = [None] * len(myDates)
    my64knots = [None] * len(myDates)
    myRmax    = [None] * len(myDates)

    # Run on lines, compare with unique dates, fill out the fields
    for line in plines:
        r = line.strip().split(',')
        d = datetime.strptime(r[2].strip(),'%Y%m%d%H')
        tau = int(r[5].strip())
        if tau:
            d = d + timedelta(hours=tau)

        for n in range(len(myDates)):

            if myDates[n] == d:
                
                latSign = -1.0
                if 'N' in r[6]:
                    latSign = 1.0     
                myLat[n] = latSign*0.1*float(r[6][:-1])

                lonSign = -1.0
                loopOver   = 0.
                if 'E' in r[7]:
                    lonSign = 1.0
                    loopOver = -360.
                myLon[n] = lonSign*0.1*float(r[7][:-1])+loopOver

                myVmax[n]    = float(r[8])
                myMSLP[n]    = float(r[9])
                
                isotach = float(r[11].strip())
                if isotach == 34.:
                    my34knots[n] = [ float(r[13].strip()), 
                                     float(r[14].strip()),
                                     float(r[15].strip()),
                                     float(r[16].strip()) ]
                if isotach == 50.:
                    my50knots[n] = [ float(r[13].strip()), 
                                     float(r[14].strip()),
                                     float(r[15].strip()),
                                     float(r[16].strip()) ]
                if isotach == 64.:
                    my64knots[n] = [ float(r[13].strip()), 
                                     float(r[14].strip()),
                                     float(r[15].strip()),
                                     float(r[16].strip()) ]
                try:    
                    myRmax[n]    = [ float(r[34].strip()), 
                                     float(r[35].strip()),
                                     float(r[36].strip()),
                                     float(r[37].strip()) ]
                except:
                    pass

    return { 
            'dates' : myDates, 
            'lat'   : myLat,   'lon' : myLon,
            'vmax'  : myVmax, 'mslp' : myMSLP,
            'neq34' : my34knots,
            'neq50' : my50knots,
            'neq64' : my64knots,
            'rmax'  : myRmax}



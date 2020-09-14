"""
@author: Sergey.Vinogradov@noaa.gov
Note: Add parsers here
"""

import csv
import numpy as np

#==============================================================================
def csvTable ( csvFile, fieldList ):
    """
    Parses fields in a csv file
    Returns values in the order of provided fields.
    """
    output = []
    with open(csvFile) as csvf:
        reader = csv.DictReader (csvf)        
        for row in reader:
            line = []
            for f in fieldList:
                try:
                    line.append (row[f])
                except:
                    continue
            output.append(line)
        csvf.close()
    return output

#==============================================================================
def csvTableToDict ( csvFile, fieldList ):
    """
    Parses fields in a csv file
    Returns a dictionary.
    """
    with open(csvFile) as csv_data:
        reader = csv.reader(csv_data)

        # eliminate blank rows if they exist
        rows = [row for row in reader if row]
        headings = rows[0] # get headings

        output = {}
        for row in rows[1:]:
            # append the dataitem to the end of the dictionary entry
            # set the default value of [] if this key has not been seen
            for col_header, data_column in zip(headings, row):
                output.setdefault(col_header, []).append(data_column)
        csv_data.close()
    
    for n in output:
        print (n)
    stop

    return output

#==============================================================================
def datumsAndLevels (stationid, masterList):
    """
    Parses file masterList of the type
       ftp://ocsftp.ncd.noaa.gov/estofs/data/ETSS_ESTOFS_Stations.csv
    for datums and flood levels 
    by stationid according to NOS CO-OPS
    """
    query = ['NOSID','Name','NWSID', \
             'ETSS HAT-ft','ETSS MSL-ft','ETSS MLLW-ft','ETSS MHHW-ft', \
             'Minor MHHW ft','Moderate MHHW ft','Major MHHW ft']
    master = csvTable (masterList, query)  

    datums = dict()
    datums['datum_hat_ft']  = np.nan
    datums['datum_msl_ft']  = np.nan
    datums['datum_mhhw_ft'] = np.nan
    datums['datum_mllw_ft'] = np.nan
    
    floodlevels = dict()
    floodlevels['fl_minor_ft']   = np.nan
    floodlevels['fl_moder_ft']   = np.nan
    floodlevels['fl_major_ft']   = np.nan

    stationDescr  = stationid   
    nosid         = stationid
    # Get data from master list
    for m in master:
        nosid = m[query.index('NOSID')]
        if nosid in stationid:
            try:
                stationDescr  = m[query.index('Name')] + \
                                  ' (NOS:' + m[query.index('NOSID')] + ' ' + \
                                  ' NWS:' + m[query.index('NWSID')] + ')'
                
                datums['datum_hat_ft']  = float(m[query.index('ETSS HAT-ft')])
                datums['datum_msl_ft']  = float(m[query.index('ETSS MSL-ft')])
                datums['datum_mhhw_ft'] = float(m[query.index('ETSS MHHW-ft')])
                datums['datum_mllw_ft'] = float(m[query.index('ETSS MLLW-ft')])
            except:
                pass
            try:
                floodlevels['fl_minor_ft'] = float(m[query.index('Minor MHHW ft')])
                floodlevels['fl_moder_ft'] = float(m[query.index('Moderate MHHW ft')])
                floodlevels['fl_major_ft'] = float(m[query.index('Major MHHW ft')])
            except:
                pass
            break

    return datums, floodlevels, nosid, stationDescr

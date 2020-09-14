"""
@author: Sergey.Vinogradov@noaa.gov
"""
import csdllib as cs
import matplotlib
matplotlib.use('Agg',warn=False)
import matplotlib.pyplot as plt
import matplotlib.tri    as tri
import matplotlib.patches as patches
import numpy as np

#==============================================================================
def set(lonlim, latlim, coast=None, fig_w=8.0):
    """
    Sets the map according to configuration
    """
    minx, maxx = lonlim
    miny, maxy = latlim
    dx = float(maxx - minx)
    dy = float(maxy - miny)
    fig_h = np.round(fig_w*dy/dx, 2)
    cs.oper.sys.msg('i','Creating a figure with sizes :' \
                    + str(fig_w) + 'x' + str(fig_w))
    fig = plt.figure(figsize=[fig_w, fig_h])
    if coast is not None:
        addCoastline(coast)
    plt.xlim([minx, maxx])
    plt.ylim([miny, maxy])
    fig.tight_layout()

# Draw parallels
    if dx <= 10.:
        dx = 1.
    elif dx <= 20.:
        dx = 2.
    elif dx <= 50.:
        dx = 5.
    elif dx <= 100.:
        dx = 10.
    else:
        dx = 20.

    if dy <= 10.:
        dy = 1.
    elif dy <= 20.:
        dy = 2.
    elif dy <= 50.:
        dy = 5.
    elif dy <= 100.:
        dy = 10.
    else:
        dy = 20.

    meridians = np.arange(np.floor(minx/10.)*10.,np.ceil(maxx/10.)*10.,dx)
    parallels = np.arange(np.floor(miny/10.)*10.,np.ceil(maxy/10.)*10.,dy)
    
    for m in meridians:
        plt.plot([m,m],[miny,maxy],':',color='gray',linewidth=1,zorder=0)
    for p in parallels:
        plt.plot([minx,maxx],[p,p],':',color='gray',linewidth=1,zorder=0)
    plt.tick_params(labelsize=7)    
    plt.xlabel('LONGITUDE, deg E')
    plt.ylabel('LATITUDE, deg N')
    
    return fig

#==============================================================================
def addField (grid, field, clim = [0,3], zorder=0, plotMax = False, lonlim=None, latlim=None):
    """
    Adds (unstructured) gridded field to the map
    """
    cs.oper.sys.msg('i','Plotting the surface.')

    lon       = grid['lon']
    lat       = grid['lat']
    triangles = grid['Elements']
    z         = field
    if len(z) != len(lon):
        cs.oper.sys.msg('e','Mesh and field sizes are not the same')
        cs.oper.sys.msg('e','   Field length is ' + str(len(z)))
        cs.oper.sys.msg('e','   Mesh  length is ' + str(len(lon)))
        return
    
    newTriangles = []
    nboundaryTriangles = 0
    for t in triangles-1:
        lons = []
        for n in range(len(t)):
            lons.append( lon[t[n]] )
        if np.ptp( np.asarray(lons) ) < 180.0:
            newTriangles.append (t)
            nboundaryTriangles += 1
    cs.oper.sys.msg('i','Number of found boundary elements: ' + 
        str(len(triangles)-nboundaryTriangles))

    #Tri  = tri.Triangulation(lon, lat, triangles=triangles-1)
    Tri  = tri.Triangulation(lon, lat, triangles=newTriangles) #-1)

    if hasattr(z,'mask'): 
        zmask = z.mask
    else:
        zmask = np.ones(len(z), dtype=bool)        
    # Set mask 
    # TODO : Optimize this following loop
    #
    mask = np.ones(len(Tri.triangles), dtype=bool)
    count = 0
    for t in Tri.triangles:
        count+=1
        ind = t
        if np.any(zmask[ind-1]):
            mask[count-1] = False    
    Tri.set_mask = mask

    myCmap = plt.cm.jet
    #print ('zmin/max = ' + str(np.nanmin(z)) + ' ' + str(np.nanmax(z)))
    #print ('clim = ' + str(clim[0]) + ' ' + str(clim[1]))
    #print('len(z)  =' + str(len(z)))

    plt.tripcolor(Tri, z, shading='gouraud',\
                          edgecolors='none', \
                          cmap=myCmap, \
                          vmin=clim[0], \
                          vmax=clim[1], zorder=zorder)
    #current_cmap = matplotlib.cm.get_cmap()
    #current_cmap.set_bad(color='gray')

    if plotMax:
        zmax = np.nanmax(z)
        imax = np.where (z == zmax)[0][0]


        if lonlim is not None and latlim is not None:
            zmax = np.nanmax(z[ (lonlim[0]  <= grid['lon']) & 
                            (grid['lon']<= lonlim[1])   &
                            (latlim[0]  <= grid['lat']) & 
                            (grid['lat']<= latlim[1] ) ])
            imax = np.where(z == zmax)[0][0]

        strzmax = str(np.round(zmax,1))
        plt.plot( lon[imax], lat[imax], 'ok', markerfacecolor='r',zorder=zorder+1)
        plt.text( lon[imax], lat[imax], strzmax,fontsize=6,zorder=zorder+2)  

    cbar = plt.colorbar()
    cbar.ax.tick_params(labelsize=8)     

#==============================================================================
def readCoastline (coastlineFile): 

    f = open(coastlineFile,'rb')
    xc = []
    yc = []
    for line in f:
        xc.append(float(line.split()[0]))
        yc.append(float(line.split()[1]))
    f.close()        

    return {'lon' : xc, 
            'lat' : yc}

#==============================================================================
def addCoastline (coast, col = 'gray'):
    """
    Adds coastline to the map
    """
    plt.plot(coast['lon'], coast['lat'],',',color=col,zorder=2)
    plt.plot( [360.-x for x in coast['lon']], coast['lat'],',',color=col,zorder=1)

#==============================================================================
def addCities (ax, citiesFile):
    """
    Adds cities to the map
    """
    return ax

#==============================================================================
def addPoints (ax, pointsFile):
    """
    Adds points or triangles to the map
    """
    return ax

#==============================================================================
def addGridLines (ax):
    """
    Adds grid lines to the map
    """
    return ax

#==============================================================================
def ini (iniFile, local='tmp.mapfile.ini'):
    """
    Downloads/reads ini file with domain limits.
    Also check by autoval.validate.run.setDomainLimits
    """
    cs.oper.sys.msg('i','Using map limits from ' + iniFile)

    if iniFile.startswith('ftp') or iniFile.startswith('http'):
        cs.oper.transfer.refresh (iniFile, local)
    else:
        local = iniFile

    cfg = cs.oper.sys.config(local)
    lonlim = [ float(cfg['Limits']['lonmin']), float(cfg['Limits']['lonmax']) ]
    latlim = [ float(cfg['Limits']['latmin']), float(cfg['Limits']['latmax']) ]
    cs.oper.sys.msg('i','Lonlim: ' + str(lonlim[0]) + ' ' + str(lonlim[1]))
    cs.oper.sys.msg('i','Latlim: ' + str(latlim[0]) + ' ' + str(latlim[1]))

    return lonlim, latlim

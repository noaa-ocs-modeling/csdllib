"""
@author: Sergey.Vinogradov@noaa.gov
"""
import os
import urllib.request
import uuid
import ssl
from csdllib.oper.sys import msg
from pathlib import Path

import time
from functools import wraps


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    message = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(message)
                    else:
                        print(message)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

#==============================================================================
def download (remote, local):
    """
    Downloads remote file (using urllib2) if it does not exist locally.
    """
    if not os.path.exists(local):
        msg ('info','Downloading ' + remote + ' as ' + local)
        try:
            urllib.request.urlretrieve(remote, local)
        except:
            msg ('warn', 'file ' + remote + ' was not downloaded. trying to cp...')
            try:
                os.system('cp ' + remote + ' ' + local)
            except:
                msg ('warn', 'file ' + remote + ' could not be copied')
            
    else:
        msg('warn','file ' + local + ' exists, skipping.')

#==============================================================================
def refresh (remote, local):
    """
    Downloads remote file (using urllib2), overwrites local copy if exists.
    """
    if not os.path.exists(local):
        msg('info', 'downloading ' + remote + ' as ' + local)
    else:
        msg ('info', 'overwriting ' + local + ' file with ' + remote)
    try:
        urllib.request.urlretrieve(remote, local)
    except:
        msg('warn', 'file ' + remote + ' was not downloaded. trying to cp...')
        try:
            os.system('cp ' + remote + ' ' + local)
        except:
            msg('warn', 'file ' + remote + ' could not be copied')

#==============================================================================
def readlines (remote, verbose=False, tmpDir=None, tmpFile=None):
    """
    1. Downloads remote into temporary file
    2. Reads line by line
    3. Removes temporary file
    """
    
    if tmpFile is None:
        tmpFile  = str(uuid.uuid4()) + '.tmp' # Unique temporary name
    if tmpDir is not None:
        tmpFile = os.path.join(tmpDir, tmpFile)

    if verbose:
        msg('info','downloading ' + remote + ' as temporary ' + tmpFile)

    urllib.request.urlretrieve(remote, tmpFile)
    fp = open(tmpFile,errors='replace')
    lines  = fp.readlines()
    fp.close()
    os.remove( tmpFile )
            
    return lines

#==============================================================================
@retry(Exception, tries=100, delay=10, backoff=1)
def readlines_ssl (remote, verbose=False, tmpDir=None, tmpFile=None):
    """
    Deals with expired SSL certificate issue.
    1. Downloads remote into temporary file
    2. Reads line by line
    3. Removes temporary file
    """
    lines = []

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE 
 
    if tmpFile is None:
        tmpFile  = str(uuid.uuid4()) + '.tmp'
    if tmpDir is not None:
        tmpFile = os.path.join(tmpDir, tmpFile)

    if verbose:
        msg ('info', 'downloading ' + remote + ' as temporary ' + tmpFile)

    try:
        urllib.request.urlretrieve(remote, tmpFile)
    except:
        msg ('error', 'Cannot download ' + remote)       

    fp = open(tmpFile,errors='replace')
    lines  = fp.readlines()
    os.remove( tmpFile )
    fp.close()

    return lines

#==============================================================================
def upload(localFile, userHost, remoteFolder):
    #Remove the old Files before copying
    remoteFile = Path(localFile).name
    remoteFilePath = os.path.join(remoteFolder, remoteFile)
    cmd = 'ssh -q ' + userHost + " 'rm -rf " + remoteFilePath + "'"
    if os.system(cmd) == 0:
        msg('info', 'executed ' + cmd)
    else:
        msg('error', 'failed to execute ' + cmd)

    #Copy the new Files
    cmd = 'scp -q ' + localFile + ' ' + userHost + ':' + remoteFolder
    if os.system(cmd) == 0:
        msg('info', 'executed ' + cmd)
    else:
        msg('error', 'failed to execute ' + cmd)
        
#==============================================================================
def cleanup (tmpDir='.', tmpExt='.tmp'):
    """
    Removes files with extension tmpExt from the tmpDir.
    """
    files = os.listdir(tmpDir)
    for file in files:
        if file.endswith(tmpExt):
            os.remove(os.path.join(tmpDir,file))

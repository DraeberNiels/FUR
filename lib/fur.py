# Python3 built-in modules
import os
import time
import threading
from ftplib import FTP_TLS
from datetime import datetime
from multiprocessing import Process

# External modules
import h5py
import pysftp
import numpy as np

def _get_stamp():
    today = datetime.now()
    stamp = today.strftime("%b-%d-%Y-%H_%M_%S")
    return stamp

def _create_hdf5(location, stamp, chunks, chunk_row, dtype, col_size, compression):
    """
    Creates HDF5 file for appending upload method.
    """
    f = h5py.File('{}{}.h5'.format(location,stamp), 'w')

    if compression != 'gzip':
        compression = None
    if dtype == 'object':
        if chunks != ():
            dset = f.create_dataset('data', shape=(0,), chunks=(chunks[0],), dtype=dtype, maxshape=(None,), compression=compression)
        else:
            dset = f.create_dataset('data', shape=(0,), chunks=(chunk_row,), dtype=dtype, maxshape=(None,), compression=compression)
    else:
        if chunks == ():
            chunks = (chunk_row,col_size)
        dset = f.create_dataset('data', shape=(0,col_size), chunks=chunks, dtype=dtype, maxshape=(None,col_size),compression=compression)
    f.close()
    print("Created initial HDF5 file")

def _generate_hdf5_seq(counter, package_info, dtype, chunks, compression, location, name, extension, stamp, removal):
    """
    Creates HDF5 file for separate hdf5 uploading method.
    """
    start_file, end_file, fill_len = package_info

    # Packages file data into list.
    data = []

    for i in range(start_file,end_file):

        file_name = "{}{}{}.{}".format(location, name, str(i).zfill(fill_len) ,extension)

        if dtype == 'object':
            fin = open(file_name, 'rb')
            binary_data = fin.read()
            data.append(np.frombuffer(binary_data, dtype='uint8'))

        else:
            data.append(np.fromfile(file_name, dtype=dtype, sep=""))

        if removal:
            os.remove(file_name)
    
    # Specify compression filter and dtype of dataset.
    if compression != 'gzip':
        compression = None

    file_name = '{}{}-{}.h5'.format(location,counter,stamp)
    f = h5py.File(file_name, 'w')

    if dtype == 'object':
        dtype = h5py.special_dtype(vlen=np.dtype('uint8'))
    
    else:
        data = np.array(data, dtype)

    # Create dataset with data from list
    if len(chunks) < 1:

        if dtype == 'object':
            dset = f.create_dataset('data', shape=(len(data),), dtype=dtype, compression=compression)
            f['data'][:] = data[:]

        else:
            dset = f.create_dataset('data', data=data, dtype=dtype, compression=compression)

    else:
        dset = f.create_dataset('data', data=data, chunks=chunks, dtype=dtype, compression=compression)

    f.close()

    return file_name

def _upload_ftps(connection, file_name, removal):
    """
    Uploads the given filename with the specified connection.
    Allows for cleanup of specified file.
    """

    print("Uploading {} to cloud".format(file_name))
 
    t1 = time.time()

    f = open(file_name,'rb')
    remote_name = (file_name.split("/")[-1])
    connection.storbinary('STOR {}'.format(remote_name), f)
    f.close()
    
    # if removal paramter is set to true or it is an hdf5 file then remove
    if removal or file_name[-2:] == "h5":
        os.remove(file_name)

    t2 = time.time()

    print("Time to upload & delete file: ", t2-t1)

def _upload_sftp(connection, file_name, removal):
    """
    Uploads the given filename with the specified connection.
    Allows for cleanup of specified file.
    """
    
    print("Uploading {} to cloud".format(file_name))
 
    t1 = time.time()

    connection.put(file_name)
    
    if removal or file_name[-2:] == "h5":
        os.remove(file_name)

    t2 = time.time()

    print("Time to upload & delete file: ", t2-t1)

def _create_dir_ftps(ftps,path):
    """
    Create the directories specified in path with the given FTPS connection
    """
    if len(path) != 0:

        try:
            ftps.cwd(path)

        except:
            _create_dir_ftps(ftps, "/".join(path.split("/")[:-1]))
            ftps.mkd(path)
            ftps.cwd(path)

    return ftps

def _create_conn_ftps(host, username, password, path):
    """
    Create FTPS connection with ftplib and create/navigate to path
    """

    # initialise FTPS connection to host
    ftps = FTP_TLS(host)
    # insert ERDA credentials here
    ftps.login(username, password)           # login before securing control channel
    ftps.prot_p()
    # navigate to path and create dirs
    ftps = _create_dir_ftps(ftps,path)
    return ftps

def _create_conn_sftp(host, username, password, path):

    """
    Create SFTP connection with pysftp and create/navigate to path
    """
    # initialise SFTP connection to host
    sftp = pysftp.Connection(host, username=username,
        password=password)
    channel = sftp.sftp_client.get_channel()
    channel.settimeout(10)
    # channel.out_window_size += 21203488
    try:
        sftp.cwd(path)  # Test if remote_path exists

    except IOError:
        sftp.makedirs(path)
        sftp.cwd(path)

    return sftp

def _inspect_initial_file(initial_file, batch_size, dtype):
    """
    Inspects initial data file.
    Returns shapes for size and chunking and dtype,
    that will all be used for creating hdf5 files
    """

    initial_data = np.fromfile(initial_file)

    if dtype == 'binary':

        colsize = 0
        dtype = h5py.special_dtype(vlen=np.dtype('uint8'))
        chunk_row = batch_size

    else:

        initial_data = np.fromfile(initial_file)
        colsize = initial_data.shape[0]
        bytesize = (os.stat(initial_file).st_size)/colsize

        if dtype == 'int' or dtype=='float':
            
            dtype += str(int(bytesize))

        initial_data = np.fromfile(initial_file, dtype=dtype)
        colsize = initial_data.shape[0]
        
        ## constant works well for uploads to IDMC/ERDA
        ## look into setting this constant for optimal use
        chunk_row = max(int(300000 / os.stat(initial_file).st_size),1)

    return (colsize,chunk_row, dtype)

def _buffer_data(location, name, extension, file_num, batch_size, file_num_len, dtype, removal):
    """
    Buffers data into a list of numpy arrays
    Has possiblity of removing files once buffered
    Returns the list of data
    """

    data = []

    for i in range(1,(batch_size+1)):

        file_num_i = (str(int(file_num) - batch_size + i).zfill(file_num_len))
        file_name = "{}{}{}.{}".format(location, name, file_num_i,extension)

        if dtype == 'object':

            fin = open(file_name, 'rb')
            binary_data = fin.read()
            data.append(np.frombuffer(binary_data, dtype='uint8'))

        else:
            data.append(np.fromfile(file_name, dtype=dtype, sep=""))

        if removal:
            os.remove(file_name)

    return data

def _upload_thread(connection, protocol, removal, stamp, buffer, b_lock, stop):
    """
    Function for uploading data buffered by a runner_simple and runner_hdf5 functions.
    debuffers file names from buffer and uploads them seperately 
    with the specified connection
    """
    print ("Upload thread initiated")

    while((len(stop) < 1) or
        (len(buffer) > 1)):
        
        if len(buffer) > 0:

            file_name = buffer[0]

            globals()["_upload_{}".format(protocol)](connection, file_name, removal)

            b_lock.acquire()
            buffer.pop(0)
            b_lock.release()

        time.sleep(0.1)
    
    if len(buffer) > 0:

            file_name = buffer[0]

            globals()["_upload_{}".format(protocol)](connection, file_name, removal)

            b_lock.acquire()
            buffer.pop(0)
            b_lock.release()
    connection.close()
    print ("Terminating upload thread")

def _upload_thread_append(connection,location, stamp, dtype, batch_size, buffer,b_lock,stop):
    """
    Function for uploading data buffered by runner_hdf5_append.
    debuffers file names from buffer and uploads them seperately 
    with the specified connection
    """

    print ("Upload thread initiated")

    #handle initial upload
    file_name = '{}.h5'.format(stamp)

    while(not(os.path.exists(location + file_name))):
        time.sleep(0.1)

    print("Uploading and deleting initial HDF5 file")
    one = time.time()
    connection.put(location + file_name)
    os.remove(location + file_name)
    two = time.time()
    
    print("Took " + str(two-one) + " for initial upload and delete")

    # Open remote file objects with pysftp and h5py
    with connection.open(file_name,'r+',32768) as f:
        f.set_pipelined()
        
        with h5py.File(f,'r+') as h5:
            dset = h5['data']

            # Wait for data to be buffered
            while((len(stop) < 1) or
                (len(buffer) > 1)):

                if (len(buffer) > 0):

                    t1 = time.time()
                    # load data into memory and delete .npy file
                    data_name = buffer[0]
                    data = np.load(data_name,allow_pickle=True)
                    os.remove(data_name)

                    b_lock.acquire()
                    buffer.pop(0)
                    b_lock.release()
                    
                    # resize remote dataset
                    dset.resize(dset.shape[0]+data.shape[0], axis = 0)
                    
                    # append data to remote dataset
                    if dtype == 'object':

                        f.set_pipelined()
                        dset[-data.shape[0]:-1] = data[:-1]
                        f.set_pipelined(False)
                        dset[-1] = data[-1]
                        h5.flush()

                    else:
                        dset[-data.shape[0]:] = data
                    t2 = time.time()
                    print("Took : " + str(t2-t1) + " seconds to upload batch " +  str(int((dset.shape[0]-batch_size)/(batch_size))))
                time.sleep(0.01)
        
            # If buffer still contains last
            if (len(buffer) > 0):

                    t1 = time.time()
                    data_name = buffer[0]

                    data = np.load(data_name,allow_pickle=True)
                    os.remove(data_name)
                    b_lock.acquire()
                    buffer.pop(0)
                    b_lock.release()

                    # resize remote dataset
                    dset.resize(dset.shape[0]+data.shape[0], axis = 0)
                    
                    # append data to remote dataset
                    if dtype == 'object':

                        f.set_pipelined()
                        dset[-data.shape[0]:-1] = data[:-1]
                        f.set_pipelined(False)
                        dset[-1] = data[-1]
                        h5.flush()

                    else:
                        dset[-data.shape[0]:] = data
                    printer = ((dset.shape[0])/(batch_size))
                    t2 = time.time()
                    print("Took : " + str(t2-t1) + " seconds to upload batch " +  str(printer))
    print ("Terminating upload thread")

def runner_simple(host, username, password, path,
    location, name, extension, start_number, n, protocol="ftps", removal = False):
    """
    runner function for uploading files as-is
    initiates connection with specified protocol
    spawns upload function in new thread
    buffers filenames of file ready for transport

    Input parameters:

    host : String of hostname, e.g. "io.erda.dk"
    username, password : Strings of Username and Password for SFTP/FTPS 
    path : String of path where data should be stored on host
    location : String of where local data files are stored. 
      Should be defined in relation to current path is being run.
    name : String of local file name up until numbering .
        For "image1.jpeg" it would be "image"
    extension: String of file extension.
        For "image1.jpeg" it would be "jpeg"
    start_number : String of what file number should be initial file
        For "image1.jpeg" it would be "1"
        For "image0001.jpeg" it would be "0001"
    n : Integer of number of datafiles to be handled
    protocol : String of protocol. Choices are "sftp" and "ftps". 
        Optional argument, default is "ftps"
    removal : Boolean, if True then local data files are removed after upload.
        Optional argument, default to False meaning no cleanup is performed 
    """

    # simple input tests
    assert(isinstance(start_number, str))
    assert(int(start_number) >= 0)
    assert(protocol == "sftp" or protocol == "ftps")
    assert((len(name) > 0) and (len(extension) > 0))

    assert(batch_size > 0)
    assert(size > 0)

    # initiate connection
    connection = globals()["_create_conn_{}".format(protocol)](host,username,password,path)
    # start upload thread 

    # Datetime stamp for hdf5 file name
    stamp = _get_stamp()

    # setup buffer and lock
    buffer = []
    b_lock = threading.Lock()
    stop = []
    
    location += '/'

    # start upload function in new thread 
    up_thread = threading.Thread(target=_upload_thread, args=(connection, protocol, removal, stamp, buffer, b_lock, stop))
    up_thread.start()
    
    counter = 0
    file_num = start_number

    # buffer files for upload thread
    while((counter < n)):
        
        file_name = '{}{}{}.{}'.format(location, name, file_num, extension)

        if(os.path.exists(file_name)):

            b_lock.acquire()
            buffer.append(file_name)
            b_lock.release()

            counter += 1
            
            file_num = str(int(file_num) + 1).zfill(len(file_num))

        time.sleep(0.001)

    # inform upload thread that buffering is complete
    stop.append(1)
    print("Buffering Done")
    print('Waiting for upload thread to finish upload')
    up_thread.join()
    print("Ending Process for hdf5 upload")

def runner_hdf5(host, username, password, path,
    location, name, extension,start_number, size, batch_size, dtype, chunks=(), compression = None, protocol="ftps", removal = False):
    """
    runner function for uploading batches formatted in HDF5
    initiates connection with specified protocol
    spawns upload function in new thread
    packages batches in HDF5 format
    buffers filenames of HDF5 files ready for upload

    Input parameters:

    host : String of hostname, e.g. "io.erda.dk"
    username, password : Strings of Username and Password for SFTP/FTPS 
    path : String of path where data should be stored on host
    location : String of where local data files are stored. 
      Should be defined in relation to current path is being run.
    name : String of local file name up until numbering .
        For "image1.jpeg" it would be "image"
    extension: String of file extension.
        For "image1.jpeg" it would be "jpeg"
    start_number : String of what file number should be initial file
        For "image1.jpeg" it would be "1"
        For "image0001.jpeg" it would be "0001"
    size : Integer of total amount of datafiles to be handled
    batch_size : Integer of batch size used for packaging and uploads.
    dtype : String of data type in. e.g "int8" , "float16".
        Use "binary" for binary data type such as e.g JPEG that is not homogoneously shaped
    chunks : Tuple of wanted chunk size. The shape of the entire dataset should
        be a multiple of the chunking shape. Optional argument, defaults to framework deciding chunks.
    compression : String of compression. If GZIP compression is wanted
        set to "gzip", else do not specify. Oprional argument, default is no compression.
    protocol : String of protocol. Choices are "sftp" and "ftps". 
        optional argument, default is "ftps"
    removal : Boolean, if True then local data files are removed after upload.
        Optional argument, default to False meaning no cleanup is performed 
    """

    print ("Starting Process for Packaging and Uploading")

    # simple input tests
    assert(isinstance(start_number, str))
    assert(int(start_number) >= 0)
    assert(batch_size > 0)
    assert(size > 0)

    assert(protocol == "sftp" or protocol == "ftps")
    assert((len(name) > 0) and (len(extension) > 0))
    assert(dtype in ['int', 'float', 'float16','float32','float64', 'binary'])

    connection = globals()["_create_conn_{}".format(protocol)](host,username,password,path)

    # Datetime stamp for hdf5 file name
    stamp = _get_stamp()

    # Setup shared buffer and lock
    buffer = []
    b_lock = threading.Lock()
    stop = []
    
    location += '/'

    # Wait for first dataentry
    initial_file = '{}{}{}.{}'.format(location,name,start_number,extension)
    while (not(os.path.exists(initial_file))):
        time.sleep(0.01)

    # Inspect initial file
    col_size , chunk_row, dtype = _inspect_initial_file(initial_file, batch_size, dtype)
    
    # Initiate and start upload function in new thread
    up_thread = threading.Thread(target=_upload_thread, args=(connection, protocol, removal, stamp, buffer, b_lock, stop))
    
    up_thread.start()

    start_number_len = len(start_number)    
    
    counter = 0

    # calculate which file is last in current batch
    file_num  = str(int(start_number) - 1 + batch_size).zfill(start_number_len)

    while counter < int(size/batch_size):
        
        if os.path.exists('{}{}{}.{}'.format(location, name, file_num, extension)):
            
            # generate HDF5 for batch
            start_file = counter*batch_size+int(start_number)
            package_info = (start_file, start_file + batch_size,start_number_len)

            file_name = _generate_hdf5_seq(counter, package_info, dtype, chunks, compression, location, name, extension, stamp, removal)

            # buffer data 
            b_lock.acquire()
            buffer.append(file_name)
            b_lock.release()
            
            # update which file is last in next batch
            file_num = str(int(file_num) + batch_size).zfill(start_number_len)
            counter += 1
            
        time.sleep(0.01)
    
    while((counter * batch_size) < size):

        file_num = str(size - 1 + int(start_number)).zfill(start_number_len)
        if(os.path.exists('{}{}{}.{}'.format(location, name, file_num, extension))):
            
            # generate HDF5 for batch
            start_file = counter*batch_size+int(start_number)
            package_info = (start_file, int(file_num)+1,start_number_len)

            file_name = _generate_hdf5_seq(counter, package_info, dtype, chunks, location, name, extension, stamp, removal)

            # buffer data
            b_lock.acquire()
            buffer.append(file_name)
            b_lock.release()

            counter += 1

        time.sleep(0.01)

    # inform upload thread that buffering is complete
    stop.append(1)
    print ("Packaging done")
    print('Waiting for upload thread to finish upload')
    up_thread.join()
    print ("Ending Process for hdf5 upload")

def runner_hdf5_append(host, username, password, path,
    location, name, extension, start_number, size, batch_size, dtype, chunks = (), compression = None, removal = False):
    """
    runner function for appending uploading batches to a remote HDF5 file
    initiates connection with specified protocol
    spawns upload function in new thread
    packages batches in .npy format for upload thread
    buffers filenames of npy files ready for upload

    Input parameters:

    host : String of hostname, e.g. "io.erda.dk"
    username, password : Strings of Username and Password for SFTP/FTPS 
    path : String of path where data should be stored on host
    location : String of where local data files are stored. 
      Should be defined in relation to current path is being run.
    name : String of local file name up until numbering .
        For "image1.jpeg" it would be "image"
    extension: String of file extension.
        For "image1.jpeg" it would be "jpeg"
    start_number : String of what file number should be initial file
        For "image1.jpeg" it would be "1"
        For "image0001.jpeg" it would be "0001"
    size : Integer of total amount of datafiles to be handled
    batch_size : Integer of batch size used for packaging and uploads.
    dtype : String of data type in. e.g "int8" , "float16".
        Use "binary" for binary data type such as e.g JPEG that is not homogoneously shaped
    chunks : Tuple of wanted chunk size. The shape of the entire dataset should
        be a multiple of the chunking shape. Optional argument, defaults to framework deciding chunks.
    compression : String of compression. If GZIP compression is wanted
        set to "gzip", else do not specify. Oprional argument, default is no compression.
    removal : Boolean, if True then local data files are removed after upload.
        Optional argument, default to False meaning no cleanup is performed 
    """

    print ("Starting Process for Appending hdf5 upload")

    # simple input tests
    assert(isinstance(start_number, str))
    assert(int(start_number) >= 0)
    assert(dtype in ['int', 'float', 'float16','float32','float64', 'binary'])
    assert(batch_size > 0)
    assert(size > 0)
    connection = _create_conn_sftp(host,username,password,path)
    
    # Datetime stamp for hdf5 file name
    stamp = _get_stamp()

    # Setup shared buffer and lock
    buffer = []
    b_lock = threading.Lock()
    stop = []
    
    location += '/'

    # Inspect initial file
    initial_file = '{}{}{}.{}'.format(location,name,start_number,extension)
    while (not(os.path.exists(initial_file))):
        time.sleep(0.01)

    col_size , chunk_row, dtype = _inspect_initial_file(initial_file, batch_size, dtype)

    # Initiate and start upload function in new thread
    upload_thread = threading.Thread(target=_upload_thread_append, args=(connection, location, stamp, dtype, batch_size, buffer,b_lock, stop))
    upload_thread.start()
    
    # Create initial HDF5 file which will be appended to
    _create_hdf5(location, stamp, chunks, chunk_row , dtype, col_size, compression)

    start_number_len = len(start_number)    
    counter = 0

    # calculate which file is last in current batch
    file_num  = str(int(start_number) - 1 + batch_size).zfill(start_number_len)

    while(counter < int(size/batch_size)): 

        if(os.path.exists('{}{}{}.{}'.format(location,name,file_num,extension))):

            # package data and save to .npy file
            data = _buffer_data(location ,name, extension, file_num, batch_size, start_number_len, dtype, removal)
            d_name = "data{}-{}.npy".format(counter,stamp)
            np.save(d_name,data)

            #buffer file name of packaged data
            b_lock.acquire()
            buffer.append(d_name)
            b_lock.release()
            counter += 1

            # update which file is last in next batch
            file_num = str(int(file_num) + batch_size).zfill(start_number_len)
        time.sleep(0.01)

    # buffer remaining data    
        
    while((counter * batch_size) < size):
        file_num = str(size - 1 + int(start_number)).zfill(start_number_len)

        if(os.path.exists('{}{}{}.{}'.format(location, name, file_num, extension))):
            
            # package remaining data and save to .npy
            done_no = counter*batch_size
            data = _buffer_data(location, name, extension, file_num, size - done_no, start_number_len, dtype, removal)
            d_name = "data{}-{}.npy".format(counter,stamp)
            np.save(d_name,data)
            
            #buffer file name of packaged data
            b_lock.acquire()
            buffer.append(d_name)
            b_lock.release()
            counter += 1
        time.sleep(0.01)

    # inform upload thread that buffering is complete
    stop.append(1)
    print('Waiting for upload thread to finish upload')
    upload_thread.join()
    print ("Ending Process for Appending hdf5 upload")

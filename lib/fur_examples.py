import fur
from threading import Thread
from multiprocessing import Process

# Example of using fur for HDF5 appending upload
def runner_hdf5_append_test():

    if __name__ == "__main__":
            # host : String of hostname, e.g. "io.erda.dk"
            host = ""
            # username, password : Strings of Username and Password for SFTP/FTPS 
            username, password = "" , ""
            # path : String of path where data should be stored on host
            path  = ""
            # location : String of where local data files are stored. 
            #   Should be defined in relation to current path is being run.
            location = ""
            # name : String of local file name up until numbering .
            #     For "image1.jpeg" it would be "image"
            name = ""
            # extension: String of file extension.
            #     For "image1.jpeg" it would be "jpeg"
            extension = ""
            # start_number : String of what file number should be initial file
            #     For "image1.jpeg" it would be "1"
            #     For "image0001.jpeg" it would be "0001"
            start_number = ""
            # size : Integer of total amount of datafiles to be handled
            size = 0
            # batch_size : Integer of batch size used for packaging and uploads.
            batch_size = 0
            # dtype : String of data type in. e.g "int8" , "float16".
            #     Use "binary" for binary data type such as e.g JPEG
            dtype = ""
            # chunks : Tuple of wanted chunk size. The shape of the entire dataset should
            #     be a multiple of the chunking shape. Optional argument, defaults to framework deciding chunks.
            chunks = ()
            # compression : String of compression. If GZIP compression is wanted
            #     set to "gzip", else do not specify. Oprional argument, default is no compression.
            compression = ""
            # removal : Boolean, if True then local data files are removed after upload.
            #     Optional argument, default to False meaning no cleanup is performed 
            removal = False

            # for starting in a new process
            p1 = Process(target=fur.runner_hdf5_append, 
                args=(host, username, password, path, location, name, extension, start_number,
                    size, batch_size, dtype, chunks, compression, removal))

            # remember to start and join thread/process    
            p1.start()
            p1.join()
            
            # for starting in a new thread
            p2 = Thread(target=fur.runner_hdf5_append, 
                args=(host, username, password, path, location, name, extension, start_number,
                        size, batch_size, dtype, chunks, compression, removal))

            # remember to start and join thread/process    
            p2.start()
            p2.join()

# Example of using fur for packaging data batches in HDF5
#   and uploading batches separately
def runner_hdf5_test():

    if __name__ == "__main__":
            # host : String of hostname, e.g. "io.erda.dk"
            host = ""
            # username, password : Strings of Username and Password for SFTP/FTPS 
            username, password = "" , ""
            # path : String of path where data should be stored on host
            path  = ""
            # location : String of where local data files are stored. 
            #   Should be defined in relation to current path is being run.
            location = ""
            # name : String of local file name up until numbering .
            #     For "image1.jpeg" it would be "image"
            name = ""
            # extension: String of file extension.
            #     For "image1.jpeg" it would be "jpeg"
            extension = ""
            # start_number : String of what file number should be initial file
            #     For "image1.jpeg" it would be "1"
            #     For "image0001.jpeg" it would be "0001"
            start_number = ""
            # size : Integer of total amount of datafiles to be handled
            size = 0
            # batch_size : Integer of batch size used for packaging and uploads.
            batch_size = 0
            # dtype : String of data type in. e.g "int8" , "float16".
            #     Use "binary" for binary data type such as e.g JPEG
            dtype = ""
            # chunks : Tuple of wanted chunk size. The shape of the entire dataset should
            #     be a multiple of the chunking shape. Optional argument, defaults to framework deciding chunks.
            chunks = ()
            # compression : String of compression. If GZIP compression is wanted
            #     set to "gzip", else do not specify. Oprional argument, default is no compression.
            compression = ""
            # protocol : String of protocol. Choices are "sftp" and "ftps". 
            #     optional argument, default is "ftps"
            protocol = "ftps"
            # removal : Boolean, if True then local data files are removed after upload.
            #     Optional argument, default to False meaning no cleanup is performed 
            removal = False

            # for starting in a new process
            p1 = Process(target=fur.runner_hdf5, 
                args=(host, username, password, path, location, name, extension, start_number,
                        size, batch_size, dtype, chunks, compression, protocol, removal))

            # remember to start and join thread/process    
            p1.start()
            p1.join()

# Example of using fur for uploading data files as-is one at a time
def runner_simple_test():

    if __name__ == "__main__":
            # host : String of hostname, e.g. "io.erda.dk"
            host = ""
            # username, password : Strings of Username and Password for SFTP/FTPS 
            username, password = "" , ""
            # path : String of path where data should be stored on host
            path  = ""
            # location : String of where local data files are stored. 
            #   Should be defined in relation to current path is being run.
            location = ""
            # name : String of local file name up until numbering .
            #     For "image1.jpeg" it would be "image"
            name = ""
            # extension: String of file extension.
            #     For "image1.jpeg" it would be "jpeg"
            extension = ""
            # start_number : String of what file number should be initial file
            #     For "image1.jpeg" it would be "1"
            #     For "image0001.jpeg" it would be "0001"
            start_number = ""
            # n : Integer of total amount of datafiles to be handled
            n = 0
            # protocol : String of protocol. Choices are "sftp" and "ftps". 
            #     optional argument, default is "ftps"
            protocol = "ftps"
            # removal : Boolean, if True then local data files are removed after upload.
            #     Optional argument, default to False meaning no cleanup is performed 
            removal = False

            # for starting in a new process
            p1 = Process(target=fur.runner_simple, 
                args=(host, username, password, path, location, name, extension, start_number,
                        n, protocol, removal))

            # remember to start and join thread/process    
            p1.start()
            p1.join()

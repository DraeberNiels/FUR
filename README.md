# Framework for Upload of Research data (FUR)

FUR is a Python 3 library for uploading data to remote storage solutions in the HDF5 format via SFTP or FTPS.
The FUR library focuses on uploading and packaging data contiously. Furthermore, it allows for cleanup of local
data once it has been uploaded to the specified remote storage solution.

Its functionality is intended to run concurrently with the process of data capturing in a separate process or thread.
This allows FUR to utilize time during data capture and reduce excessively storing data that has already been packaged or uploaded.

FUR provides three functions for uploading data:

1. runner_hdf5_append:
  Used for append uploading data to a single remote HDF5 file using SFTP. Cleanup of local files once uploaded can be enabled.
  Intermediate npy files for buffering the data are always deleted after upload.

2. runner_hdf5:
  Used for gathering and packaging data into local HDF5 files, creating one HDF5 file per batch locally and uploading them.
  Uploads are done through either SFTP or FTPS. Intermediate HDF5 files are always deleted after upload.

3. runner_simple:
  Used for gathering data files and uploading them as-is to designated remote location on a server through either SFTP or FTPS.

An example for each FUR functionality of how it should be called along with parameter explanations can be found in lib/fur_examples.py

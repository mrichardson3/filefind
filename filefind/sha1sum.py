import hashlib

def chunk_reader(fobj, chunk_size=4096):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk

 
def sha1sum(full_path, hash=hashlib.sha1):
    """ Given a filename, return its sha1 sum
    """
    hashobj = hash()
    for chunk in chunk_reader(open(full_path, 'rb')):
        hashobj.update(chunk)
    return hashobj.hexdigest()


import os
import struct


def oid_generated_on_client(oid):
    """Is this process's PID in this ObjectId?"""
    pid_from_doc = struct.unpack('>H', oid.binary[7:9])[0]
    return (os.getpid() % 0xFFFF) == pid_from_doc

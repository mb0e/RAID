import fsconfig
import logging
from block import *
from inode import *
from inodenumber import *
from filename import *

## This class implements methods for file operations

class FileOperations():
    def __init__(self, FileNameObject):
        self.FileNameObject = FileNameObject

    ## Create an object in the file system
    ## name is the string name of the object to be created
    ## type is its type
    ## dir is the inode of the directory where it is to be bound to
    ## This function returns two values: an integer status (0=success, -1=error) and a string message

    def Create(self, dir, name, type):
        logging.debug("FileOperations::Create: dir: " + str(dir) + ", name: " + str(name) + ", type: " + str(type))

        # Ensure type is valid, otherwise return
        if not (type == fsconfig.INODE_TYPE_FILE or type == fsconfig.INODE_TYPE_DIR):
            logging.debug("ERROR_CREATE_INVALID_TYPE " + str(type))
            return -1, "ERROR_CREATE_INVALID_TYPE"

        # Find if there is an available inode
        inode_position = self.FileNameObject.FindAvailableInode()
        if inode_position == -1:
            logging.debug("ERROR_CREATE_INODE_NOT_AVAILABLE")
            return -1, "ERROR_CREATE_INODE_NOT_AVAILABLE"

        # Obtain dir_inode_number_inode, ensure it is a directory
        dir_inode = InodeNumber(dir)
        dir_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        if dir_inode.inode.type != fsconfig.INODE_TYPE_DIR:
            logging.debug("ERROR_CREATE_INVALID_DIR " + str(dir))
            return -1, "ERROR_CREATE_INVALID_DIR"

        # Find available slot in directory data block
        fileentry_position = self.FileNameObject.FindAvailableFileEntry(dir)
        if fileentry_position == -1:
            logging.debug("ERROR_CREATE_DATA_BLOCK_NOT_AVAILABLE")
            return -1, "ERROR_CREATE_DATA_BLOCK_NOT_AVAILABLE"

        # Ensure it's not a duplicate - if Lookup returns anything other than -1
        if self.FileNameObject.Lookup(name, dir) != -1:
            logging.debug("ERROR_CREATE_ALREADY_EXISTS " + str(name))
            return -1, "ERROR_CREATE_ALREADY_EXISTS"

        logging.debug("FileOperations::Create: inode_position: " + str(inode_position) + ", fileentry_position: " + str(fileentry_position))

        if type == fsconfig.INODE_TYPE_DIR:
            # We're creating a new directory (e.g. mkdir)
            # First, create an appropriate inode object in memory for this new directory we're creating
            newdir_inode = InodeNumber(inode_position)
            newdir_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
            newdir_inode.inode.type = fsconfig.INODE_TYPE_DIR
            # it starts with size 0 and refcnt 1
            newdir_inode.inode.size = 0
            newdir_inode.inode.refcnt = 1
            # Allocate one data block and set as first entry in block_numbers[]
            newdir_inode.inode.block_numbers[0] = self.FileNameObject.AllocateDataBlock()
            # Store this inode object back into the inode table in raw storage
            newdir_inode.StoreInode(self.FileNameObject.RawBlocks)

            # Now need to create a new binding for (filename,inode) in the directory table
            # Add to directory (filename,inode) table
            self.FileNameObject.InsertFilenameInodeNumber(dir_inode, name, inode_position)

            # Also add binding for "." to new directory bound to itself
            self.FileNameObject.InsertFilenameInodeNumber(newdir_inode, ".", inode_position)

            # Add add binding ".." to new directory bound to parent
            self.FileNameObject.InsertFilenameInodeNumber(newdir_inode, "..", dir)

            # Update directory inode
            # increment refcnt
            dir_inode.inode.refcnt += 1
            dir_inode.StoreInode(self.FileNameObject.RawBlocks)

        elif type == fsconfig.INODE_TYPE_FILE:
            # we're creating a regular file here (e.g. create)
            # First, create an appropriate inode object in memory for this new directory we're creating
            newfile_inode = InodeNumber(inode_position)
            newfile_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
            newfile_inode.inode.type = fsconfig.INODE_TYPE_FILE
            newfile_inode.inode.size = 0
            newfile_inode.inode.refcnt = 1
            # Unlike DIRs, for FILES they are not allocated a block upon creatin; these are allocated on a Write()
            newfile_inode.StoreInode(self.FileNameObject.RawBlocks)

            # Add to parent's (filename,inode) table
            self.FileNameObject.InsertFilenameInodeNumber(dir_inode, name, inode_position)

            # Update directory inode
            # refcnt incremented by one
            dir_inode.inode.refcnt += 1
            dir_inode.StoreInode(self.FileNameObject.RawBlocks)

        # Return new object's inode number
        return inode_position, "SUCCESS"


    ## Writes data to a file, starting at offset
    ## offset must be less than or equal to the file's size
    ## data is a bytearray
    ## returns number of bytes written

    def Write(self, file_inode_number, offset, data):

        logging.debug(
            "FileOperations::Write: file_inode_number: " + str(file_inode_number) + ", offset: " + str(offset) + ", len(data): " + str(
                len(data)))
        # logging.debug (str(data))

        file_inode = InodeNumber(file_inode_number)
        file_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

        # perform checks on type and bounds
        if file_inode.inode.type != fsconfig.INODE_TYPE_FILE:
            logging.debug("ERROR_WRITE_NOT_FILE " + str(file_inode_number))
            return -1, "ERROR_WRITE_NOT_FILE"

        if offset > file_inode.inode.size:
            logging.debug("ERROR_WRITE_OFFSET_LARGER_THAN_SIZE " + str(offset))
            return -1, "ERROR_WRITE_OFFSET_LARGER_THAN_SIZE"

        if offset + len(data) > fsconfig.MAX_FILE_SIZE:
            logging.debug("ERROR_WRITE_EXCEEDS_FILE_SIZE " + str(offset + len(data)))
            return -1, "ERROR_WRITE_EXCEEDS_FILE_SIZE"

        # initialize variables used in the while loop
        # current_offset keeps track of the current offset where data is going to be written
        # start from the requested offset argument
        current_offset = offset
        # bytes_written keeps track of the total number of bytes written
        # start with zero
        bytes_written = 0

        # the data to be written may span multiple blocks
        # this loop iterates through one or more blocks, ending when all data is written
        while bytes_written < len(data):
            # determine block index corresponding to the current offset where the write should take place
            current_block_index = current_offset // fsconfig.BLOCK_SIZE

            # determine the next block's boundary (in Bytes relative to the file's offset 0)
            next_block_boundary = (current_block_index + 1) * fsconfig.BLOCK_SIZE

            logging.debug('FileOperations::Write: current_block_index: ' + str(current_block_index) + ' , next_block_boundary: ' + str(
                next_block_boundary))

            # byte position where the slice of data to write should start, within a block
            # we use modulo arithmetic
            # the first time around in the loop, this may not be aligned with block boundary (i.e. 0) depending on offset
            # in subsequent iterations of this loop, it will always be 0
            write_start = current_offset % fsconfig.BLOCK_SIZE

            # determine byte position where the writing ends
            # this may be BLOCK_SIZE if the data yet to be written spills over to the next block
            # or, it may be smaller than BLOCK_SIZE if the data ends in this bloc
            if (offset + len(data)) >= next_block_boundary:
                # the data length is such that it goes beyond this block, so we're writing this entire block
                write_end = fsconfig.BLOCK_SIZE
            else:
                # otherwise, the data is truncated within this block
                write_end = (offset + len(data)) % fsconfig.BLOCK_SIZE

            logging.debug('FileOperations::Write: write_start: ' + str(write_start) + ' , write_end: ' + str(write_end))

            # retrieve index of block to be written from inode's list
            block_number = file_inode.inode.block_numbers[current_block_index]

            # if the data block to be written is not allocated (i.e. the block_numbers list in the inode is zero at
            # current_block_index), we need to allocate it
            if block_number == 0:
                # allocate new data block
                new_block = self.FileNameObject.AllocateDataBlock()
                # update inode's block number list (it will be written to raw storage before the method returns)
                file_inode.inode.block_numbers[current_block_index] = new_block
                block_number = new_block

            # now we have either an existing block, or a newly allocated one
            # either way, first, we read the whole block from raw storage
            # (if it's a newly allocated block, it's full of zeroes)
            block = self.FileNameObject.RawBlocks.Get(block_number)

            # copy slice of data into the right position in this block
            block[write_start:write_end] = data[bytes_written:bytes_written + (write_end - write_start)]

            # now write modified block back to disk
            self.FileNameObject.RawBlocks.Put(block_number, block)

            # update offset, bytes written
            current_offset += write_end - write_start
            bytes_written += write_end - write_start

            logging.debug('FileOperations::Write: current_offset: ' + str(current_offset) + ' , bytes_written: ' + str(
                bytes_written) + ' , len(data): ' + str(len(data)))

        # Update inode's metadata to increment size by bytes_written, and write inode back to inode table in raw storage
        file_inode.inode.size = offset + bytes_written
        file_inode.StoreInode(self.FileNameObject.RawBlocks)

        return bytes_written, "SUCCESS"

    ## Reads data from a file, starting at offset
    ## offset must be less than or equal to the file's size
    ## returns a bytearray with the data read, if successful

    def Read(self, file_inode_number, offset, count):
        logging.debug("FileOperations::Read: file_inode_number: " + str(file_inode_number) + ", offset: " + str(offset) + ", count: " + str(count))

        file_inode = InodeNumber(file_inode_number)
        file_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

        # type and bounds check
        if file_inode.inode.type != fsconfig.INODE_TYPE_FILE:
            logging.debug("ERROR_READ_NOT_FILE " + str(file_inode_number))
            return -1, "ERROR_READ_NOT_FILE"

        if offset > file_inode.inode.size:
            logging.debug("ERROR_READ_OFFSET_LARGER_THAN_SIZE " + str(offset))
            return -1, "ERROR_READ_OFFSET_LARGER_THAN_SIZE"

        # initialize variables used in the while loop
        current_offset = offset
        bytes_read = 0

        # make sure we don't read past file's size
        if offset + count > file_inode.inode.size:
            bytes_to_read = file_inode.inode.size - offset
        else:
            bytes_to_read = count

        read_data = bytearray(bytes_to_read)

        # this loop iterates through one or more blocks, ending when all data is read
        while bytes_read < bytes_to_read:

            # block index corresponding to the current offset
            current_block_index = current_offset // fsconfig.BLOCK_SIZE

            # next block's boundary (in Bytes relative to file 0)
            next_block_boundary = (current_block_index + 1) * fsconfig.BLOCK_SIZE

            logging.debug('FileOperations::Read: current_block_index: ' + str(current_block_index) + ' , next_block_boundary: ' + str(
                next_block_boundary))

            read_start = current_offset % fsconfig.BLOCK_SIZE

            if (offset + bytes_to_read) >= next_block_boundary:
                # the data length is such that it goes beyond this block, so we're reading this entire block
                read_end = fsconfig.BLOCK_SIZE
            else:
                # otherwise, the data is truncated within this block
                read_end = (offset + bytes_to_read) % fsconfig.BLOCK_SIZE

            logging.debug('FileOperations::Read: read_start: ' + str(read_start) + ' , read_end: ' + str(read_end))

         # retrieve index of block to be written from inode's list
            block_number = file_inode.inode.block_numbers[current_block_index]

            # first, we read the whole block from raw storage
            block = self.FileNameObject.RawBlocks.Get(block_number)

            # copy slice of data from block into the right position in the read_block
            read_data[bytes_read:bytes_read + (read_end - read_start)] = block[read_start:read_end]

            bytes_read += read_end - read_start
            current_offset += read_end - read_start

            logging.debug('FileOperations::Read: current_offset: ' + str(current_offset) + ' , bytes_read: ' + str(bytes_read))

        return read_data, "SUCCESS"


    ## Skeleton functions - you'll implement these in HW#2

#   def Slice(self, file_inode_number, offset, count):
#       logging.debug("FileOperations::Slice: file_inode_number: " + str(file_inode_number) + ", offset: " + str(offset) + ", count: " + str(count))
#        return -1, "Not implemented"

#    def Mirror(self, file_inode_number):
#        logging.debug("FileOperations::Mirror: file_inode_number: " + str(file_inode_number))
#        return -1, "Not implemented"

#    def Unlink(self, dir, name):
#        logging.debug("FileOperations::Unlink: dir: " + str(dir) + ", name: " + str(name))
#        return -1, "Not implemented"

# BEGIN_REMOVE_TO_DISTRIBUTE
    def Slice(self, file_inode_number, offset, count):
        logging.debug("FileOperations::Slice: file_inode_number: " + str(file_inode_number) + ", offset: " + str(offset) + ", count: " + str(count))
        file_inode = InodeNumber(file_inode_number)
        file_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        if offset > file_inode.inode.size:
            logging.debug("ERROR_SLICE_OFFSET_OUT_BOUNDS " + str(offset))
            return -1, "ERROR_SLICE_OFFSET_OUT_BOUNDS"
        if (offset+count) > file_inode.inode.size:
            logging.debug("ERROR_SLICE_COUNT_OUT_BOUNDS " + str(count+offset))
            return -1, "ERROR_SLICE_COUNT_OUT_BOUNDS"
        block_slice, status = self.Read(file_inode_number, offset, count)
        if status == -1:
            logging.debug("ERROR_SLICE_READ_ERROR")
            return -1, "ERROR_SLICE_READ_ERROR"
        bytes_written, message = self.Write(file_inode_number, 0, block_slice)
        return bytes_written, message

    def Mirror(self, file_inode_number):
        logging.debug("FileOperations::Mirror: file_inode_number: " + str(file_inode_number))
        file_inode = InodeNumber(file_inode_number)
        file_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        block_original, status = self.Read(file_inode_number, 0, file_inode.inode.size)
        if status == -1:
            logging.debug("ERROR_MIRROR_READ_ERROR")
            return -1, "ERROR_MIRROR_READ_ERROR"
        block_mirrored = bytearray(file_inode.inode.size)
        for i in range(0,file_inode.inode.size):
            block_mirrored[file_inode.inode.size-i-1] = block_original[i]
        bytes_written, message = self.Write(file_inode_number, 0, block_mirrored)
        return bytes_written, message


## Unlink a file
## dir is the inode number of current working directory
## name is the file's name

    def Unlink(self, dir, name):

        logging.debug ("FileOperations::Unlink: dir: " + str(dir) + ", name: " + str(name))

        # Obtain dir_inode_number_inode, ensure it is a directory
        dir_inode = InodeNumber(dir)
        dir_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
        if dir_inode.inode.type != fsconfig.INODE_TYPE_DIR:
          logging.debug ("ERROR_UNLINK_INVALID_DIR " + str(dir))
          return -1, "ERROR_UNLINK_INVALID_DIR"

        # Ensure file exists - if Lookup returns -1 it does not exist
        file_inode = self.FileNameObject.Lookup(name, dir)
        if file_inode == -1:
          logging.debug ("ERROR_UNLINK_DOESNOT_EXIST " + str(name))
          return -1, "ERROR_UNLINK_DOESNOT_EXIST"

        # Ensure it is a regular file
        file = InodeNumber(file_inode)
        file.InodeNumberToInode(self.FileNameObject.RawBlocks)
        if file.inode.type != fsconfig.INODE_TYPE_FILE:
          logging.debug ("ERROR_UNLINK_NOT_FILE " + str(name))
          return -1, "ERROR_UNLINK_NOT_FILE"

        # Step 1: Remove binding of inode in directory
        # We need to search all directory entries until we find a match with inode number

        block_index = 0
        # create a temporary directory table, large enough to hold the number of blocks for this directory
        tempdirtable = bytearray((dir_inode.inode.size // fsconfig.BLOCK_SIZE) * fsconfig.BLOCK_SIZE)
        # Scan through all possible blocks in the directory and Get() from raw blocks to build this table
        while block_index <= (dir_inode.inode.size // fsconfig.BLOCK_SIZE):
          # Read block from disk
          tempdirtable[block_index*fsconfig.BLOCK_SIZE:block_index*fsconfig.BLOCK_SIZE+fsconfig.BLOCK_SIZE-1] = self.FileNameObject.RawBlocks.Get(dir_inode.inode.block_numbers[block_index])
          block_index += 1
        logging.debug("FileOperations::Unlink: tempdirtable: " + str(tempdirtable.hex()))
        # pad the filename with zeroes for comparison
        padded_filename = bytearray(name, "utf-8")
        padded_filename = bytearray(padded_filename.ljust(fsconfig.MAX_FILENAME, b'\x00'))
        # bounds for searching for a match
        current_position = 0
        end_position = dir_inode.inode.size * fsconfig.FILE_NAME_DIRENTRY_SIZE
        # Now scan within the block
        while current_position < end_position:
          # Retrieve bytearray slice with name for this binding
          entryname = tempdirtable[current_position:current_position+fsconfig.MAX_FILENAME]
          entryname_padded = bytearray(entryname.ljust(fsconfig.MAX_FILENAME,b'\x00'))
          if entryname_padded == padded_filename:
            logging.debug("FileOperations::Unlink: found a match " + str(padded_filename))
            # found the entry to be removed - inode matches
            break
          else:
            current_position += fsconfig.FILE_NAME_DIRENTRY_SIZE
        # we'll now shift entries through the end of the directory list here
        while current_position < end_position:
          tempdirtable[current_position:current_position+fsconfig.FILE_NAME_DIRENTRY_SIZE] = tempdirtable[current_position+fsconfig.FILE_NAME_DIRENTRY_SIZE:current_position + 2*fsconfig.FILE_NAME_DIRENTRY_SIZE]
          current_position += fsconfig.FILE_NAME_DIRENTRY_SIZE
        # now we save directory table blocks back to disk
        block_index = 0
        while block_index <= (dir_inode.inode.size // fsconfig.BLOCK_SIZE):
          logging.debug("FileOperations::Unlink: writing back block_index " + str(block_index))
          self.FileNameObject.RawBlocks.Put(dir_inode.inode.block_numbers[block_index],tempdirtable[block_index*fsconfig.BLOCK_SIZE:block_index*fsconfig.BLOCK_SIZE+fsconfig.BLOCK_SIZE])
          block_index += 1
        # now update size and refcnt of directory inode and commit back to rawblocks
        logging.debug("FileOperations::Unlink: updating dir_inode")
        dir_inode.inode.size = dir_inode.inode.size - fsconfig.FILE_NAME_DIRENTRY_SIZE
        dir_inode.inode.refcnt -= 1
        dir_inode.StoreInode(self.FileNameObject.RawBlocks)

        # Step 2: decrement refcnt of file being unlinked
        file.inode.refcnt -= 1
        file.StoreInode(self.FileNameObject.RawBlocks)

        # Step 3: if it's the last binding (refcnt==0), Free data block resources, then free inode
        if file.inode.refcnt == 0:
          logging.debug ("FileOperations::Unlink: last reference; freeing data blocks and inode")

          # Free data blocks one by one by marking them as FREE (0) in the free bitmap
          # Scan all blocks in inode
          for i in range(0,fsconfig.MAX_INODE_BLOCK_NUMBERS):
            block_number = file.inode.block_numbers[i]
            # if the block is allocated i.e. != 0, free it up in bitmap
            if block_number != 0:
              # print ("TBD: free block " + str(block_number))
              # index bitmap block
              bitmap_block = fsconfig.FREEBITMAP_BLOCK_OFFSET + (block_number // fsconfig.BLOCK_SIZE)
              # retrieve bitmap from disk
              block = self.FileNameObject.RawBlocks.Get(bitmap_block)
              # Update bitmap entry for block_number to be 0 (free)
              block[block_number % fsconfig.BLOCK_SIZE] = 0
              # Write back to disk
              self.FileNameObject.RawBlocks.Put(bitmap_block,block)

          # Free inode
          # Create new inode that is blank i.e. invalid for this inode number
          new_blank_inode = InodeNumber(file_inode)
          # store blank inode to disk
          new_blank_inode.StoreInode(self.FileNameObject.RawBlocks)

        return 0, "SUCCESS"
# END_REMOVE_TO_DISTRIBUTE


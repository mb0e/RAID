import pickle, logging
import fsconfig
import xmlrpc.client, socket, time
import hashlib

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self, num_servers):

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        # initialize XMLRPC client connection to raw block server
        if fsconfig.PORT:
            PORT = fsconfig.PORT
        else:
            print('Must specify port number')
            quit()
##RAID
        self.checksums = {}
        self.num_servers = num_servers
        self.block_servers = []
        for server_id in range(num_servers):
            server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(fsconfig.PORT + server_id)
            server_proxy = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
            self.block_servers.append(server_proxy)
        
        self.rsm_server = self.block_servers[0]
        print(f'RSM server {self.rsm_server.id}')

##RAID end

        socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)
        # initialize block cache empty
        self.blockcache = {}

    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE

##RAID
    def distribute_block_to_servers(self, block_data):
        distributed_blocks = []
        chunk_size = fsconfig.BLOCK_SIZE // (self.num_servers - 1)

        for i in range(self.num_servers - 1):
            chunk = block_data[i*chunk_size : (i+1)*chunk_size]
            distributed_blocks.append(chunk)

        parity_block = bytearray(fsconfig.BLOCK_SIZE)
        for block in distributed_blocks:
            for i in range(len(block)):
                parity_block[i] ^= block[i]

        distributed_blocks.append(parity_block)

        return distributed_blocks
##RAID end

##RAID
    def generate_checksum(block_data):
        sha_signature = hashlib.sha256(block_data).hexdigest()
        return sha_signature
##RAID end

    def Put(self, block_number, block_data):
        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

##RAID
        distributed_blocks = self.distribute_block_to_servers(block_data)
        for i, block in enumerate(distributed_blocks):
            server = self.block_servers[i % self.num_servers]
            try:
                server.Put(block_number, block)
            except Exception as e:
                logging.error(f'Failed to put block {block_number} to server {i}: {e}')
                print(f'SERVER_DISCONNECTED PUT {block_number}')

        checksum = self.generate_checksum(block_data)
        self.checksums[block_number] = checksum

##RAID end

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # ljust does the padding with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # Write block
            # commenting this out as the request now goes to the server
            # self.block[block_number] = putdata
            # call Put() method on the server; code currently quits on any server failure
            rpcretry = True
            while rpcretry:
                rpcretry = False
                try:
                    ret = self.block_server.Put(block_number, putdata)
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
                    time.sleep(fsconfig.RETRY_INTERVAL)
                    rpcretry = True
            # update block cache
            print('CACHE_WRITE_THROUGH ' + str(block_number))
            self.blockcache[block_number] = putdata
            # flag this is the last writer
            # unless this is a release - which doesn't flag last writer
            if block_number != fsconfig.TOTAL_NUM_BLOCKS-1:
                LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 2
                updated_block = bytearray(fsconfig.BLOCK_SIZE)
                updated_block[0] = fsconfig.CID
                rpcretry = True
                while rpcretry:
                    rpcretry = False
                    try:
                        self.block_server.Put(LAST_WRITER_BLOCK, updated_block)
                    except socket.timeout:
                        print("SERVER_TIMED_OUT")
                        time.sleep(fsconfig.RETRY_INTERVAL)
                        rpcretry = True
            if ret == -1:
                logging.error('Put: Server returns error')
                quit()
            return 0
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def Get(self, block_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # logging.debug ('\n' + str((self.block[block_number]).hex()))
            # commenting this out as the request now goes to the server
            # return self.block[block_number]
            # call Get() method on the server
            # don't look up cache for last two blocks

##RAID
            blocks = []
            for i in range(self.num_servers):
                server = self.block_servers[i % self.num_servers]
                try:
                    block = server.Get(block_number)
                    blocks.append(block)
                except Exception as e:
                    logging.error(f'Failed to get block {block_number} from server {i}: {e}')
                    print(f'CORRUPTED_BLOCK {block_number}')

                    blocks.append(None)

            for i in range(len(blocks)):
                if blocks[i] is None:
                    blocks[i] = self.reconstruct_block(blocks)

            read_data = self.block_servers[0].Get(block_number)
            read_checksum = self.generate_checksum(read_data)
            stored_checksum = self.checksums[block_number]
            if read_checksum != stored_checksum:
                logging.error(f'Checksum mismatch for block {block_number}. Data is corrupted.')

            #return b''.join(blocks)
## RAID end
            if (block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (block_number in self.blockcache):
                print('CACHE_HIT '+ str(block_number))
                data = self.blockcache[block_number]
            else:
                print('CACHE_MISS ' + str(block_number))
                rpcretry = True
                while rpcretry:
                    rpcretry = False
                    try:
                        data = self.block_servers[0].Get(block_number)
                    except socket.timeout:
                        print("SERVER_TIMED_OUT")
                        time.sleep(fsconfig.RETRY_INTERVAL)
                        rpcretry = True
                # add to cache
                self.blockcache[block_number] = data
            # return as bytearray
            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

## RSM: read and set memory equivalent
    def reconstruct_block(self, blocks):
        missing_block = bytearray(fsconfig.BLOCK_SIZE)
        for block in blocks:
            if block is not None:
                for i in range(len(block)):
                    missing_block[i] ^= block[i]

        return missing_block

    def RSM(self, block_number, request): #check if request is valid!!
        logging.debug('RSM: ' + str(block_number))
#RAID
        try:
            return self.rsm_server.RSM(request)
        except Exception as e:
            print(f'Failed to send RSM request to server {self.rsm_server.id}: {e}')
#RAID end

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            rpcretry = True
            while rpcretry:
                rpcretry = False
                try:
                    data = self.block_server.RSM(block_number)
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
                    time.sleep(fsconfig.RETRY_INTERVAL)
                    rpcretry = True

            return bytearray(data)

        logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

        ## Acquire and Release using a disk block lock

##RAID
    def repair(self, failed_server_id):
        # Replace the failed server with a new one
        failed_server_url = self.block_servers[failed_server_id].url
        self.block_servers[failed_server_id] = xmlrpc.client.ServerProxy(failed_server_url)

        # Reconstruct each block on the new server
        for block_number in range(fsconfig.TOTAL_NUM_BLOCKS):
            blocks = []
            for i in range(self.num_servers):
                if i == failed_server_id:
                    blocks.append(None)
                else:
                    server = self.block_servers[i]
                    try:
                        block = server.Get(block_number)
                        blocks.append(block)
                    except Exception as e:
                        print(f'Failed to get block {block_number} from server {i}: {e}')
                        blocks.append(None)

            missing_block = self.reconstruct_block(blocks)
            self.block_servers[failed_server_id].Put(block_number, missing_block)
##RAID end
      
    def Acquire(self):
        logging.debug('Acquire')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        lockvalue = self.RSM(RSM_BLOCK);
        logging.debug("RSM_BLOCK Lock value: " + str(lockvalue))
        while lockvalue[0] == 1:  # test just first byte of block to check if RSM_LOCKED
            logging.debug("Acquire: spinning...")
            lockvalue = self.RSM(RSM_BLOCK);
        # once the lock is acquired, check if need to invalidate cache
        self.CheckAndInvalidateCache()
        return 0

    def Release(self):
        logging.debug('Release')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        # Put()s a zero-filled block to release lock
        self.Put(RSM_BLOCK,bytearray(fsconfig.RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')))
        return 0

    def CheckAndInvalidateCache(self):
        LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 2
        last_writer = self.Get(LAST_WRITER_BLOCK)
        # if ID of last writer is not self, invalidate and update
        if last_writer[0] != fsconfig.CID:
            print("CACHE_INVALIDATED")
            self.blockcache = {}
            updated_block = bytearray(fsconfig.BLOCK_SIZE)
            updated_block[0] = fsconfig.CID
            self.Put(LAST_WRITER_BLOCK,updated_block)

    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))

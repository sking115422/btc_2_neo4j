

# BTC 2 NEO4J IMPORTER
# Spencer King <sking115422@gmail.com>

# Command to run
# json_to_neo4j.py

# 25% of DAT file > ~ 125000 node > ~150000 relationships > in ~ 2 hours

# Importing libraries
from neo4j import GraphDatabase
from py2neo import Graph
import numpy as np
import pandas as pd
import json
import os
import traceback
from datetime import datetime
import pytz
import time
import logging


# Logging
# If logging is set to true it will record the creation of every node and relationship in a log file called "logs"
# Other good format: '%(name)s > %(process)d > %(levelname)s:     %(message)s'
logging_ = True
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('./logs/json_to_neo4j.log', mode='a+')
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def getTimeStamp ():
    
    EST = pytz.timezone("EST")
    datetime_est = datetime.now(EST)
    ct = datetime_est.strftime('%Y:%m:%d %H:%M:%S %Z %z')
    return str(ct)


def createBlockNode (sess, blk, df_start, bn_start):
    
    hash_ = str(blk["hash"])
    
    try:
        version = str(blk["version"])
    except:
        version = "null"
        
    previousBlockHash = str(blk["previousblockhash"])
    
    try:
        merkleRoot = str(blk["merkleroot"])
    except:
        merkleRoot
    try:
        time = str(blk["time"])
    except:
        time = "null"
    try:
        difficulty = str(blk["difficulty"])
    except:
        difficulty = "null"
    try:
        nonce = str(blk["nonce"])
    except:
        nonce = "null"
        
    numTransactions = str(blk["nTx"])
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
    
    ps = "CREATE (n:block {" 
    p1 = "hash: '" + hash_ + "', "
    p2 = "version: '" + version + "', "
    p3 = "previousBlockHash: '" + previousBlockHash + "', "
    p4 = "merkleRoot: '" + merkleRoot + "', "
    p5 = "time: '" + time + "', "
    p6 = "difficulty: '" + difficulty + "', "
    p7 = "nonce: '" + nonce + "', "
    p8 = "numTranactions: '" + numTransactions +"', "
    p9 = "datFileNum: '" + dat_file_num + "', "
    p10 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + p10 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : block > hash_id : " + hash_)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createCoinbaseNode (sess, tx, df_start, bn_start):

    value = str(tx["vout"][0]["value"])
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
        
    ps = "CREATE (n:coinbase {"
    p1 = "value: '" + value + "', "
    p2 = "datFileNum: '" + dat_file_num + "', "
    p3 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : coinbase > value : " + value)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createTxNode (sess, tx, df_start, bn_start):
    
    txid = str(tx["txid"])
    
    try:
        version = str(tx["version"])
    except:
        version = "null"
        
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
    
    ps = "CREATE (n:tx {"
    p1 = "txid: '" + txid +"', "
    p2 = "version: '" + version +"', "
    p3 = "datFileNum: '" + dat_file_num + "', "
    p4 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + p4 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : tx > txid : " + txid)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createOutputNode(sess, tx, ind, df_start, bn_start):
    
    vout = str(ind)
    value = str(tx["value"])
    scriptPK_hex = str(tx["scriptPubKey"]["hex"])

    try:
        scriptPK_asm = str(tx["scriptPubKey"]["asm"])
    except:
        scriptPK_asm = "null"
    try:
        type_ = str(tx["scriptPubKey"]["type"])
    except:
        type_ = "null"
    try:
        address = str(tx["scriptPubKey"]["address"])
    except:
        address = "null"
        
    dat_file_num = str(df_start)
    blk_num = str(bn_start)
    
    ps = "CREATE (n:output {"
    p1 = "vout: '" + vout + "', "
    p2 = "value: '" + value + "', "
    p3 = "scriptPK_hex: '" + scriptPK_hex + "', "
    p4 = "scriptPK_asm: '" + scriptPK_asm + "', "
    p5 = "type: '" + type_ + "', "
    p6 = "address: '" + address + "', "
    p7 = "datFileNum: '" + dat_file_num + "', "
    p8 = "blkNum: '" + blk_num + "' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3+ p4 + p5 + p6 + p7 + p8 + pl
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating node : output > value : " + value)
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id
    

def checkAddressExists (sess, addr):
    
    cmd1 = "MATCH (n:address) WHERE n.address = '" + addr + "' RETURN n"
    
    ret = sess.run(cmd1)
    
    if len(ret.data()) == 0:
        return False
    else:
        return True
        

def createAddressNode(sess, address, df_start, bn_start):
        
    exists = False
    
    exists = checkAddressExists(sess, address)
    
    dat_file_num = str(df_start)
    blk_num = str(bn_start)

    if not exists :
        
        ps = "CREATE (n:address {"
        p1 = "address: '" + address + "', "
        p2 = "datFileNum: '" + dat_file_num + "', "
        p3 = "blkNum: '" + blk_num + "' "
        pl = "}) RETURN id(n)"
        
        cmd1 = ps + p1 + p2 + p3 + pl
        
        if logging_:
            logger.debug(getTimeStamp() + " | creating node : address > address : " + address)
        
        ret = sess.run(cmd1)

        node_id = ret.data()[0]["id(n)"]
        
        return node_id
            

def createChainRel(sess, prevBlkHash, n4j_blk_id):

    cmd1 = """ 
            MATCH
            (a:block),
            (b:block)
            WHERE 
            a.hash = "{0}"
            AND
            id(b) = {1}
            CREATE (a)-[r:chain]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(prevBlkHash, str(n4j_blk_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : chain")
    
    sess.run(cmd1)


def createRewardRel(sess, n4j_blk_id, n4j_cb_id):

    cmd1 = """ 
            MATCH
            (a:block),
            (b:coinbase)
            WHERE 
            id(a) = {0}
            AND
            id(b) = {1}
            CREATE (a)-[r:reward]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_blk_id), str(n4j_cb_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : reward")
    
    sess.run(cmd1)
    

def createSeedsRel(sess, n4j_cb_id, n4j_tx_id):   

    cmd1 = """ 
            MATCH
            (a:coinbase),
            (b:tx)
            WHERE 
            id(a) = {0} 
            AND
            id(b) = {1}
            CREATE (a)-[r:seeds]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_cb_id), str(n4j_tx_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : seeds")
    
    sess.run(cmd1)


def createIncludesRel(sess, n4j_tx_id, n4j_blk_id):

    cmd1 = """ 
            MATCH
            (a:tx),
            (b:block)
            WHERE 
            id(a) = {0}
            AND
            id(b) = {1}
            CREATE (a)-[r:includes]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_tx_id), str(n4j_blk_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : includes")
            
    sess.run(cmd1)


def createOutRel(sess, n4j_tx_id, n4j_out_id):

    cmd1 = """ 
            MATCH
            (a:tx),
            (b:output)
            WHERE 
            id(a) = {0}
            AND
            id(b) = {1}
            CREATE (a)-[r:out]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_tx_id), str(n4j_out_id))
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : out")
            
    sess.run(cmd1)


def createLockedRel(sess, n4j_out_id, address):

    cmd1 = """ 
            MATCH
            (a:output),
            (b:address)
            WHERE 
            id(a) = {0}
            AND
            b.address = "{1}"
            CREATE (a)-[r:locked]->(b)
            RETURN type(r)
            """
            
    cmd1 = cmd1.format(str(n4j_out_id), address)
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : locked")
            
    sess.run(cmd1)


def createUnlockRel(sess, vin, n4j_tx_id):
    
    txid_in = str(vin["txid"])
    vout = str(vin["vout"])
    scriptSig_ = str(vin["scriptSig_hex"])

    cmd1 = "MATCH (a:tx) - [:out]-> (b:output) WHERE a.txid = '{0}' AND b.vout = '{1}' "
    cmd1 = cmd1.format(txid_in, vout, str(n4j_tx_id))
    
    cmd2 = "MATCH (c:tx) WHERE id(c) = {0} "
    cmd2 = cmd2.format(str(n4j_tx_id)) 
        
    cmd3 = "CREATE (b) - [r:unlock {scriptSig: '" + scriptSig_ + "'}] -> (c) RETURN r"
        
    cmd4 = cmd1 + cmd2 + cmd3
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating relationship : unlock > scriptsig : " + scriptSig_)
    
    sess.run(cmd4)


def checkIndexExists(sess, nodelLabel, propName):
    
    cmd1 = "CALL db.indexes()"
    
    ret = sess.run(cmd1)
    
    index_list = ret.data()
    
    exists = False
    
    for each in index_list:
        
        labelsOrTypes = each["labelsOrTypes"]
        properties = each["properties"]
        
        if len(labelsOrTypes) > 0 and len(properties) > 0:
            if (labelsOrTypes[0] == nodelLabel and properties[0] == propName):
                exists = True
    
    return exists


def createIndex (sess, nodeLabel, propName):
    
    # CALL db.indexes()
    # DROP INDEX ON :nodeLabel(propName)
    
    cmd1 = "CREATE INDEX ON :" + nodeLabel + "(" + propName + ")"
    
    if logging_:
        logger.debug(getTimeStamp() + " | creating index > nodetype : " + nodeLabel + " > propname : " + propName)
        
    if not checkIndexExists(sess, nodeLabel, propName):
        sess.run(cmd1)
        
        
def deleteBlockNodes (sess, datFileNum, blkNum):
    
    datFileNum = str(datFileNum)
    blkNum = str(blkNum)
    
    cmd1 =  """ 
            match (n)
            where 
            n.datFileNum = "{0}" 
            and 
            n.blkNum = "{1}"
            detach delete n
            """
            
    cmd1 = cmd1.format(datFileNum, blkNum)

    if logging_:
        logger.debug(getTimeStamp() + " | deleted block : " + blkNum + " from DAT file : " + datFileNum)
            
    sess.run(cmd1)
    

# Creating neo4j database driver and session
# graph = Graph(uri='neo4j://localhost:7687', user="neo4j", password="password")
dbc = GraphDatabase.driver(uri = "bolt://localhost:7687", auth=("neo4j", "password"))
logger.debug("")
logger.debug("Connected to Neo4J database : " + str(dbc))
sess = dbc.session(database="neo4j")


# Return lists of the DAT file jsons
block_list = sorted(os.listdir("./result/"))

# Import checkpoint values 
cp = open("./checkpoint.json")
cpjs = json.load(cp)
df_start = cpjs["dat_file"]
bn_start = cpjs["block_num"]

# If checkpoint.json file is not 0, 0 delete last partial imported block
if df_start != 0 or bn_start != 0:
    deleteBlockNodes(sess, df_start, bn_start)


### MAIN DRIVER CODE


# Try until some error happens
try:
    logger.debug("")
    logger.debug("STARTING IMPORT TO NEO4J")
    logger.debug("***********************************")

    # Iterating through each pair of jsons for each DAT file
    # testing loop below
    # for a in range(0, 1):
    for a in range(df_start, len(block_list)):
        
        # Reset bn_start each time DAT file finished
        if a != df_start:
            bn_start = 0
        
        df_start = a
        
        
        logger.debug("")
        logger.debug("loading > ./result/" + block_list[a])
        with open("./result/" + block_list[a]) as bl:
            bl_json = json.load(bl)
        logger.debug("")

        #Iterate through all blocks in dat file
        # testing loops below
        # for i in range(len(bl_json)-5, len(bl_json)):    
        # for i in range(0, 10):
        for i in range(bn_start, len(bl_json)):
            
            bn_start = i
            
            start = time.time()
            
            blk = bl_json[i]
            n4j_blk_id = createBlockNode(sess, blk, df_start, bn_start)
            tx_list = blk["tx"]
            
            # If block is the not the first block create chain relationship
            prevBlkHash = str(blk["previousblockhash"])
            if prevBlkHash != "0000000000000000000000000000000000000000000000000000000000000000":
                createChainRel(sess, prevBlkHash, n4j_blk_id)
            
            #Iterating through each transaction associated with the current block
            for j in range(0, len(tx_list)):
            # for j in range(0,1):
                
                tx_data = tx_list[j]
                
                # Creating transaction node and includes relationship
                n4j_tx_id = createTxNode(sess, tx_data, df_start, bn_start)
                createIncludesRel(sess, n4j_tx_id, n4j_blk_id)
                
                # Iterate through vin list in transaction
                for each in tx_data["vin"]:
                    
                    # If it is the first transaction create coinbase node, reward relationship, and seeds relationship
                    txid_in = str(each["txid"])
                    if txid_in == "0000000000000000000000000000000000000000000000000000000000000000":
                        n4j_cb_id = createCoinbaseNode(sess, tx_data, df_start, bn_start)
                        createRewardRel(sess, n4j_blk_id, n4j_cb_id)
                        createSeedsRel(sess, n4j_cb_id, n4j_tx_id)
                        exit
                        
                    # Otherwise create the unlock relationship
                    else:
                        createUnlockRel(sess, each, n4j_tx_id)
                
                # Iterate through outputs for each transaction
                outputs = tx_data["vout"]
                for z in range(0, len(outputs)):
                    
                    # Creating output node and out relationship
                    n4j_out_id = createOutputNode(sess, outputs[z], z, df_start, bn_start)
                    createOutRel(sess, n4j_tx_id, n4j_out_id)
                    
                    # Create creating address node if it does not exist and locked relationship to an address (might be address just created might not)
                    try:
                        address = str(outputs[z]["scriptPubKey"]["address"])
                    except:
                        address = "N/A"
                        
                    if address != "N/A":
                        n4j_addr_id = createAddressNode(sess, address, df_start, bn_start)
                        createLockedRel(sess, n4j_out_id, address)
                
            end = time.time()
            
            diff = end - start
            
            logger.debug("")
            logger.debug(getTimeStamp() + " DAT file : " + str(df_start) + " > block: " + str(bn_start) + " | import complete > execution time : " + str(diff))
            logger.debug("") 


    # Creating indexes
    createIndex(sess, "block", "hash")
    createIndex(sess, "tx", "txid")
    createIndex(sess, "address", "address")


    logger.debug("")
    logger.debug("***********************************")
    logger.debug("FINISHED IMPORT TO NEO4J")

    # Closing session
    sess.close()

# If error happens save progress
except:
    
    cpjs["dat_file"] = df_start
    cpjs["block_num"] = bn_start
    
    tmp = json.dumps(cpjs, indent=4)
    
    with open("checkpoint.json", "w") as outfile:
        outfile.write(tmp)
    
    print(traceback.format_exc())
        


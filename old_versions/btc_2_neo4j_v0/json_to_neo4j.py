

# BTC 2 NEO4J IMPORTER
# Spencer King <sking115422@gmail.com>
#

# 25% of DAT file > ~ 125000 node > ~150000 relationships > in ~ 2 hours

# Importing libraries
from neo4j import GraphDatabase
from py2neo import Graph
import numpy as np
import pandas as pd
import json
import os
from datetime import datetime
import pytz


# Logging
# If logging is set to true it will record the creation of every node and relationship in a log file called "logs"
logging = True

# 

def getTimeStamp ():
    
    EST = pytz.timezone("EST")
    datetime_est = datetime.now(EST)
    ct = datetime_est.strftime('%Y:%m:%d %H:%M:%S %Z %z')
    return str(ct)


def createBlockNode (sess, blk):
    
    ps = "CREATE (n:block {" 
    p1 = "hash: '" + str(blk["hash"]) + "', "
    p2 = "version: '" + str(blk["version"]) + "', "
    p3 = "previousBlockHash: '" + str(blk["previousblockhash"]) + "', "
    p4 = "merkleRoot: '" + str(blk["merkleroot"]) + "', "
    p5 = "time: '" + str(blk["time"]) + "', "
    p6 = "difficulty: '" + str(blk["difficulty"]) + "', "
    p7 = "nonce: '" + str(blk["nonce"]) + "', "
    p8 = "numTranactions: '" + str(blk["nTx"]) +"' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + pl
    
    if logging:
        print(getTimeStamp() + " creating node : block > hash_id : " + str(blk["hash"]))
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createCoinbaseNode (sess, tx):
    
    ps = "CREATE (n:coinbase {"
    p1 = "value: '" + str(tx["vout"][0]["value"]) +"' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + pl
    
    if logging:
        print(getTimeStamp() + " creating node : coinbase > value : " + str(tx["vout"][0]["value"]))
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createTxNode (sess, tx):
    
    ps = "CREATE (n:tx {"
    p1 = "txid: '" + str(tx["txid"]) +"', "
    p2 = "version: '" + str(tx["version"]) +"' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + pl
    
    if logging:
        print(getTimeStamp() + " creating node : tx > txid : " + str(tx["txid"]))
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id


def createOutputNode(sess, tx, ind):
    
    ps = "CREATE (n:output {"
    p1 = "vout: '" + str(ind) + "', "
    p2 = "value: '" + str(tx["value"]) +"', "
    p3 = "scriptPK_hex: '" + str(tx["scriptPubKey"]["hex"]) +"', "
    p4 = "scriptPK_asm: '" + str(tx["scriptPubKey"]["asm"]) +"', "
    p5 = "type: '" + str(tx["scriptPubKey"]["type"]) +"', "
    p6 = "address: '" + str(tx["scriptPubKey"]["address"]) +"' "
    pl = "}) RETURN id(n)"
    
    cmd1 = ps + p1 + p2 + p3+ p4 + p5 + p6 + pl
    
    if logging:
        print(getTimeStamp() + " creating node : output > value : " + str(tx["value"]))
    
    ret = sess.run(cmd1)
    
    node_id = ret.data()[0]["id(n)"]
    
    return node_id
    


def checkAddressExists (sess, addr):
    
    cmd1 = "MATCH (n:address) WHERE n.address = '" + addr + "' RETURN n"
    
    ret = sess.run(cmd1)
    
    # print(ret.data()[0]["n"]["address"])
    
    if len(ret.data()) == 0:
        return False
    else:
        return True
        


def createAddressNode(sess, address):
        
    exists = False
    
    exists = checkAddressExists(sess, address)

    if not exists :
        
        ps = "CREATE (n:address {"
        p1 = "address: '" + address +"' "
        pl = "}) RETURN id(n)"
        
        cmd1 = ps + p1 + pl
        
        if logging:
            print(getTimeStamp() + " creating node : address > address : " + address)
        
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : chain")
    
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : reward")
    
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : seeds")
    
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : includes")
            
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : out")
            
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : locked")
            
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
    
    if logging:
        print(getTimeStamp() + " creating relationship : unlock > scriptsig : " + scriptSig_)
    
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
    
    if logging:
        print(getTimeStamp() + " creating index > nodetype : " + nodeLabel + " > propname : " + propName)
        
    if not checkIndexExists(sess, nodeLabel, propName):
        sess.run(cmd1)


# Creating neo4j database driver and session
# graph = Graph(uri='neo4j://localhost:7687', user="neo4j", password="password")
dbc = GraphDatabase.driver(uri = "bolt://localhost:7687", auth=("neo4j", "password"))
print()
print("Connected to Neo4J database : " + str(dbc))
print()
sess = dbc.session(database="neo4j")


# Return lists of the DAT file jsons
block_list = sorted(os.listdir("./result/block_list/"))
tr_list = sorted(os.listdir("./result/tr_list/"))


print()
print ("STARTING IMPORT TO NEO4J")
print ("***********************************")
print ()

# Iterating through each pair of jsons for each DAT file
# testing loop below
# for a in range(0, 1):
for a in range(0, len(block_list)):
    
    print('loading > ./result/block_list/' + block_list[a])
    bl = open('./result/block_list/' + block_list[a])
    bl_json = json.load(bl)
    
    
    print('loading > ./result/tr_list/' + tr_list[a])
    print()
    trl = open('./result/tr_list/' + tr_list[a])
    trl_json = json.load(trl)

    #Iterate through all blocks in dat file
    # testing loops below
    # for i in range(len(bl_json)-5, len(bl_json)):    
    # for i in range(0, 10):
    for i in range(0, len(bl_json)):
        
        blk = bl_json[i]
        n4j_blk_id = createBlockNode(sess, blk)
        tx_list = blk["tx"]
        
        # If block is the not the first block create chain relationship
        prevBlkHash = str(blk["previousblockhash"])
        if prevBlkHash != "0000000000000000000000000000000000000000000000000000000000000000":
            createChainRel(sess, prevBlkHash, n4j_blk_id)
        
        #Iterating through each transaction associated with the current block
        for j in range(0, len(tx_list)):
        # for j in range(0,1):
            
            txid_blk = tx_list[j]
            
            # Getting index of transactions ids associated with the current block from trl_json
            for k in range(0,len(trl_json)):
                txid_tx = trl_json[k]["txid"]
                if (txid_blk == txid_tx):
                    index = k
                    exit
            
            # Get transaction data from the related index
            tx_data = trl_json[index]
            
            # Creating transaction node and includes relationship
            n4j_tx_id = createTxNode(sess, tx_data)
            createIncludesRel(sess, n4j_tx_id, n4j_blk_id)
            
            # Iterate through vin list in transaction
            for each in tx_data["vin"]:
                
                # If it is the first transaction create coinbase node, reward relationship, and seeds relationship
                txid_in = str(each["txid"])
                if txid_in == "0000000000000000000000000000000000000000000000000000000000000000":
                    n4j_cb_id = createCoinbaseNode(sess, tx_data)
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
                n4j_out_id = createOutputNode(sess, outputs[z], z)
                createOutRel(sess, n4j_tx_id, n4j_out_id)
                
                # Create creating address node if it does not exist and locked relationship to an address (might be address just created might not)
                address = str(outputs[z]["scriptPubKey"]["address"])
                if address != "N/A":
                    n4j_addr_id = createAddressNode(sess, address)
                    createLockedRel(sess, n4j_out_id, address)
            

# Creating indexes
# createIndex(sess, "block", "hash")
# createIndex(sess, "tx", "txid")
# createIndex(sess, "address", "address")


print()
print ("***********************************")
print ("FINISHED IMPORT TO NEO4J")

# Closing session
sess.close()



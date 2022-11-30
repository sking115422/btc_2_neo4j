# BTC 2 NEO4J

This package contains both a parser for blk.dat files and an import script for dumping the json into Neo4J graph database.

## Get .DAT files

I obtained the files need by starting my own validation node for Bitcoin using Bitcoin Core. The process is explained more fully in the link below:

[Setup BTC Validation Node](https://link-url-here.org)

## Basic Setup

1. Pull down this repo from GitHub
2. cd in the main directory BTC_2_NEO4J
3. Create a directory called "blocks" and a directory called "result"
4. Inside of the new result folder make 2 more directories named "block_list" and "tr_list"
5. Add desired BTCXXXXX.DAT files into blocks folder
6. Create and activate a virtual enviroment
7. Use pip install -r requirements.txt to install needed libraries

*** NOTE replace requirements.txt with requirements_lin_mac.txt for linux or mac users and requirements_win.txt for windows users ***

## Run Parser

1. Run dat_to_json.py script

## Setup Neo4J Locally

1. Create new Database in Neo4J as follows:
   1. project name: BTC Ledger Test
   2. DBMS name: BTC_NEO4J
   3. password: password

## Import to Neo4J

1. Run json_to_neo4j.py
2. This will import all the json information into Neo4J
3. Once finished open the browser in neo4j to interact with the BTC graph you have created

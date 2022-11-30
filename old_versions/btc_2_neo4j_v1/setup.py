

# Importing libraries
import os
import platform
import json

### ADDING DIRECTORIES 

# Directories to add
dirs = ["./blocks/", "./logs/", "./result"] 

# Loop through the directory list
for each in dirs:
    
    # Create a new directory if it does not exist
    isExist = os.path.exists(each)
    if not isExist:
        os.makedirs(each)
        print("directory created : " + each)
        
### CREATING CHECKPOINT JSON

# Creating dictionary of initial values
cp = {
    "dat_file":0,
    "block_num":0
}

# Writing initial values to checkpoint.json file
tmp = json.dumps(cp, indent=4)
if not os.path.exists("./checkpoint.json"):
    with open("checkpoint.json", "w+") as outfile:
        outfile.write(tmp)
        
### CREATING VIRTUAL ENVIRONMENT

# Creating virtual environment
if not os.path.exists("./venv/"):
    os.system("python3 -m venv ./venv")

# Determining OS type 
os_type = platform.system()

# Setting up virtual environment
if os_type == "Darwin" or os_type == "Linux":
    os.system("source venv/bin/activate && pip install -r requirements_lin_mac.txt")
elif os_type == "Windows":
    os.system("./venv/bin/activate && pip install -r requirements_win.txt")
else:
    print("current system OS not recongnized... please setup manually")


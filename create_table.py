import os
import sys
import subprocess

user = os.getenv('USER')
config_path = f"/home/{user}/ydb/config/config.yaml"
base_path = None
script_path = sys.argv[3]

#=========Take base url from config file========#
with open(config_path, 'r') as file:
    for line in file:
        if "database" in line:
            base_path = line.split(":")[1].strip() # Remove trailing spaces and newlines from both sides

#========Swap TablePathPrefix in script==========#
with open(script_path, 'r') as scr:
    lines = scr.readlines() # Read all lines to a list

# Create the new prefix line
new_prefix_line = f'PRAGMA TablePathPrefix = "{base_path}/{sys.argv[2]}";\n'

# Check if first line is PRAGMA TablePathPrefix
if lines[0].startswith("PRAGMA TablePathPrefix"):
    lines[0] = new_prefix_line # Replace the first line
else:
    lines.insert(0, new_prefix_line) # Insert as the first line

# Write lines back to file
with open(script_path, 'w') as scr:
    scr.writelines(lines)

# Run the ydb command
command = ['ydb', '-p', sys.argv[1], 'scripting', 'yql', '-f', script_path]
subprocess.run(command)

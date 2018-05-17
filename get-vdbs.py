#The purpose of this script is to grab vdbs that are not located in the deployments folder
#This can be accomplished by finding the SHA generated for the corresponding vdb in the standalone.xml (or other mode) <deployments> section.
#Then navigate to STANDALONE_HOME/data/content/FIRST_TWO_CHAR_OF_SHA/REMAINING_CHAR/content
#Rename this file to .vdb and you have it.

import re, sys
from shutil import copyfile

# v1 Assumptions
# 1. Grab all deployments (non-file deployed)
# 2. Copy to current directory
# 3. Using standalone.xml
# 4. Keep name the same
# 5. Running from bin/ on respective server

# default: standalone.xml
mode = "standalone"
mode_file = "standalone.xml"
mode_home = "../" + mode + "/"
mode_config_home = mode_home + "configuration/"
mode_file_path = mode_config_home + mode_file

conf = open(mode_file_path)
conf_lines = conf.read()

re_vdb = r"(deployment name=\"(\w*\.vdb).*\n.*sha1=\"(\w*))"
matches = re.findall(re_vdb, conf_lines)

vdb_dict = {}

for match in matches:
    if match[1] not in vdb_dict:
        vdb_dict[match[1]] = match[2]
        print "Found: ", match[1], ": ", match[2]

copy_dest = "./"
# Copy binary to current directory as .vdb
for vdb in vdb_dict:
    vdb_prefix = vdb_dict[vdb][0:2]
    vdb_postfix = vdb_dict[vdb][2:]
    vdb_bin = mode_home + "data/content/" + vdb_prefix + "/" + vdb_postfix + "/content"
    copyfile(vdb_bin, copy_dest + vdb)
    print "Copied: ", vdb_bin, " to: ", "./" + vdb

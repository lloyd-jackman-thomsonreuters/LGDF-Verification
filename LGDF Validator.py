# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 08:53:35 2017

@author: Lloyd Jackman
"""

from ftplib import FTP
import zipfile
import os
import hashlib
import xml.etree.ElementTree as ET
from multiprocessing import Pool, Lock
from itertools import repeat
import getpass


def init(l):
    global lock
    lock = l

def uni_verify(zip_file, md5_dict, outputfolder, xmlfolder, ftp_user, ftp_pw):
    ftp = FTP('lipperftp.thomsonreuters.com', ftp_user, ftp_pw)
    ftp.cwd('/datafeeds/LipperGlobalDataFeed5/standardfeed/')
    local_filename = os.path.join(xmlfolder, zip_file)
    f = open(local_filename, 'wb')
    ftp.retrbinary('RETR ' + zip_file, f.write, 262144)
    f.close()
    xml_list = zipfile.ZipFile(local_filename).namelist()
    no_xml_list = len(xml_list)
    actual_md5 = hashlib.md5(open(local_filename,'rb').read()).hexdigest()
    if actual_md5 == md5_dict[zip_file]:
        ok = "OK"
    else:
        ok = "Not OK"
    xml_list = zipfile.ZipFile(local_filename).namelist()
    for xml in xml_list:
        zipfile.ZipFile(local_filename).extract(xml, path = xmlfolder)
        local_xml = os.path.join(xmlfolder, xml)
        try:
            ET.parse(local_xml)
        except ET.XMLSyntaxError:
            lock.acquire()
            with open(os.path.join(outputfolder, "Bad XML.txt"), 'a') as output:
                output.write(zip_file + "\t" + xml + "\n")
            lock.release()
        finally:
            lock.acquire()
            try:
                os.remove(local_xml)
            except:
                print("Issue removing " + local_xml)
            lock.release()
    lock.acquire()
    with open(os.path.join(outputfolder, "md5 verification.txt"), 'a') as output:
        output.write(zip_file + "\t" + str(md5_dict[zip_file]) + "\t" + actual_md5 + "\t" + ok + "\t" + str(no_xml_list) + "\n")
    try:
        os.remove(local_filename)
    except:
        print("Issue removing " + local_filename)
    lock.release()

if __name__ == '__main__':
    ftp_user = input("What is your FTP user name? ")
    ftp_pw = input("What is your FTP password? ")
    ftp = FTP('lipperftp.thomsonreuters.com', ftp_user, ftp_pw)
    ftp.cwd('/datafeeds/LipperGlobalDataFeed5/standardfeed/')
    username = getpass.getuser()
    outputfolder = "C:\\Users\\"+username+"\\Documents\\LGDF Validator"
    try:
        os.chdir(outputfolder)
    except FileNotFoundError:
        os.mkdir(outputfolder)
        os.chdir(outputfolder)
    xmlfolder = "C:\\Users\\"+username+"\\Documents\\LGDF Validator\\Temp"
    if not os.path.isdir(xmlfolder):
        os.mkdir(xmlfolder)
    req_patterns = input("Do you require to check:\n1. Global ex USA full and USA full\n2. Global ex USA Holdings full and USA Holdings Full\n3. All of the above (1/2/3) ")
    patterns_dict = {'1': ["usafull", "globalexusafull"], '2': ["usaholdingsfull", "globalexusaholdingsfull"], '3': ["usaholdingsfull", "globalexusaholdingsfull", "usafull", "globalexusafull"]}
    patterns = patterns_dict[req_patterns]
    latest_del = {}
    md5_dict = {}

    for line in ftp.mlsd():
        file_name = line[0]
        if not file_name.endswith(".md5"):
            continue
        for pattern in patterns:
            if not file_name.split("_")[0] == pattern:
                continue
            latest_del[pattern] = file_name
    for file in latest_del.values():
        local_filename = os.path.join(xmlfolder, file)
        f = open(local_filename, 'wb')
        ftp.retrbinary('RETR ' + file, f.write)
        f.close()
        for md5_file in os.listdir(xmlfolder):
            if not md5_file.endswith(".md5"):
                continue
            print("Found :" + md5_file)
            for line in open(os.path.join(xmlfolder, md5_file)):
                md5_checksum = line.split()[0]
                md5_file_name = line.split()[1]
                md5_dict[md5_file_name] = md5_checksum
            os.remove(os.path.join(xmlfolder,md5_file))

    open("md5 verification.txt", 'w')
    open("md5 verification.txt", 'w').close()
    open("md5 verification.txt", 'a').write("File name\tLipper checksum\tVerified checksum\tOK?\tNumber of XML files\n")

    open("Bad XML.txt", 'w')
    open("Bad XML.txt", 'w').close()
    open("Bad XML.txt", 'a').write("Zip file name\tXML file name\n")

    l = Lock()
    file_list = list(md5_dict.keys())
    md5_verify_pool = Pool(initializer=init, initargs=(l,))
    md5_verify_pool.starmap(uni_verify, zip(file_list, repeat(md5_dict), repeat(outputfolder), repeat(xmlfolder), repeat(ftp_user), repeat(ftp_pw)))
    md5_verify_pool.close()
    md5_verify_pool.join()
    print("That's all folks!")

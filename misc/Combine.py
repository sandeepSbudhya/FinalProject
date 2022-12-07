import shutil
import glob


#import csv files from folder
path = r'/home/sandeep/Uni/ISP_Project/cleaned2017Data'
allFiles = glob.glob(path + "/*.csv")
allFiles.sort()  # glob lacks reliable ordering, so impose your own if output order matters
with open('2017Data.csv', 'wb') as outfile:
    for i, fname in enumerate(allFiles):
        with open(fname, 'rb') as infile:
            if i != 0:
                infile.readline()  # Throw away header on all but first file
            # Block copy rest of file from input to output without parsing
            shutil.copyfileobj(infile, outfile)
            print(fname + " has been imported.")
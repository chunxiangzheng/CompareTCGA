import pandas as pd
import synapseclient
import os
import sys

def compare2Files(fname, originFiles, newFiles, syn):
    if not fname in originFiles:
        return fname + "\tNot found\t\t\n"
    df1 = pd.read_csv(syn.get(originFiles[fname]).path, sep="\t", index_col=0, na_values=['null']).astype('float')
    df1 = df1.ix[:, sorted(df1.columns)]
    df2 = pd.read_csv(syn.get(newFiles[fname]).path, sep="\t", index_col=0, na_values=['null']).astype('float')
    df2 = df2.ix[:, sorted(df2.columns)]
    missingGene = ",".join(df2.index - df1.index)
    maxdiff = (df1.ix[:,:] - df2.ix[df1.index,:]).abs().max().max()
    return "%s\t%s\t%g\n"%(fname, missingGene, maxdiff)
    
original = "syn2812961"
new = "syn3270657"
orginalFolder = "original"
newFolder = "new"

syn=synapseclient.login()

originFiles = {x["file.name"]: x["file.id"] for x in syn.chunkedQuery("select name from file where benefactorId=='%s'"%original + " and fileType != 'clinicalMatrix' and fileType != 'maf'")}
newFiles = {x["file.name"]: x["file.id"] for x in syn.chunkedQuery("select name from file where benefactorId=='%s'"%new + " and fileType != 'clinicalMatrix' and fileType != 'maf'")}

f = open("fileDifference.tsv", "r")
s = set()
for line in f:
    arr = line.split("\t")
    s.add(arr[0])
f.close()

fout = open("fileDifference.tsv", "a")
for fname in newFiles:
    if fname in s: continue
    fout.write(compare2Files(fname, originFiles, newFiles, syn))

fout.close()
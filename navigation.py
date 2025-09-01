from pyspark import SparkContext


# Finds out the index of "name" in the array firstLine
# returns -1 if it cannot find it
def findCol(firstLine, name):
    if name in firstLine:
        return firstLine.index(name)
    else:
        return -1


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# read the input file into an RDD[String]
wholeFile = sc.textFile("./data/CLIWOC15.csv")

# The first line of the file defines the name of each column in the cvs file
# We store it as an array in the driver program
firstLine = wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"', '').split(',')

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not ("RecID" in x))


# split each line into an array of items
entries = entries.map(lambda x: x.split(','))

# keep the RDD in memory
entries.cache()

##### Create an RDD that contains all nationalities observed in the
##### different entries

# Information about the nationality is provided in the column named
# "Nationality"

# First find the index of the column corresponding to the "Nationality"
#column_index = findCol(firstLine, "Nationality")
#print("{} corresponds to column {}".format("Nationality", column_index))

# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates
# Question 1. Élimination des espaces par l'usage de la fonction replace et filtrage des NA pour ne pas les afficher.
# On sait qu'on gère des variables de type string, donc on se permet d'utiliser replace sur des types lambdas.
#nationalities = entries.map(lambda x: x[column_index].replace(' ', '')).filter(
#    lambda x: x != "NA").distinct()

# Display the 5 first nationalities
#print("A few examples of nationalities:")
#for elem in nationalities.sortBy(lambda x: x).take(5):
#    print(elem)

# 2. On utilise count pour compter toutes les observations dans le tableau
#print totalLength = entries.count()
#print("Nombre total d'observations : {}".format(totalLength))

column_index_year = findCol(firstLine, "Year")
print("{} corresponds to column {}".format("Year", column_index_year))
years = entries.map(lambda x: x[column_index_year].replace(' ', '')).filter(
    lambda x: x != "NA").distinct()

# TODO CONVERTIR
minYear = int(years.min())
maxYear = int(years.max())

studyNumberOfYears = maxYear - minYear
print("Nombre total d'années de l'étude {}".format(studyNumberOfYears))

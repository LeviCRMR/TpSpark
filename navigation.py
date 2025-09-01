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
column_index = findCol(firstLine, "Nationality")
print("{} corresponds to column {}".format("Nationality", column_index))

# Use 'map' to create a RDD with all nationalities and 'distinct' to remove duplicates
# Question 1. Élimination des espaces par l'usage de la fonction replace et filtrage des NA pour ne pas les afficher.
# On sait qu'on gère des variables de type string, donc on se permet d'utiliser replace sur des types lambdas.
nationalities = entries.map(lambda x: x[column_index].replace(' ', '')).filter(
    lambda x: x != "NA").distinct()

# Display the 5 first nationalities
print("A few examples of nationalities:")
for elem in nationalities.sortBy(lambda x: x).take(5):
   print(elem)

# 2. On utilise count pour compter toutes les observations dans le tableau
# (la première ligne étant exclue au filtrage à la création de la variable entries).
total_length = entries.count()
print("Nombre total d'observations : {}".format(total_length))

column_index_year = findCol(firstLine, "Year")
print("{} corresponds to column {}".format("Year", column_index_year))
years = entries.map(lambda x: x[column_index_year].replace(' ', '')).filter(
    lambda x: x != "NA").distinct()

min_year = int(years.min())
max_year = int(years.max())

study_number_of_years = max_year - min_year
print("Nombre total d'années de l'étude {}".format(study_number_of_years))

print("Année de la première observation : {}".format(min_year))
print("Année de la dernière observation : {}".format(max_year))

# On a plusieurs cas possibles, le cas que l'on aime bien,
# une année avec le nombre minimum d'observations / une année avec le nombre maximum d'observations
# Et le cas que l'on aime moins bien, mais qui peut arriver
# plusieurs années avec le nombre minimum d'observations / plusieurs années avec le nombre minimum d'observations
# et les cas entre deux ou il y a une année avec un nombre minimum d'observations d'un coté et plusieurs années avec un nombre maximum


year_count_obs = entries.map(lambda x: (x[column_index_year].replace(' ', ''), 1)).filter(
    lambda x: x != "NA").reduceByKey(lambda a, b: a + b)

year_count_obs_list = year_count_obs.collect()

min_count_obs = min(year_count_obs_list, key=lambda x: x[1])[1]
max_count_obs = max(year_count_obs_list, key=lambda x: x[1])[1]

min_years = [y for y, c in year_count_obs_list if c == min_count_obs]
max_years = [y for y, c in year_count_obs_list if c == max_count_obs]

print("Année(s) avec le nombre minimum d'observations ({}) : {}".format(min_count_obs, ", ".join(min_years)))
print("Année(s) avec le nombre maximum d'observations ({}) : {}".format(max_count_obs, ", ".join(max_years)))


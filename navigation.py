from pyspark import SparkContext
import time


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

# 5. Pour fournir l'année avec le plus petit nombre d'observations et celle où il y en a le plus
# on doit prendre en compte deux cas possibles
# le cas ou il y a une année avec le minimum / maximum d'observations
# le cas ou il y en a plusieurs qui ont le minimum / maximum d'observations

# On commence par récupérer (key :année, value :nombre_obs) chaque année avec le nombre d'observations lui correspondant (reduceByKey)
# après avoir filtré les NA pour pouvoir compter correctement
year_count_obs = entries.map(lambda x: (x[column_index_year].replace(' ', ''), 1)).filter(
    lambda x: x[0] != "NA").reduceByKey(lambda a, b: a + b)

# On transforme en liste au sens python
year_count_obs_list = year_count_obs.collect()

# On compte le minimum et le maximum d'observations qui sont situés à la deuxième position dans le tableau
min_count_obs = min(year_count_obs_list, key=lambda x: x[1])[1]
max_count_obs = max(year_count_obs_list, key=lambda x: x[1])[1]

# On récupère l'année (ou les années),
# en parcourant le tuple (année, nombre_obs) où le compte d'observations pour cette année est le minimum / maximum récupéré plus haut
min_years = [y for y, c in year_count_obs_list if c == min_count_obs]
max_years = [y for y, c in year_count_obs_list if c == max_count_obs]

print("Année(s) avec le nombre minimum d'observations ({}) : {}".format(min_count_obs, ", ".join(min_years)))
print("Année(s) avec le nombre maximum d'observations ({}) : {}".format(max_count_obs, ", ".join(max_years)))

# 6. Pour compter les lieux de départ sans doublons on commence par récupérer la colonne VoyageFrom, la colonne des lieux de départ
column_index_voyage_from = findCol(firstLine, "VoyageFrom")
print("{} corresponds to column {}".format("VoyageFrom", column_index_voyage_from))

# 1ere méthode: en utilisant distinct()
# on veut savoir combien de temps prend chaque méthode donc on démarre le chrono
start_time = time.time()

# on filtre les NA, puis on utilise distinct
voyage_from_distinct = entries.map(lambda x: x[column_index_voyage_from].replace(' ', '')).filter(
    lambda x: x != "NA").distinct()
# on récupère le count des destinations
distinct_count = voyage_from_distinct.count()
# on arrête le chrono
end_time = time.time()

print("Methode 1 - Nombre de lieux de destination sans doublons (méthode distinct) : {}".format(distinct_count))
# on affiche les secondes que prennent le traitement par distinct()
# et on les arrondit à 2 chiffres après la virgule
print("Methode 1 - Temps d'exécution via count : {:.2f} secondes".format(end_time - start_time))

# 2ème méthode : en utilisant reduceByKey()
start_time = time.time()

# entries.map(lambda x: (x[column_index_voyage_from].replace(' ', ''), 1)) ça permet de créer des tuples (lieu, 1)
# Depuis le début on filtre au cas où le lieu serait NA
# reduceByKey(lambda a, b: a + b) permet de sommer les 1 pour chaque lieu
# On obtient donc un RDD avec des tuples (lieu, nombre d'occurrences)
# Il ne reste plus qu'à compter le nombre de tuples avec count()
voyage_from_reduce = entries.map(lambda x: (x[column_index_voyage_from].replace(' ', ''), 1)).filter(
    lambda x: x[0] != "NA").reduceByKey(lambda a, b: a + b)
reduce_count = voyage_from_reduce.count()
end_time = time.time()

print("Method 2 - Nombre de places distinctes de départ via ReduceByKey : {}".format(reduce_count))
print("Method 2 - Temps d'exécution via ReduceByKey : {:.2f} secondes".format(end_time - start_time))
# Cette seconde méthode prend environ 2 secondes de plus, le distinct est donc plus performant dans notre cas.

# 7. Affichage des 10 lieux de départ les plus populaires (avec le plus d'occurrences)
# Utilisation du RDD créé précédemment voyage_from_reduce car il contient des tuples avec
# les lieux et le nombre d'occurrences de chaque lieu
# Application de la transformation sortByKey pour trier par nombre d'occurrences décroissant (ascending=False)
# Il ne reste plus qu'à prendre les 10 premiers avec take(10)
top_10_voyage_from = voyage_from_reduce.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)
print("Les 10 lieux de départ les plus populaires sont : {}".format(top_10_voyage_from))

# 8. Afficher les 10 routes les plus souvent empruntées

# On commence par récupérer l'index de la colonne VoyageTo
column_index_voyage_to = findCol(firstLine, "VoyageTo")
print("{} corresponds to column {}".format("VoyageTo", column_index_voyage_to))

# Version 1 : les routes sont orientées, A->B et B->A sont différentes
# on extrait (from, to) pour chaque entrée, puis on filtre les lignes avec "NA",
# on compte les occurrences par paire via reduceByKey, on trie par nombre décroissant et on prend les 10 premières
routes_directed_counts = (
    entries
    .map(lambda x: (x[column_index_voyage_from].replace(' ', ''), x[column_index_voyage_to].replace(' ', '')))
    .filter(lambda t: t[0] != "NA" and t[1] != "NA")
    .map(lambda t: (t, 1))
    .reduceByKey(lambda a, b: a + b)
)

# Enfin, on trie par ordre décroissant par compte, puis on extrait les 10 premiers
top10_directed = (
    routes_directed_counts
    .map(lambda kv: (kv[1], kv[0]))
    .sortByKey(ascending=False)
    .take(10)
)

print("Top 10 des routes orientées (Version 1) :")
for count, (frm, to) in top10_directed:
    print(f"{frm} -> {to} : {count}")

# Version 2 : les routes ne sont pas orientées, A->B et B->A sont identiques
# Pour cela, on crée une paire triée (min, max) à partir de (from, to)
# de façon à ce que A->B et B->A deviennent tous deux (A, B)

# On récupère (from, to) en supprimant les espaces, puis on enlève les NA,
# ensuite on trie la paire pour que (A, B) et (B, A) deviennent identiques
# et enfin on peut compter le nombre d’occurrences de chaque route
routes_undirected_counts = (
    entries
    .map(lambda x: (x[column_index_voyage_from].replace(' ', ''), x[column_index_voyage_to].replace(' ', '')))
    .filter(lambda t: t[0] != "NA" and t[1] != "NA")
    .map(lambda t: (tuple(sorted(t)), 1))
    .reduceByKey(lambda a, b: a + b)
)

# Ici on trie par ordre décroissant selon le nombre d’occurrences pour pouvoir extraire les 10 routes les plus empruntées
top10_undirected = (
    routes_undirected_counts
    .map(lambda kv: (kv[1], kv[0]))
    .sortByKey(ascending=False)
    .take(10)
)

print("Top 10 des routes non orientées (Version 2) :")
for count, (frm, to) in top10_undirected:
    print(f"{frm} <-> {to} : {count}")

# 9. Recherche du mois le plus chaud (colonne "Month") en moyenne au cours
# de l'année en prenant en compte toutes les températures (colonne "ProbTair") du dataset
column_index_month = findCol(firstLine, "Month")
column_index_prob_tair = findCol(firstLine, "ProbTair")

# Pour chaque opération on crée un tuple (mois, (température, 1)), après on les somme tous avec un reduceByKey
# pour obtenir en retour (mois, (somme des températures, nombre d'occurrences))
# Ensuite on peut calculer la moyenne en faisant somme des températures / nombre d'occurrences
# La commande est un peut plus complexe parce qu'on continue à gérer depuis le début le cas des valeurs NA
# Dans ce cas là on remplace le tuple par (mois, (0.0, 0)) pour ne pas fausser la moyenne
month_temp = (entries.map(lambda x: (x[column_index_month].replace(' ', ''), (
    float(x[column_index_prob_tair].replace(' ', '')), 1)) if (
            x[column_index_prob_tair].replace(' ', '') != "NA" and x[column_index_month].replace(' ',
                                                                                                 '') != "NA") else (
    x[column_index_month].replace(' ', ''), (0.0, 0))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))

# A la sortie on a des tuples (mois, (somme des températures, nombre d'occurrences))
# Filtrage des mois où il n'y a pas de données (nombre d'occurrences = 0)
month_temp_filtered = month_temp.filter(lambda x: x[1][1] > 0)
# Calcul de la moyenne pour chaque mois. Résultat sous la forme (mois, température moyenne)
month_avg_temp = month_temp_filtered.map(lambda x: (x[0], x[1][0] / x[1][1]))
# Recherche du mois avec la température moyenne la plus élevée
hottest_month = month_avg_temp.max(key=lambda x: x[1])

print(
    "Le mois le plus chaud en moyenne est le mois {} avec une température moyenne de {:.2f}°C".format(hottest_month[0],
                                                                                                      hottest_month[1]))

# traitement-distribue-esgi

## DESCRIPTION  

Programme en python en utilsant l'outil Spark pour analyser un DataSet contenant tout les commits fait sur github depuis debut jusqu'en 2017 

## OBJECTIFS 

1 -  Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de commit.

2 - Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark.

3 - Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 4 dernières années

4 - Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets (hors stopword)

Bonus
A la place d’utiliser une liste de stopwords, vous pouvez utiliser le stopWordsRemover de Spark ML.

## CONFIGURATION

Voir le docker compose


## LANCEMENT 

Commande : /spark/bin/spark-submit /app/project-final.py

## RESULTAT 

### Objectif 1 
![image](https://github.com/RemyMach/traitement-distribue-esgi/assets/60228588/109dc48e-33eb-4acb-aa6e-89def3e746d8)

### Objectif 2 
![image2](https://github.com/RemyMach/traitement-distribue-esgi/assets/60228588/c4c63019-99d7-4487-8aff-04c44b8efc09)

### Objectif 3 
![image3](https://github.com/RemyMach/traitement-distribue-esgi/assets/60228588/8e9f869e-8225-47ca-951b-33bcdb4024be)

### Objectif 4 
![image4](https://github.com/RemyMach/traitement-distribue-esgi/assets/60228588/2f1b3a02-505b-4f8d-9196-df697335b1e4)


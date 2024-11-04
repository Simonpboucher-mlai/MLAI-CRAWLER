# MLAI-CRAWLER
Ce code implémente un web crawler conçu pour explorer un site web de manière systématique et extraire le contenu textuel de pages HTML et de fichiers PDF. Il est structuré en plusieurs classes et fonctions pour organiser efficacement les tâches, gérer les liens visités, et stocker le texte extrait dans des fichiers. Voici une description détaillée des différentes parties du code :

### 1. **Importations et configuration initiale**
   - Les bibliothèques `requests` et `BeautifulSoup` sont utilisées pour envoyer des requêtes HTTP et analyser le contenu HTML.
   - `pdfplumber` est utilisé pour extraire du texte et des tableaux des fichiers PDF.
   - `logging` configure un système de journalisation (`crawler_logger`) qui enregistre les événements du crawler dans un fichier `crawler_log.txt` et affiche les logs dans la console.
   - `hashlib`, `sqlite3`, et `threading` permettent de gérer les fichiers, les verrous pour accès simultané aux données et la base de données SQLite.
   - `ThreadPoolExecutor` et `Queue` gèrent les threads, ce qui permet au crawler de fonctionner en parallèle pour accélérer le traitement des pages.

### 2. **Configuration de la journalisation (logging)**
   - Un logger nommé `crawler_logger` est configuré pour enregistrer les événements importants avec des niveaux d'information et d'erreur. Les messages sont formatés pour inclure l'horodatage et le niveau de gravité, ce qui aide à diagnostiquer les problèmes.

### 3. **Classe `DatabaseHandler`**
   - Cette classe gère la base de données SQLite où sont enregistrées les URLs visitées, ce qui permet d’éviter de crawler plusieurs fois la même page.
   - **Méthode `setup_database`** : crée une table `visited_urls` si elle n'existe pas encore, avec une clé primaire pour l'URL. 
   - **Méthode `is_visited`** : vérifie si une URL a déjà été visitée en utilisant un verrou (`lock`) pour éviter les conflits d’accès.
   - **Méthode `mark_as_visited`** : ajoute l’URL à la table `visited_urls` pour marquer la page comme visitée, en utilisant également un verrou pour la sécurité thread-safe.
   - **Méthode `close`** : ferme la connexion à la base de données une fois le crawl terminé.

### 4. **Classe `Crawler`**
   - Cette classe est le cœur du crawler. Elle gère l’extraction de texte et la gestion des liens.
   - **Attributs d'initialisation** : `start_url`, `local_domain`, et `base_url` sont utilisés pour définir le point de départ et la portée du crawl. Une session `requests` est créée avec un en-tête `User-Agent` pour se présenter comme un crawler. Le nombre de threads (`max_workers`) et le dossier de sortie sont également définis.

#### Principales Méthodes :

   - **`sanitize_filename`** : nettoie et tronque les noms de fichiers pour éviter les problèmes liés aux caractères spéciaux. Cette méthode génère un nom de fichier unique basé sur un hash de l’URL.

   - **`extract_text_from_pdf`** : utilise `pdfplumber` pour ouvrir et lire un fichier PDF en mémoire. Elle extrait le texte et les tableaux des pages, en structurant le texte de façon lisible et en incluant des tableaux extraits dans le texte sous forme de sections structurées. Les erreurs d’extraction sont journalisées.

   - **`clean_text`** : nettoie le texte extrait en supprimant les espaces en trop et en normalisant l’espacement.

   - **`extract_text_from_html`** : analyse le contenu HTML avec `BeautifulSoup`, élimine les éléments de script et de style, puis extrait le texte des paragraphes (`p`), titres (`h1` à `h6`), et listes (`li`). Le texte est nettoyé et formaté.

   - **`extract_text_alternative`** : utilise une méthode d'extraction alternative pour les cas où l'extraction principale ne retourne aucun contenu significatif, en récupérant l’ensemble du texte brut de la page.

   - **`normalize_url`** : normalise les URLs en supprimant les barres obliques finales, ce qui facilite la gestion des doublons.

   - **`get_hyperlinks`** : récupère tous les liens (`href`) dans une page et vérifie si le contenu est de type HTML avant de l’analyser. Si le type de contenu est incorrect ou si une erreur survient, elle est journalisée.

   - **`get_domain_hyperlinks`** : filtre les liens récupérés pour ne conserver que ceux appartenant au domaine local. Elle ignore les liens internes (ancrages `#`) et les adresses mail, puis utilise `urljoin` pour compléter les liens relatifs. 

   - **`crawl_page`** : principale méthode de crawl d’une page :
       1. Elle normalise l’URL, vérifie si elle a déjà été visitée, et, si non, la marque comme visitée.
       2. Elle télécharge le contenu de la page, vérifie le type de contenu (`HTML` ou `PDF`), puis extrait le texte en fonction du format.
       3. Le contenu extrait est stocké sous forme de fichier texte dans le dossier de sortie.
       4. Elle récupère les nouveaux liens du domaine et les ajoute à la queue si ils n'ont pas encore été visités.

   - **`crawl`** : méthode de contrôle du crawler. Elle utilise `ThreadPoolExecutor` pour créer un pool de threads et lancer l'exploration des pages dans des threads simultanés. Chaque URL dans la queue de liens est traitée par un thread jusqu’à ce que la queue soit vide, avec un délai entre chaque cycle pour éviter de surcharger le serveur. Une fois le crawl terminé, elle ferme la base de données.

### 5. **Exécution principale (`__main__`)**
   - Définit l’URL de départ (`START_URL`) et le nombre maximal de threads (`MAX_WORKERS`) pour le crawl.
   - Instancie un objet `Crawler` et lance la méthode `crawl` pour démarrer l’exploration du site.


## How to use it 
Pour exécuter ce web crawler, suivez les étapes ci-dessous :

### 1. **Installer les dépendances**
   Avant de lancer le script, vous devez installer les bibliothèques Python nécessaires. Ce script utilise plusieurs packages externes, donc vérifiez qu’ils sont installés avec la commande suivante :

   ```bash
   pip install requests beautifulsoup4 pdfplumber lxml
   ```

   Ces bibliothèques comprennent :
   - **requests** : pour effectuer les requêtes HTTP.
   - **beautifulsoup4** et **lxml** : pour analyser le contenu HTML.
   - **pdfplumber** : pour extraire le texte et les tableaux des fichiers PDF.

### 2. **Configurer le script**
   Ouvrez le script et vérifiez les éléments suivants :

   - **URL de départ** : Remplacez `START_URL` par l'URL que vous souhaitez explorer. Assurez-vous que l'URL est complète et bien formatée (par exemple, `https://www.votresite.com`).
   
     ```python
     START_URL = "https://www.votresite.com"
     ```

   - **Nombre de threads (MAX_WORKERS)** : Ajustez le nombre de threads (`MAX_WORKERS`) en fonction de votre machine et de la capacité du serveur que vous explorez. Un nombre plus élevé de threads permet de crawler plus vite, mais peut surcharger le serveur et entraîner des blocages ou des limitations.

     ```python
     MAX_WORKERS = 10  # Ajustez selon les ressources disponibles et les règles d'accès du site cible.
     ```

### 3. **Exécuter le script**
   Assurez-vous de vous placer dans le dossier où se trouve le script, puis lancez le crawler avec la commande suivante dans le terminal :

   ```bash
   python mon_crawler.py
   ```

   > Remplacez `mon_crawler.py` par le nom du fichier dans lequel vous avez sauvegardé ce code.

### 4. **Observer les logs**
   Le script enregistre les événements importants dans un fichier `crawler_log.txt` situé dans le même dossier que le script. Vous pouvez également suivre le déroulement du crawl dans la console, car les logs sont configurés pour y être affichés.

   - **Informations des logs** : vous y verrez des messages pour chaque page explorée, les erreurs rencontrées, et des détails sur les pages non trouvées ou les erreurs de connexion. 

### 5. **Vérifier les résultats**
   - Les résultats sont sauvegardés dans un dossier `text/` qui se crée automatiquement dans le dossier de votre script. Dans ce dossier, vous trouverez un sous-dossier nommé en fonction du domaine du site exploré (par exemple, `text/votresite.com`).
   - Chaque page explorée est enregistrée sous forme de fichier texte `.txt`, dont le nom est une version normalisée de l'URL de la page.

### 6. **Arrêter le script**
   Le script continue d'explorer les pages jusqu'à ce qu'il n’y ait plus de nouvelles URLs dans la queue. Si vous souhaitez arrêter le crawler manuellement, vous pouvez le faire avec un **Ctrl+C** dans le terminal, ce qui terminera le processus.

### 7. **Relancer le crawler**
   Si vous souhaitez relancer le crawler ultérieurement, il ne revisitera pas les pages déjà explorées grâce à la base de données SQLite qui garde une trace des URLs visitées. Assurez-vous que le fichier de base de données `crawler.db` est toujours dans le dossier de travail pour que le script continue là où il s'est arrêté.

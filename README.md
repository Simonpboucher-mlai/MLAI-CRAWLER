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

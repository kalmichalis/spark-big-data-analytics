# spark-big-data-analytics

Big data analytics with Apache Spark & Scala. Includes RDD-based word pair counting, market basket frequent itemset mining, IMDB movie analysis (avg score per year, top movie per country), and COVID-19 vaccination data analysis.

Αυτό το repository περιέχει τις εργασίες μου στο πλαίσιο του **Ελληνικού Ανοικτού Πανεπιστημίου (ΕΑΠ)** για το μάθημα Big Data Analytics, υλοποιημένες σε **Apache Spark** με **Scala** σε περιβάλλον Ubuntu VM.

---

## 📁 Περιεχόμενα

```text
spark-big-data-analytics/
│
├── data/                           # Αρχεία δεδομένων
│   ├── Shakespeare.txt
│   ├── SherlockHolmes.txt
│   ├── groceries.csv
│   ├── movies.csv
│   └── country_vaccinations_by_manufacturer.csv
│
├── src/main/scala/
│   ├── assignment1/
│   │   └── WordPairApp.scala       # Word Pairs Analysis
│   ├── assignment2/
│   │   └── MarketBasketApp.scala   # Market Basket Analysis
│   ├── assignment3/
│   │   └── IMDB.scala              # IMDB Movie Analysis
│   ├── assignment4/
│   │   └── Covid.scala             # COVID-19 Vaccination Analysis
│   └── utils/
│       ├── SimpleApp.scala         # Testing
│       └── HelloWorld.scala        # Testing
│
├── build.sbt
├── README.md
└── LICENSE
```

---

## 🔍 Projects

### 1. Word Pairs Analysis

**Αρχείο:** `src/main/scala/assignment1/WordPairApp.scala`

Ανάλυση κειμένου με τα εξής βήματα:
- Αφαίρεση σημείων στίξης και μετατροπή σε πεζούς
- Καταμέτρηση ζευγαριών λέξεων που εμφανίζονται μαζί στην ίδια γραμμή
- Φιλτράρισμα μόνο για λέξεις μήκους ≥ 3 χαρακτήρες
- Εμφάνιση των 5 συχνότερων ζευγαριών

**Δεδομένα:** `data/Shakespeare.txt`, `data/SherlockHolmes.txt`

```bash
spark-submit \
  --class assignment1.WordPairApp \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/Shakespeare.txt
```

---

### 2. Market Basket Analysis

**Αρχείο:** `src/main/scala/assignment2/MarketBasketApp.scala`

Ανάλυση καλαθιών αγορών από supermarket:
- Κάθε γραμμή = ένα καλάθι προϊόντων
- Εύρεση όλων των ζευγαριών και τριάδων προϊόντων που αγοράζονται μαζί
- Εμφάνιση μόνο των συνδυασμών που εμφανίζονται τουλάχιστον s φορές (παράμετρος)
- Παράδειγμα: (βούτυρο, γάλα) → 4 εμφανίσεις

**Δεδομένα:** `data/groceries.csv`

```bash
spark-submit \
  --class assignment2.MarketBasketApp \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/groceries.csv 3
```

---

### 3. IMDB Movie Analysis

**Αρχείο:** `src/main/scala/assignment3/IMDB.scala`

Στατιστική ανάλυση της βάσης ταινιών του IMDB:
- Μέσο σκορ και πλήθος ταινιών ανά έτος κυκλοφορίας
- Για κάθε χώρα, η ταινία (τίτλος + έτος) με το υψηλότερο σκορ
- Εύρεση όλων των ζευγαριών ταινιών με διαφορά σκορ ≤ 1

**Δεδομένα:** `data/movies.csv`

```bash
spark-submit \
  --class assignment3.IMDB \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/movies.csv
```

---

### 4. COVID-19 Vaccination Analysis

**Αρχείο:** `src/main/scala/assignment4/Covid.scala`

Ανάλυση δεδομένων εμβολιασμού κατά του COVID-19:
- Υπολογισμός μέσου όρου ημερήσιων εμβολιασμών (συνολικά, για 2021, για 2022)
- Για κάθε χώρα, πόσες ημέρες ξεπέρασαν τον παγκόσμιο μέσο όρο
- Για κάθε χώρα, το εμβόλιο που χρησιμοποιήθηκε περισσότερο και λιγότερο

**Δεδομένα:** `data/country_vaccinations_by_manufacturer.csv`

```bash
spark-submit \
  --class assignment4.Covid \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/country_vaccinations_by_manufacturer.csv
```

---

## ▶️ Πώς να τρέξετε όλα τα projects

### Build

```bash
sbt clean package
```

### Εκτέλεση

```bash
# Word Pairs Analysis
spark-submit \
  --class assignment1.WordPairApp \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/Shakespeare.txt

# Market Basket Analysis (minSupport = 3)
spark-submit \
  --class assignment2.MarketBasketApp \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/groceries.csv 3

# IMDB Movie Analysis
spark-submit \
  --class assignment3.IMDB \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/movies.csv

# COVID-19 Vaccination Analysis
spark-submit \
  --class assignment4.Covid \
  target/scala-2.12/spark-big-data-analytics_2.12-1.0.jar \
  data/country_vaccinations_by_manufacturer.csv
```

### Διαθέσιμα ClassName

- `assignment1.WordPairApp`
- `assignment2.MarketBasketApp`
- `assignment3.IMDB`
- `assignment4.Covid`

---

## 📝 Σημειώσεις

- Για το Market Basket Analysis, η παράμετρος `3` είναι το ελάχιστο support (minSupport)
- Το JAR name μπορεί να διαφέρει ανάλογα με το `name` και `version` στο `build.sbt`


# :bar_chart: Political Spanish Sentiment
The goal of this project to analyze the social media activy of this profiles and the opinion that they generate into the Spanish population.

To achieve this, I have created a pipeline that extracts tweets of different accounts (political parties and their representatives) and mentions of users about them, and also 


![alt text](https://images.pexels.com/photos/3943746/pexels-photo-3943746.jpeg?auto=compress&cs=tinysrgb&dpr=3&h=750&w=1260)


# :start: Sentiment Analysis (NLP) of Twitter mentions
The model used to analyze the twitter mentions has been bert-base-multilingual-uncased-sentiment, that allows to have a classification of 1-5 stars of a text.

This a model was trained with product reviews in six languages: English, Dutch, German, French, Spanish and Italian, predicting the sentiment of the review as a number of stars (between 1 and 5).


# :fast_forward: How it works
The main script is locally executed every day in order to get the tweets and mentions of the last day. If you want to execute it, just write:

```
python main.py
```



# :computer: Technology stack
- Python==3.8.5
- pandas==1.1.3 
- numpy==1.19.2
- tweepy==3.10.0
- transformers==4.5.1
- regex==2020.10.15


# :zap: Tableau Dashboard
The result of this analysis it's showed in this [Tableau Dashboard](https://public.tableau.com/profile/sara.hern.ndez#!/vizhome/ih_datamadpt0420_project_m2_16141539604710/DiamondDashboard?publish=yes). 


# :file_folder: Folder structure
```
└── project
    ├── __trash__
    ├── .gitignore
    ├── requirements.txt
    ├── README.md
    ├── main.py
    ├── p_acquisition
    │   ├── __init__.py
    │   └── m_acquisition.py
    ├── p_wrangling
    │   ├── __init__.py
    │   └── m_wrangling.py
    ├── p_analysis
    │   ├── __init__.py
    │   └── m_analysis.py
    ├── p_reporting
    │   ├── __init__.py
    │   └── m_reporting.py
    └── data
        ├── raw
        ├── processed
        └── results
```

# :incoming_envelope: Contact info
If you have some question, drop me a line! (sara.shr5@gmail.com)



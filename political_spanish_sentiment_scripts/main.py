import pandas as pd
import tweepy
import functions as fc
import numpy as np
import os
from os.path import join, dirname
from dotenv import load_dotenv

CONSUMER_KEY = os.environ.get("CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")

AUTH = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
AUTH.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
API = tweepy.API(AUTH, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# MENTIONS

# Menciones VOX General
print('Extracting Vox mentions...')
vox_query = '@vox_es OR "vox" OR "los de vox" OR "el de VOX" OR "la de VOX" ' \
            'OR "Santiago Abascal" OR "Rocío Monasterio" -filter:retweets'
df_vox_mentions = fc.extracting_mentions(vox_query, 'VOX', 'mención')
#df_vox_mentions.to_csv('../data/data_bases/mentions_of_today.csv')


# Menciones Más Madrid
print('Extracting mentions of Más Madrid...')
masmad_query = '@MasMadridCM OR "Mas Madrid" OR "Mónica García" OR "Errejón" -filter:retweets'
df_masmad_mentions = fc.extracting_mentions(masmad_query, 'Más Madrid', 'mención')
#df_masmad_mentions.to_csv('../data/data_bases/mentions_of_today.csv', mode='a', header=False)


# Menciones PP General
print('Extracting mentions of PP...')
pp_query = '@populares OR "partido popular" OR "el pp" OR "los del pp" ' \
           'OR "Ayuso" OR "Isabel Díaz Ayuso" OR "Pablo Casado" -filter:retweets'
df_pp_mentions = fc.extracting_mentions(pp_query, 'Partido Popular', 'mención')
#df_pp_mentions.to_csv('../data/data_bases/mentions_of_today.csv', mode='a', header=False)


# Menciones PSOE General
print('Extracting mentions of PSOE...')
psoe_query = '@psoe OR "partido socialista" OR "Partido Socialista Obrero Español" ' \
             'OR "el psoe" OR "los del psoe" OR "Pedro Sánchez" OR "Ángel Gabilondo" -filter:retweets'
df_psoe_mentions = fc.extracting_mentions(psoe_query, 'PSOE','mención')
#df_psoe_mentions.to_csv('../data/data_bases/mentions_of_today.csv', mode='a', header=False)


# Menciones CIUDADANOS General
print('Extracting mentions of Ciudadanos...')
ciudadanos_query = '@CiudadanosCs OR "ciudadanos" OR "los de ciudadanos" OR "Inés Arrimadas" ' \
                   'OR "Edmundo Bal" -filter:retweets'
df_ciudadanos_mentions = fc.extracting_mentions(ciudadanos_query, 'CIUDADANOS', 'mención')
#df_ciudadanos_mentions.to_csv('../data/data_bases/mentions_of_today.csv', mode='a', header=False)


# Menciones PODEMOS General
print('Extracting mentions of Podemos...')
podemos_query = '@PODEMOS OR "podemos" OR "los de podemos" OR "podemitas" OR "el de podemos" ' \
                'OR "el coletas" OR "Pablo Iglesias" OR "el iglesias -filter:retweets'
df_podemos_mentions = fc.extracting_mentions(podemos_query, 'PODEMOS', 'mención')
#df_podemos_mentions.to_csv('../data/data_bases/mentions_of_today.csv', mode='a', header=False)



# DATA FRAME ALL MENTIONS
print('Putting together df_all_mentions...')
frames_mentions = [df_pp_mentions, df_psoe_mentions, df_podemos_mentions,
                   df_ciudadanos_mentions, df_vox_mentions, df_masmad_mentions]

df_final_mentions = pd.concat(frames_mentions)


# MACHINE LEARNING SENTIMENT --> MENTIONS
print('Applying Sentiment Analysis to mentions...')

print('Splitting the final data frame into 50 sections...')

list_dfs = np.array_split(df_final_mentions, 50)

for idx in range(len(list_dfs)):
  dataframe = list_dfs[idx]
  dataframe_ml = fc.sentiment_classificaton(dataframe, dataframe['full_text'])
  dataframe_ml.to_csv('../data/data_bases/political_spanish_sentiment_ddbb.csv', mode='a', header=False, index=False)
  print(f'{idx}')


print('Mentions and Sentiment Analysis Added to Data Base!')

"""

# TWEETS OF PARTIES -> GENERAL

print('Reading the data base...')
df_bbdd = pd.read_csv('../data/data_bases/political_spanish_sentiment_ddbb.csv', index_col=0)


print('Extracting tweets of parties and applying sentiment analysis...')

# Partido Popular (General)
df_pp = fc.extracting_tweets(df_bbdd, 'populares', 'Partido Popular', 'publicación', 'general')

# PSOE (General)
df_psoe = fc.extracting_tweets(df_bbdd, 'psoe', 'PSOE', 'publicación', 'general')

# PODEMOS (General)
df_podemos = fc.extracting_tweets(df_bbdd, 'PODEMOS', 'PODEMOS', 'publicación', 'general')

# CIUDADANOS (General)
df_ciudadanos = fc.extracting_tweets(df_bbdd, 'ciudadanoscs', 'CIUDADANOS', 'publicación', 'general')

# VOX (General)
df_vox = fc.extracting_tweets(df_bbdd, 'vox_es', 'VOX', 'publicación', 'general')




# TWEETS OF PARTIES -> MADRID
print('Extracting tweets of Madrid campaign parties...')

# Partido Popular (Madrid)
df_pp_madrid = fc.extracting_tweets(df_bbdd, 'ppmadrid', 'Partido Popular', 'publicación', 'madrid')

# PSOE (Madrid)
df_psoe_madrid = fc.extracting_tweets(df_bbdd, 'psoe_m', 'PSOE', 'publicación', 'madrid')

# PODEMOS (Madrid)
df_podemos_madrid = fc.extracting_tweets(df_bbdd, 'podemosmad', 'PODEMOS', 'publicación', 'madrid')

# CIUDADANOS (Madrid)
df_ciudadanos_madrid = fc.extracting_tweets(df_bbdd, 'CsMadridCiudad', 'CIUDADANOS', 'publicación', 'madrid')

# Más Madrid (Madrid)
df_masmad_madrid = fc.extracting_tweets(df_bbdd, 'MasMadridCM', 'Más Madrid', 'publicación', 'madrid')

# VOX (Madrid)
df_vox_madrid = fc.extracting_tweets(df_bbdd, 'VOX_AytoMadrid', 'VOX', 'publicación', 'madrid')




# TWEETS OF CANDIDATES AND SECRETARIES

print('Extracting tweets of leaders...')

# Pablo Casado (Partido Popular) (General)
df_pcasado = fc.extracting_tweets(df_bbdd, 'pablocasado_', 'Partido Popular', 'publicación', 'madrid')

# Pedro Sánchez (PSOE) (General)
df_psanchez = fc.extracting_tweets(df_bbdd, 'sanchezcastejon', 'PSOE', 'publicación', 'madrid')

# Pablo Iglesias (PODEMOS) (General)
df_piglesias = fc.extracting_tweets(df_bbdd, 'PabloIglesias', 'PODEMOS', 'publicación', 'general')

# Inés Arrimadas (CIUDADANOS) (General)
df_iarrimadas = fc.extracting_tweets(df_bbdd, 'InesArrimadas', 'CIUDADANOS', 'publicación', 'general')

# Santiago Abascal (VOX) (General)
df_abascal = fc.extracting_tweets(df_bbdd, 'Santi_ABASCAL', 'VOX', 'publicación', 'general')

# Isabel Díaz Ayuso (Partido Popular) (Madrid)
df_ayuso = fc.extracting_tweets(df_bbdd, 'IdiazAyuso', 'Partido Popular', 'publicación', 'madrid')

# Ángel Gabilondo (PSOE) (Madrid)
df_gabilondo = fc.extracting_tweets(df_bbdd, 'equipoGabilondo', 'PSOE', 'publicación', 'madrid')

# Rocío Monasterio (VOX) (Madrid)
df_monasterio = fc.extracting_tweets(df_bbdd, 'monasterioR', 'VOX', 'publicación', 'madrid')

# Mónica García (Más Madrid) (Madrid)
df_mgarcia = fc.extracting_tweets(df_bbdd, 'Monica_Garcia_G', 'Más Madrid', 'publicación', 'madrid')

# Edmundo (Ciudadanos) (Madrid)
df_edmundo = fc.extracting_tweets(df_bbdd, 'BalEdmundo', 'CIUDADANOS', 'publicación', 'madrid')

# DATA FRAME ALL TWEETS

print('Creating df_final_tweets...')

frames_tweets = [df_pp, df_psoe, df_ciudadanos, df_podemos, df_vox,
          df_ciudadanos_madrid, df_masmad_madrid, df_pp_madrid, df_podemos_madrid, df_vox_madrid,
          df_pcasado, df_psanchez, df_piglesias, df_abascal, df_iarrimadas,
          df_ayuso, df_gabilondo, df_mgarcia, df_monasterio, df_edmundo]

# he quitado el dataframe del psoe. Revisar y añadir de nuevo.

df_final_tweets = pd.concat(frames_tweets)
df_final_tweets.to_csv('../data/data_bases/political_spanish_sentiment_ddbb.csv', mode='a', header=False, index=False)

print('Data Base updated!')

"""


if __name__ == '__main__':
    print('Pipeline finished')

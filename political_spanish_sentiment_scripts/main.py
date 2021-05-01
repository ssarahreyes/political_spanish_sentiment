import pandas as pd
import tweepy
import functions as fc
import numpy as np

""""
# MENTIONS

print('Extracting Vox mentions...')

# Menciones VOX General
vox_query = "
@vox_es OR "vox" OR "los de vox" OR "el de VOX" OR "la de VOX"
OR "Santiago Abascal" OR "Rocío Monasterio" -filter:retweets
"
df_vox_mentions = fc.extracting_mentions(vox_query)
df_vox_mentions = fc.adding_party_column(df_vox_mentions, 'VOX')
df_vox_mentions = fc.adding_type_post_column(df_vox_mentions, 'mención')
df_vox_mentions['hashtags'] = fc.extract_hashtags(df_vox_mentions['full_text'])
fc.adding_campaign_column(df_vox_mentions, 'general')

df_vox_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of Más Madrid...')

# menciones Más Madrid
masmad_query = '@MasMadridCM OR "Mas Madrid" OR "Mónica García"  -filter:retweets'
df_masmad_mentions = fc.extracting_mentions(masmad_query)
df_masmad_mentions = fc.adding_party_column(df_masmad_mentions, 'Más Madrid')
df_masmad_mentions = fc.adding_type_post_column(df_masmad_mentions, 'mención')
df_masmad_mentions['hashtags'] = fc.extract_hashtags(df_masmad_mentions['full_text'])
fc.adding_campaign_column(df_masmad_mentions, 'general')

df_masmad_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of PP...')

# Menciones PP General
pp_query = '@populares OR "partido popular" OR "el pp" OR "los del pp" -filter:retweets'
df_pp_mentions = fc.extracting_mentions(pp_query)
df_pp_mentions = fc.adding_party_column(df_pp_mentions, 'Partido Popular')
df_pp_mentions = fc.adding_type_post_column(df_pp_mentions, 'mención')
df_pp_mentions['hashtags'] = fc.extract_hashtags(df_pp_mentions['full_text'])
fc.adding_campaign_column(df_pp_mentions, 'general')

df_pp_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of PSOE...')

# Menciones PSOE General
psoe_query = "
@psoe OR "partido socialista" OR "Partido Socialista Obrero Español" 
OR "el psoe" OR "los del psoe" OR "Pedro Sánchez" OR "Ángel Gabilondo" -filter:retweets
"
df_psoe_mentions = fc.extracting_mentions(psoe_query)
df_psoe_mentions = fc.adding_party_column(df_psoe_mentions, 'PSOE')
df_psoe_mentions = fc.adding_type_post_column(df_psoe_mentions, 'mención')
df_psoe_mentions['hashtags'] = fc.extract_hashtags(df_psoe_mentions['full_text'])
fc.adding_campaign_column(df_psoe_mentions, 'general')

df_psoe_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of Ciudadanos...')

# Menciones CIUDADANOS General
ciudadanos_query = "
@CiudadanosCs OR "ciudadanos" OR "los de ciudadanos" OR "Inés Arrimadas"
OR "Edmundo Bal" -filter:retweets
"
df_ciudadanos_mentions = fc.extracting_mentions(ciudadanos_query)
df_ciudadanos_mentions = fc.adding_party_column(df_ciudadanos_mentions, 'CIUDADANOS')
df_ciudadanos_mentions = fc.adding_type_post_column(df_ciudadanos_mentions, 'mención')
df_ciudadanos_mentions['hashtags'] = fc.extract_hashtags(df_ciudadanos_mentions['full_text'])
fc.adding_campaign_column(df_ciudadanos_mentions, 'general')

df_ciudadanos_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of Podemos...')

# Menciones PODEMOS General

podemos_query = "
@PODEMOS OR "podemos" OR "los de podemos" OR "podemitas" OR "el de podemos" -filter:retweets
OR "el coletas" OR "Pablo Iglesias" OR "el iglesias"
"
df_podemos_mentions = fc.extracting_mentions(podemos_query)
df_podemos_mentions = fc.adding_party_column(df_podemos_mentions, 'PODEMOS')
df_podemos_mentions = fc.adding_type_post_column(df_podemos_mentions, 'mención')
df_podemos_mentions['hashtags'] = fc.extract_hashtags(df_podemos_mentions['full_text'])
fc.adding_campaign_column(df_podemos_mentions, 'general')

df_podemos_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)


print('Creating df_all_mentions...')

# DATA FRAME ALL MENTIONS
frames_mentions = [df_pp_mentions, df_psoe_mentions, df_podemos_mentions,
                   df_ciudadanos_mentions, df_vox_mentions, df_masmad_mentions]

df_final_mentions = pd.concat(frames_mentions)

df_final_mentions.to_csv('political_spanish_sentiment_mentions_all.csv', mode='a', header=False)



# TWEETS OF PARTIES -> GENERAL

"""

print('reading the data frame...')

df_ddbb = pd.read_csv('political_spanish_sentiment_ddbb.csv')

"""
print('Extracting tweets of parties...')


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
          df_ciudadanos_madrid, df_masmad_madrid, df_pp_madrid, df_psoe_madrid, df_podemos_madrid, df_vox_madrid,
          df_pcasado, df_psanchez, df_piglesias, df_abascal, df_iarrimadas,
          df_ayuso, df_gabilondo, df_mgarcia, df_monasterio, df_edmundo]

df_final_tweets = pd.concat(frames_tweets)
df_final_tweets.to_csv('political_spanish_sentiment_tweets.csv', mode='a', header=False)

# UPDATING THE FINAL DATA BASE WITH TWEETS
df_final_tweets.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('df_final_tweets and ddbb updated!')


"""

# MACHINE LEARNING SENTIMENT
print('Applying ML to Sentiment Analysis...')

list_dfs = np.array_split(df_ddbb, 1000)

for idx in range(len(list_dfs)):
  dataframe = list_dfs[idx]
  dataframe_ml = fc.sentiment_classificaton(dataframe, dataframe['full_text'])
  dataframe_ml.to_csv('political_spanish_sentiment_ddbb_ml_2.csv', mode='a', header=False)
  print(f'{idx}')

print('Finished!')


if __name__ == '__main__':
    print('Pipeline finished')

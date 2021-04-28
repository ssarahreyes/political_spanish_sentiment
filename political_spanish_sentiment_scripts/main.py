import pandas as pd
import tweepy
import functions as fc


# MENTIONS

print('Extracting Vox mentions...')

# Menciones VOX General
vox_query = """
@vox_es OR "vox" OR "los de vox" OR "el de VOX" OR "la de VOX"
OR "Santiago Abascal" OR "Rocío Monasterio" -filter:retweets
"""
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
psoe_query = """
@psoe OR "partido socialista" OR "Partido Socialista Obrero Español" 
OR "el psoe" OR "los del psoe" OR "Pedro Sánchez" OR "Ángel Gabilondo" -filter:retweets
"""
df_psoe_mentions = fc.extracting_mentions(psoe_query)
df_psoe_mentions = fc.adding_party_column(df_psoe_mentions, 'PSOE')
df_psoe_mentions = fc.adding_type_post_column(df_psoe_mentions, 'mención')
df_psoe_mentions['hashtags'] = fc.extract_hashtags(df_psoe_mentions['full_text'])
fc.adding_campaign_column(df_psoe_mentions, 'general')

df_psoe_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of Ciudadanos...')

# Menciones CIUDADANOS General
ciudadanos_query = """
@CiudadanosCs OR "ciudadanos" OR "los de ciudadanos" OR "Inés Arrimadas"
OR "Edmundo Bal" -filter:retweets
"""
df_ciudadanos_mentions = fc.extracting_mentions(ciudadanos_query)
df_ciudadanos_mentions = fc.adding_party_column(df_ciudadanos_mentions, 'CIUDADANOS')
df_ciudadanos_mentions = fc.adding_type_post_column(df_ciudadanos_mentions, 'mención')
df_ciudadanos_mentions['hashtags'] = fc.extract_hashtags(df_ciudadanos_mentions['full_text'])
fc.adding_campaign_column(df_ciudadanos_mentions, 'general')

df_ciudadanos_mentions.to_csv('political_spanish_sentiment_ddbb.csv', mode='a', header=False)

print('Extracting mentions of Podemos...')

# Menciones PODEMOS General

podemos_query = """
@PODEMOS OR "podemos" OR "los de podemos" OR "podemitas" OR "el de podemos" -filter:retweets
OR "el coletas" OR "Pablo Iglesias" OR "el iglesias"
"""
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

df_final_mentions.to_csv('political_spanish_sentiment_mentions_all.csv')
""""

# TWEETS OF PARTIES -> GENERAL

print('Extracting tweets of parties...')

# Partido Popular (General)
df_pp = fc.extracting_tweets('populares')
fc.adding_party_column(df_pp, 'Partido Popular')
fc.adding_type_post_column(df_pp, 'publicación')
df_pp['hashtags'] = fc.extract_hashtags(df_pp['full_text'])
fc.adding_campaign_column(df_pp, 'general')

# PSOE (General)
df_psoe = fc.extracting_tweets('psoe')
fc.adding_party_column(df_psoe, 'PSOE')
fc.adding_type_post_column(df_psoe, 'publicación')
df_psoe['hashtags'] = fc.extract_hashtags(df_psoe['full_text'])
fc.adding_campaign_column(df_psoe, 'general')

# PODEMOS (General)
df_podemos = fc.extracting_tweets('PODEMOS')
fc.adding_party_column(df_podemos, 'PODEMOS')
fc.adding_type_post_column(df_podemos, 'publicación')
df_podemos['hashtags'] = fc.extract_hashtags(df_podemos['full_text'])
fc.adding_campaign_column(df_podemos, 'general')

# CIUDADANOS (General)
df_ciudadanos = fc.extracting_tweets('ciudadanoscs')
fc.adding_party_column(df_ciudadanos, 'CIUDADANOS')
fc.adding_type_post_column(df_ciudadanos, 'publicación')
df_ciudadanos['hashtags'] = fc.extract_hashtags(df_ciudadanos['full_text'])
fc.adding_campaign_column(df_ciudadanos, 'general')

# VOX (General)
df_vox = fc.extracting_tweets('vox_es')
fc.adding_party_column(df_vox, 'VOX')
fc.adding_type_post_column(df_vox, 'publicación')
df_vox['hashtags'] = fc.extract_hashtags(df_vox['full_text'])
fc.adding_campaign_column(df_vox, 'general')

# TWEETS OF PARTIES -> MADRID

# Partido Popular (Madrid)
df_pp_madrid = fc.extracting_tweets('ppmadrid')
fc.adding_party_column(df_pp_madrid, 'Partido Popular')
fc.adding_type_post_column(df_pp_madrid, 'publicación')
df_pp_madrid['hashtags'] = fc.extract_hashtags(df_pp_madrid['full_text'])
fc.adding_campaign_column(df_pp_madrid, 'madrid')

# PSOE (Madrid)
df_psoe_madrid = fc.extracting_tweets('psoe_m')
fc.adding_party_column(df_psoe_madrid, 'PSOE')
fc.adding_type_post_column(df_psoe_madrid, 'publicación')
df_psoe_madrid['hashtags'] = fc.extract_hashtags(df_psoe_madrid['full_text'])
fc.adding_campaign_column(df_psoe_madrid, 'madrid')

# PODEMOS (Madrid)
df_podemos_madrid = fc.extracting_tweets('podemosmad')
fc.adding_party_column(df_podemos_madrid, 'PODEMOS')
fc.adding_type_post_column(df_podemos_madrid, 'publicación')
df_podemos_madrid['hashtags'] = fc.extract_hashtags(df_podemos_madrid['full_text'])
fc.adding_campaign_column(df_podemos_madrid, 'madrid')

# CIUDADANOS (Madrid)
df_ciudadanos_madrid = fc.extracting_tweets('CsMadridCiudad')
fc.adding_party_column(df_ciudadanos_madrid, 'CIUDADANOS')
fc.adding_type_post_column(df_ciudadanos_madrid, 'publicación')
df_ciudadanos_madrid['hashtags'] = fc.extract_hashtags(df_ciudadanos_madrid['full_text'])
fc.adding_campaign_column(df_ciudadanos_madrid, 'madrid')

# Más Madrid (Madrid)
df_masmad_madrid = fc.extracting_tweets('MasMadridCM')
fc.adding_party_column(df_masmad_madrid, 'Más Madrid')
fc.adding_type_post_column(df_masmad_madrid, 'publicación')
df_masmad_madrid['hashtags'] = fc.extract_hashtags(df_masmad_madrid['full_text'])
fc.adding_campaign_column(df_masmad_madrid, 'madrid')

# VOX (Madrid)
df_vox_madrid = fc.extracting_tweets('VOX_AytoMadrid')
fc.adding_party_column(df_vox_madrid, 'VOX')
fc.adding_type_post_column(df_vox_madrid, 'publicación')
df_vox_madrid['hashtags'] = fc.extract_hashtags(df_vox_madrid['full_text'])
fc.adding_campaign_column(df_vox_madrid, 'madrid')

# TWEETS OF CANDIDATES AND SECRETARIES

print('Extracting tweets of leaders...')

# Pablo Casado (Partido Popular) (General)
df_pcasado = fc.extracting_tweets('pablocasado_')
fc.adding_party_column(df_pcasado, 'Partido Popular')
fc.adding_type_post_column(df_pcasado, 'publicación')
df_pcasado['hashtags'] = fc.extract_hashtags(df_pcasado['full_text'])
fc.adding_campaign_column(df_pcasado, 'general')

# Pedro Sánchez (PSOE) (General)
df_psanchez = fc.extracting_tweets('sanchezcastejon')
fc.adding_party_column(df_psanchez, 'PSOE')
fc.adding_type_post_column(df_psanchez, 'publicación')
df_psanchez['hashtags'] = fc.extract_hashtags(df_psanchez['full_text'])
fc.adding_campaign_column(df_psanchez, 'general')

# Pablo Iglesias (PODEMOS) (General)
df_piglesias = fc.extracting_tweets('PabloIglesias')
fc.adding_party_column(df_piglesias, 'PODEMOS')
fc.adding_type_post_column(df_piglesias, 'publicación')
df_piglesias['hashtags'] = fc.extract_hashtags(df_piglesias['full_text'])
fc.adding_campaign_column(df_piglesias, 'general')

# Inés Arrimadas (CIUDADANOS) (General)
df_iarrimadas = fc.extracting_tweets('InesArrimadas')
fc.adding_party_column(df_iarrimadas, 'CIUDADANOS')
fc.adding_type_post_column(df_iarrimadas, 'publicación')
df_iarrimadas['hashtags'] = fc.extract_hashtags(df_iarrimadas['full_text'])
fc.adding_campaign_column(df_iarrimadas, 'general')

# Santiago Abascal (VOX) (General)
df_abascal = fc.extracting_tweets('Santi_ABASCAL')
fc.adding_party_column(df_abascal, 'VOX')
fc.adding_type_post_column(df_abascal, 'publicación')
df_abascal['hashtags'] = fc.extract_hashtags(df_abascal['full_text'])
fc.adding_campaign_column(df_abascal, 'general')

# Isabel Díaz Ayuso (Partido Popular) (Madrid)
df_ayuso = fc.extracting_tweets('IdiazAyuso')
fc.adding_party_column(df_ayuso, 'Partido Popular')
fc.adding_type_post_column(df_ayuso, 'publicación')
df_ayuso['hashtags'] = fc.extract_hashtags(df_ayuso['full_text'])
fc.adding_campaign_column(df_ayuso, 'madrid')

# Isabel Díaz Ayuso (Partido Popular) (Madrid)
df_ayuso = fc.extracting_tweets('IdiazAyuso')
fc.adding_party_column(df_ayuso, 'CIUDADANOS')
fc.adding_type_post_column(df_ayuso, 'publicación')
df_ayuso['hashtags'] = fc.extract_hashtags(df_ayuso['full_text'])
fc.adding_campaign_column(df_ayuso, 'madrid')

# Ángel Gabilondo (PSOE) (Madrid)
df_gabilondo = fc.extracting_tweets('equipoGabilondo')
fc.adding_party_column(df_gabilondo, 'CIUDADANOS')
fc.adding_type_post_column(df_gabilondo, 'publicación')
df_gabilondo['hashtags'] = fc.extract_hashtags(df_gabilondo['full_text'])
fc.adding_campaign_column(df_gabilondo, 'madrid')

# Rocío Monasterio (VOX) (Madrid)
df_monasterio = fc.extracting_tweets('monasterioR')
fc.adding_party_column(df_monasterio, 'VOX')
fc.adding_type_post_column(df_monasterio, 'publicación')
df_monasterio['hashtags'] = fc.extract_hashtags(df_monasterio['full_text'])
fc.adding_campaign_column(df_monasterio, 'madrid')

# Mónica García (Más Madrid) (Madrid)
df_mgarcia = fc.extracting_tweets('Monica_Garcia_G')
fc.adding_party_column(df_mgarcia, 'Más Madrid')
fc.adding_type_post_column(df_mgarcia, 'publicación')
df_mgarcia['hashtags'] = fc.extract_hashtags(df_mgarcia['full_text'])
fc.adding_campaign_column(df_mgarcia, 'madrid')

# Edmundo (Ciudadanos) (Madrid)
df_edmundo = fc.extracting_tweets('BalEdmundo')
fc.adding_party_column(df_edmundo, 'CIUDADANOS')
fc.adding_type_post_column(df_edmundo, 'publicación')
df_edmundo['hashtags'] = fc.extract_hashtags(df_edmundo['full_text'])
fc.adding_campaign_column(df_edmundo, 'madrid')


# DATA FRAME ALL TWEETS

print('Creating df_final_tweets...')

frames_tweets = [df_pp, df_psoe, df_ciudadanos, df_podemos, df_vox,
          df_ciudadanos_madrid, df_masmad_madrid, df_pp_madrid, df_psoe_madrid, df_podemos_madrid, df_vox_madrid,
          df_pcasado, df_psanchez, df_piglesias, df_abascal, df_iarrimadas,
          df_ayuso, df_gabilondo, df_mgarcia, df_monasterio, df_edmundo]

df_final_tweets = pd.concat(frames_tweets)
df_final_tweets.to_csv('political_spanish_sentiment_tweets.csv', mode='a', header=False)

print('df_final_tweets exported!')


# FINAL DATA FRAME
final_frames = [df_final_mentions, df_final_tweets]
df_total = pd.concat(final_frames)

df_total.to_csv('political_spanish_sentiment_ddbb.csv')

print('Final data frame exported!')


# MACHINE LEARNING SENTIMENT
#print('Sentiment Analysis...')
#fc.sentiment_classificaton(df_total, df_total['full_text'])
#df_total.to_csv('political_spanish_sentiment_ddbb.csv')

"""

print('Finished!')

# solve the loc problem
# bigger: 3.000 tweets and 10.000 mentions (¿tope por día?)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('PyCharm')

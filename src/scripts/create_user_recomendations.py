import findspark
findspark.init()
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Row

class RecomendationDataProccesor:
    def __init__(self):
        self.spark = SparkSession.builder \
                                 .master("local") \
                                 .appName("Friend Recommendation") \
                                 .getOrCreate()
        self.cities_df = self.load_cities()
        self.events_df = self.load_events()
        self.user_subscriptions_df = self.get_user_subscriptions()

    def load_cities(self):
        cities_data = [
            Row(id=1, city="Sydney", city_lat=-33.865, city_lng=151.2094, TIME_UTC="UTC+11", timezone="Australia/Sydney"),
            Row(id=2, city="Melbourne", city_lat=-37.8136, city_lng=144.9631, TIME_UTC="UTC+11", timezone="Australia/Melbourne"),
            Row(id=3, city="Brisbane", city_lat=-27.4678, city_lng=153.0281, TIME_UTC="UTC+10", timezone="Australia/Brisbane"),
            Row(id=4, city="Perth", city_lat=-31.9522, city_lng=115.8589, TIME_UTC="UTC+08", timezone="Australia/Perth"),
            Row(id=5, city="Adelaide", city_lat=-34.9289, city_lng=138.6011, TIME_UTC="UTC+10:30", timezone="Australia/Adelaide"),
            Row(id=6, city="Gold Coast", city_lat=-28.0167, city_lng=153.4, TIME_UTC="UTC+10", timezone="Australia/Gold Coast"),
            Row(id=7, city="Cranbourne", city_lat=-38.0996, city_lng=145.2834, TIME_UTC="UTC+11", timezone="Australia/Cranbourne"),
            Row(id=8, city="Canberra", city_lat=-35.2931, city_lng=149.1269, TIME_UTC="UTC+11", timezone="Australia/Canberra"),
            Row(id=9, city="Newcastle", city_lat=-32.9167, city_lng=151.75, TIME_UTC="UTC+11", timezone="Australia/Newcastle"),
            Row(id=10, city="Wollongong", city_lat=-34.4331, city_lng=150.8831, TIME_UTC="UTC+11", timezone="Australia/Wollongong"),
            Row(id=11, city="Geelong", city_lat=-38.15, city_lng=144.35, TIME_UTC="UTC+11", timezone="Australia/Geelong"),
            Row(id=12, city="Hobart", city_lat=-42.8806, city_lng=147.325, TIME_UTC="UTC+11", timezone="Australia/Hobart"),
            Row(id=13, city="Townsville", city_lat=-19.2564, city_lng=146.8183, TIME_UTC="UTC+10", timezone="Australia/Townsville"),
            Row(id=14, city="Ipswich", city_lat=-27.6167, city_lng=152.7667, TIME_UTC="UTC+10", timezone="Australia/Ipswich"),
            Row(id=15, city="Cairns", city_lat=-16.9303, city_lng=145.7703, TIME_UTC="UTC+10", timezone="Australia/Cairns"),
            Row(id=16, city="Toowoomba", city_lat=-27.5667, city_lng=151.95, TIME_UTC="UTC+10", timezone="Australia/Toowoomba"),
            Row(id=17, city="Darwin", city_lat=-12.4381, city_lng=130.8411, TIME_UTC="UTC+09:30", timezone="Australia/Darwin"),
            Row(id=18, city="Ballarat", city_lat=-37.55, city_lng=143.85, TIME_UTC="UTC+11", timezone="Australia/Ballarat"),
            Row(id=19, city="Bendigo", city_lat=-36.75, city_lng=144.2667, TIME_UTC="UTC+11", timezone="Australia/Bendigo"),
            Row(id=20, city="Launceston", city_lat=-41.4419, city_lng=147.145, TIME_UTC="UTC+11", timezone="Australia/Launceston"),
            Row(id=21, city="Mackay", city_lat=-21.1411, city_lng=149.1861, TIME_UTC="UTC+10", timezone="Australia/Mackay"),
            Row(id=22, city="Rockhampton", city_lat=-23.375, city_lng=150.5117, TIME_UTC="UTC+10", timezone="Australia/Rockhampton"),
            Row(id=23, city="Maitland", city_lat=-32.7167, city_lng=151.55, TIME_UTC="UTC+11", timezone="Australia/Maitland"),
            Row(id=24, city="Bunbury", city_lat=-33.3333, city_lng=115.6333, TIME_UTC="UTC+08", timezone="Australia/Bunbury")
        ]
        return self.spark.createDataFrame(cities_data)

    def load_events(self):
        return self.spark.read.parquet('/user/master/data/geo/events')

    def get_user_subscriptions(self):
        return self.events_df.filter(F.col("event_type") == "subscription") \
                             .select("user_id", "channel_id", "city", "lat", "lon")

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        r = 6371  # радиус Земли в км
        return 2 * r * F.asin(F.sqrt(F.pow(F.sin((F.radians(lat2) - F.radians(lat1)) / 2), 2) +
                                          F.cos(F.radians(lat1)) *
                                          F.cos(F.radians(lat2)) *
                                          F.pow(F.sin((F.radians(lon2) - F.radians(lon1)) / 2), 2)))

    def find_friend_candidates(self):
        user_subscriptions = self.user_subscriptions_df.alias("u1").crossJoin(self.user_subscriptions_df.alias("u2")) \
            .filter(F.col("u1.user_id") < F.col("u2.user_id")) \
            .filter(F.col("u1.channel_id") == F.col("u2.channel_id")) \
            .filter(F.col("u1.city") == F.col("u2.city"))
        
        friend_candidates = user_subscriptions.withColumn("distance", 
                                      self.calculate_distance(F.col("u1.lat"), F.col("u1.lon"), F.col("u2.lat"), F.col("u2.lon"))) \
            .filter(F.col("distance") <= 1)
        return friend_candidates

    def generate_friend_recommendations(self, friend_candidates):
        friend_recommendations = friend_candidates.select(
            F.col("u1.user_id").alias("user_left"),
            F.col("u2.user_id").alias("user_right"),
            F.current_timestamp().alias("processed_dttm"),
            F.col("u1.city").alias("zone_id"),
            F.lit(F.current_timestamp()).alias("local_time")
        )
        return friend_recommendations

    def run(self):
        friend_candidates = self.find_friend_candidates()
        recommendations = self.generate_friend_recommendations(friend_candidates)
        
        recommendations.write.mode("overwrite").parquet('/user/aksenovnik/data/tmp/friend_recommendations')


if __name__ == "__main__":
    recommender = RecomendationDataProccesor()
    recommender.run()

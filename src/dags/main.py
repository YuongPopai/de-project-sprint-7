from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import findspark
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.window import Window
findspark.init()

class UserDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
                                 .master("local") \
                                 .appName("Load Tags") \
                                 .getOrCreate()
        self.cities_df = self.load_cities()
        self.events_df = self.load_events()

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
        return self.spark.read.parquet('/user/master/data/geo/events').where("event_type = 'message'")

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        r = 6371  # радиус Земли в км
        return 2 * r * F.asin(F.sqrt(F.pow(F.sin((F.radians(lat2) - F.radians(lat1)) / 2), 2) +
                                            F.cos(F.radians(lat1)) *
                                            F.cos(F.radians(lat2)) *
                                            F.pow(F.sin((F.radians(lon2) - F.radians(lon1)) / 2), 2)))

    def get_nearest_city(self):
        joined_df = self.events_df.crossJoin(self.cities_df.select("city", "city_lat", "city_lng"))
        nearest_city = joined_df.withColumn("distance", self.calculate_distance(F.col("lat"), F.col("lon"), F.col("city_lat"), F.col("city_lng"))) \
                      .groupBy("event") \
                      .agg(F.first("city").alias("nearest_city"), F.min("distance").alias("min_distance"))
        return nearest_city

    def process_user_data(self, nearest_city):
        user_df = self.events_df \
            .groupBy("message_from") \
            .agg(
                F.first("lat").alias("last_lat"),
                F.first("lon").alias("last_lon"),
                F.first("datetime").alias("last_event_time"),
                F.first("message_channel_to").alias("channel_id")
            )

        user_df = user_df.join(nearest_city.select("nearest_city", "event"), user_df.event == nearest_city.event, "left")
        return user_df

    def get_home_city(self):
        home_city_df = self.events_df \
            .withColumn("event_date", F.to_date("datetime")) \
            .groupBy("message_from", "city") \
            .agg(
                F.collect_list("event_date").alias("event_dates"),
                F.countDistinct("event_date").alias("days_count"),
                F.collect_set("city").alias("cities_visited")
            ) \
            .filter(F.size("event_dates") > 0) \
            .withColumn("first_date", F.expr("element_at(event_dates, 1)")) \
            .withColumn("last_date", F.expr("element_at(event_dates, -1)")) \
            .filter(F.expr("datediff(last_date, first_date) >= 27")) \
            .groupBy("message_from") \
            .agg(F.last("city").alias("home_city"))
        return home_city_df

    def finalize_user_data(self, user_df, home_city_df):
        user_df = user_df.join(home_city_df, ["message_from"], "left") \
                         .withColumn("travel_count", F.size(F.col("cities_visited"))) \
                         .withColumn("travel_array", F.expr("concat_ws(',', cities_visited)")) \
                         .withColumn("local_time", F.from_utc_timestamp(F.col("last_event_time"), F.col("timezone"))) \
                         .select(
                            F.col("message_from").alias("user_id"),  
                            F.col("nearest_city").alias("act_city"), 
                            F.col("home_city"), 
                            "travel_count", 
                            "travel_array", 
                            "local_time"
                         )
        return user_df

    def run(self):
        nearest_city = self.get_nearest_city()
        user_df = self.process_user_data(nearest_city)
        home_city_df = self.get_home_city()
        finalized_user_df = self.finalize_user_data(user_df, home_city_df)
        finalized_user_df.write.mode("overwrite").parquet('/user/aksenovnik/data/tmp/user_result')



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



class ZoneDataProccesor:
    def __init__(self):
        self.spark = SparkSession.builder \
                                 .master("local") \
                                 .appName("Geo Layer Event Distribution") \
                                 .getOrCreate()
        self.cities_df = self.load_cities()
        self.events_df = self.load_events()
        self.processed_events_df = self.process_events()

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

    def process_events(self):
        events_df = self.events_df \
            .withColumn("event_date", F.to_date("datetime")) \
            .withColumn("month", F.date_format("event_date", "yyyy-MM")) \
            .withColumn("week", F.date_format("event_date", "yyyy-MM-'W'ww"))
        return events_df

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        r = 6371  # радиус Земли в км
        return 2 * r * F.asin(F.sqrt(F.pow(F.sin((F.radians(lat2) - F.radians(lat1)) / 2), 2) +
                                          F.cos(F.radians(lat1)) *
                                          F.cos(F.radians(lat2)) *
                                          F.pow(F.sin((F.radians(lon2) - F.radians(lon1)) / 2), 2)))

    def find_nearest_city(self):
        joined_df = self.processed_events_df.crossJoin(self.cities_df.select("city", "city_lat", "city_lng"))
        
        nearest_city = joined_df.withColumn("distance", self.calculate_distance(F.col("lat"), F.col("lon"), F.col("city_lat"), F.col("city_lng"))) \
                                 .groupBy("event") \
                                 .agg(F.first("city").alias("nearest_city"), F.min("distance").alias("min_distance"))
        return nearest_city

    def enrich_events_with_city_info(self, nearest_city):
        events_with_city = self.processed_events_df.join(nearest_city, on="event", how="left")
        events_with_city = events_with_city.join(self.cities_df.select("city", "id"), on="nearest_city", how="left") \
                                            .withColumnRenamed("id", "zone_id")
        return events_with_city

    def create_geo_layer(self, events_with_city):
        window_week = Window.partitionBy("week", "zone_id")
        window_month = Window.partitionBy("month", "zone_id")

        geo_layer_df = (
            events_with_city.select(
                "zone_id",
                "week",
                "month",
                F.count(F.when(F.col("event_type") == "message", 1)).over(window_week).alias("week_message"),
                F.count(F.when(F.col("event_type") == "reaction", 1)).over(window_week).alias("week_reaction"),
                F.count(F.when(F.col("event_type") == "subscription", 1)).over(window_week).alias("week_subscription"),
                F.count(F.when(F.col("event_type") == "registration", 1)).over(window_week).alias("week_user"),
                F.count(F.when(F.col("event_type") == "message", 1)).over(window_month).alias("month_message"),
                F.count(F.when(F.col("event_type") == "reaction", 1)).over(window_month).alias("month_reaction"),
                F.count(F.when(F.col("event_type") == "subscription", 1)).over(window_month).alias("month_subscription"),
                F.count(F.when(F.col("event_type") == "registration", 1)).over(window_month).alias("month_user")
            )
        )

        return geo_layer_df.distinct()

    def run(self):
        nearest_city = self.find_nearest_city()
        events_with_city = self.enrich_events_with_city_info(nearest_city)
        geo_layer_df = self.create_geo_layer(events_with_city)
        geo_layer_df.write.mode("overwrite").parquet('/user/aksenovnik/data/tmp/zones_result')


def run_user_data_processor():
    processor = UserDataProcessor()
    processor.run()

def run_recommendation_data_processor():
    processor = RecomendationDataProccesor()
    processor.run()

def run_zone_data_processor():
    processor = ZoneDataProccesor()
    processor.run()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 25),
    'retries': 1,
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='A DAG that processes user data, recommendations, and geo layer events',
    schedule_interval='@daily',
)

task1 = PythonOperator(
    task_id='run_user_data_processor',
    python_callable=run_user_data_processor,
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_recommendation_data_processor',
    python_callable=run_recommendation_data_processor,
    dag=dag,
)

task3 = PythonOperator(
    task_id='run_zone_data_processor',
    python_callable=run_zone_data_processor,
    dag=dag,
)

task1 >> task2 >> task3

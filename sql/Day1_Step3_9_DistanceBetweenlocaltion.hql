-- ### Compute the direct distance between two longitude-latitude locations
select pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, trip_distance, trip_time_in_secs,
3959*2*2*atan((1-sqrt(1-pow(sin((dropoff_latitude-pickup_latitude)
 *radians(180)/180/2),2)-cos(pickup_latitude*radians(180)/180)
 *cos(dropoff_latitude*radians(180)/180)*pow(sin((dropoff_longitude-pickup_longitude)*radians(180)/180/2),2)))
 /sqrt(pow(sin((dropoff_latitude-pickup_latitude)*radians(180)/180/2),2)
 +cos(pickup_latitude*radians(180)/180)*cos(dropoff_latitude*radians(180)/180)*
 pow(sin((dropoff_longitude-pickup_longitude)*radians(180)/180/2),2))) as direct_distance
from nyctaxidb.trip
where month=1
and pickup_longitude between -90 and -30
and pickup_latitude between 30 and 90
and dropoff_longitude between -90 and -30
and dropoff_latitude between 30 and 90
-- ### View the number of records in each of the 12 partitions
select month, count(month) as TotalRecords from nyctaxidb.trip group by month
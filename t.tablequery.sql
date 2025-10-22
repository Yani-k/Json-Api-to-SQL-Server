USE mta_api

CREATE TABLE dbo.Staging_SubwayRidership (
    mta_id              INT IDENTITY(1000,1) PRIMARY KEY,
    transit_timestamp   DATETIME2(3),
    transit_mode        VARCHAR(40),
    station_complex_id  VARCHAR(20),
    station_complex     VARCHAR(200),
    borough             VARCHAR(40),
    payment_method      VARCHAR(20),
    fare_class_category VARCHAR(80),
    ridership           FLOAT,
    transfers           FLOAT,
    latitude            FLOAT,
    longitude           FLOAT 
    --CONSTRAINT PK_MTA_SubwayHourlyRidership
    --PRIMARY KEY (transit_timestamp, station_complex_id, payment_method, fare_class_category)
    --initial loading/staging No primary Key required
);

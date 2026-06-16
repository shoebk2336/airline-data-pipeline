create SCHEMA airlines;


create TABLE airlines.airport_dim(
    airport_id BIGINT,
    city VARCHAR,
    state VARCHAR,
    name VARCHAR
)


create TABLE airlines.daily_flight_facts(
    carrier VARCHAR,
    dep_airport VARCHAR,
    arr_airport VARCHAR,
    dep_city VARCHAR,
    arr_city VARCHAR,
    dep_state VARCHAR,
    arr_state VARCHAR,
    dep_delay BIGINT,
    arr_delay BIGINT
)

#To create user and password

CREATE USER glue_user PASSWORD '[Smkhan@1997]!';
GRANT ALL ON SCHEMA airlines TO glue_user;
GRANT ALL ON ALL TABLES IN SCHEMA airlines TO glue_user;

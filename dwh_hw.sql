
create schema bookings authorization postgres;

-- Таблица измерений пассажиры

--drop table bookings.dim_passengers; 

create table bookings.dim_passengers(
	id serial primary key,
	passenger_key int,
	passenger_name varchar(100),
	passport varchar(20),
	email varchar(100),
	phone varchar(100),
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);
create index idx_dim_passengers_passport on bookings.dim_passengers(passport);
create index idx_dim_passengers_lookup on bookings.dim_passengers(passenger_key);

-- Таблица откланеных измерений пассажиры

--drop table bookings.dim_rejected_passengers;

create table bookings.dim_rejected_passengers(
	id serial primary key,
	passenger_key int,
	passenger_name varchar(100),
	passport varchar(20),
	email varchar(100),
	phone varchar(100),
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);
create index idx_rej_dim_passengers_passport on bookings.dim_rejected_passengers(passport);
create index idx_rej_dim_passengers_lookup on bookings.dim_rejected_passengers(passenger_key);


--Таблица измерений аэропорты

--drop table bookings.dim_airports;

create table bookings.dim_airports(
	id serial primary key,
	airports_key int,
	airport_code bpchar(3),
	airport_name varchar(100),
	city varchar (250),
	longitude float8,
	latitude float8,
	time_zone varchar(100),
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);

create index idx_dim_airports_lookup on bookings.dim_airports(airports_key);

--Таблица откланеных измерений эропорты.

--drop table bookings.dim_rejected_airports;

create table bookings.dim_rejected_airports(
	id serial primary key,
	airports_key int,
	airport_code bpchar(3),
	airport_name varchar(100),
	city varchar (250),
	longitude float8,
	latitude float8,
	time_zone varchar(100),
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);

create index idx_dim_rejected_airports_lookup on bookings.dim_rejected_airports(airports_key);

-- Таблица измерений самолеты

--drop table bookings.dim_aircrafts;

create table bookings.dim_aircrafts(
	id serial primary key,
	aircrafts_key int,
    aircraft_code bpchar(3),
	model varchar(50),
	"range" int,
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);

CREATE INDEX idx_dim_aircrafts_lookup ON bookings.dim_aircrafts(aircrafts_key);

-- Таблица отклоненных измерений самолеты

--drop table bookings.dim_rejected_aircrafts;

create table bookings.dim_rejected_aircrafts(
	id serial primary key,
	aircrafts_key int,
    aircraft_code bpchar(3),
	model varchar(50),
	"range" int,
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);

create index idx_dim_rejected_aircrafts_lookup on bookings.dim_rejected_aircrafts(aircrafts_key);

-- Таблица измерений тариф обслуживания

create table bookings.dim_tariff(
	id serial primary key,
	tariff_key int,
	"name" varchar(10),
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);

CREATE INDEX idx_dim_tariff_lookup ON bookings.dim_tariff(tariff_key);


-- Таблица отклоненных измерений тариф обслуживания

create table bookings.dim_rejected_tariff(
	id serial primary key,
	tariff_key int,
	"name" varchar(10),
	start_ts date,
	end_ts date,
	is_curent bool default true,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp,
	"version" int default 1
);

CREATE INDEX idx_dim_rejected_tariff_lookup ON bookings.dim_rejected_tariff(tariff_key);
-- Таблица измерений календарь

create table bookings.dim_date
AS
WITH dates AS (
    SELECT dd::date AS dt
    FROM generate_series
 ('2010-01-01'::timestamp
 , '2030-01-01':: timestamp
 , '1 day'::interval) dd
)
SELECT
 to_char(dt, 'YYYYMMDD')::int AS id,
 dt AS date,
 to_char(dt, 'YYYY-MM-DD') AS ansi_date,
 date_part('isodow', dt)::int AS "day",
 date_part('week', dt)::int AS week_number,
 date_part('month', dt)::int AS "month",
 date_part('isoyear', dt)::int AS "year",
 (date_part('isodow', dt)::smallint BETWEEN 1 AND  5)::int AS week_day,
 (to_char(dt, 'YYYYMMDD')::int IN (
        20130101,
        20130102,
        20130103,
        20130104,
        20130105,
        20130106,
        20130107,
        20130108,
        20130223,
        20130308,
        20130310,
        20130501,
        20130502,
        20130503,
        20130509,
        20130510,
        20130612,
        20131104,
        20140101,
        20140102,
        20140103,
        20140104,
        20140105,
        20140106,
        20140107,
        20140108,
        20140223,
        20140308,
        20140310,
        20140501,
        20140502,
        20140509,
        20140612,
        20140613,
        20141103,
        20141104,
        20150101,
        20150102,
        20150103,
        20150104,
        20150105,
        20150106,
        20150107,
        20150108,
        20150109,
        20150223,
        20150308,
        20150309,
        20150501,
        20150504,
        20150509,
        20150511,
        20150612,
        20151104,
        20160101,
        20160102,
        20160103,
        20160104,
        20160105,
        20160106,
        20160107,
        20160108,
        20160222,
        20160223,
        20160307,
        20160308,
        20160501,
        20160502,
        20160503,
        20160509,
        20160612,
        20160613,
        20161104,
        20170101,
        20170102,
        20170103,
        20170104,
        20170105,
        20170106,
        20170107,
        20170108,
        20170223,
        20170224,
        20170308,
        20170501,
        20170508,
        20170509,
        20170612,
        20171104,
        20171106,
        20180101,
        20180102,
        20180103,
        20180104,
        20180105,
        20180106,
        20180107,
        20180108,
        20180223,
        20180308,
        20180309,
        20180430,
        20180501,
        20180502,
        20180509,
        20180611,
        20180612,
        20181104,
        20181105,
        20181231,
        20190101,
        20190102,
        20190103,
        20190104,
        20190105,
        20190106,
        20190107,
        20190108,
        20190223,
        20190308,
        20190501,
        20190502,
        20190503,
        20190509,
        20190510,
        20190612,
        20191104,
        20200101, 
        20200102, 
        20200103, 
        20200106, 
        20200107, 
        20200108,
        20200224, 
        20200309, 
        20200501, 
        20200504, 
        20200505, 
        20200511,
        20200612, 
        20201104))::int AS holiday
FROM dates
ORDER BY dt;
ALTER TABLE bookings.dim_date ADD PRIMARY KEY (id);





-- Таблица фактов

--drop table bookings.fact_flights;

create table bookings.fact_flights(
	id serial primary key,
	departure_date_key int not null,
	arrival_date_key int not null,
	passengers_key int not null,
	actual_departure timestamp not null,
	actual_arrival timestamp not null,
	departure_delay int not null,
	arrival_delay int not null,
	aircrafts_key int not null,
	departure_airports_key int not null,
	arrival_airports_key int not null,
	tariff_key int not null,
	price numeric (12, 2) not null,
	constraint fights_passengers_fkey foreign key (passengers_key) references bookings.dim_passengers(id),
	constraint fights_dep_airports_fkey foreign key (departure_airports_key) references bookings.dim_airports(id),
	constraint fights_arr_airports_fkey foreign key (arrival_airports_key) references bookings.dim_airports(id),
	constraint fights_tariff_fkey foreign key (tariff_key) references bookings.dim_tariff(id),
	constraint fights_aircrafts_fkey foreign key (aircrafts_key) REFERENCES bookings.dim_aircrafts(id),
	constraint fights_dep_date_key_fkey foreign key (departure_date_key) REFERENCES bookings.dim_date(id),
	constraint fights_arr_date_key_fkey foreign key (arrival_date_key) REFERENCES bookings.dim_date(id)
);


--Таблица отклоненых фактов

--drop table bookings.rejected_fact_flights;


create table bookings.rejected_fact_flights(
	id serial primary key,
	departure_date_key int not null,
	arrival_date_key int not null,
	passengers_key int not null,
	actual_departure timestamp not null,
	actual_arrival timestamp not null,
	departure_delay int not null,
	arrival_delay int not null,
	aircrafts_key int not null,
	departure_airports_key int not null,
	arrival_airports_key int not null,
	tariff_key int not null,
	price numeric not null,
	constraint fights_rej_passengers_fkey foreign key (passengers_key) references bookings.dim_passengers(id),
	constraint fights_rej_dep_airports_fkey foreign key (departure_airports_key) references bookings.dim_airports(id),
	constraint fights_rej_arr_airports_fkey foreign key (arrival_airports_key) references bookings.dim_airports(id),
	constraint fights_rej_tariff_fkey foreign key (tariff_key) references bookings.dim_tariff(id),
	constraint fights_rej_aircrafts_fkey foreign key (aircrafts_key) REFERENCES bookings.dim_aircrafts(id),
	constraint fights_rej_dep_date_key_fkey foreign key (departure_date_key) REFERENCES bookings.dim_date(id),
	constraint fights_rej_arr_date_key_fkey foreign key (arrival_date_key) REFERENCES bookings.dim_date(id)
);
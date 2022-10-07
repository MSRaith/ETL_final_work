# Процесс извлечения данных из базы данных по авиаперевозкам, преобразование, обогащение и выгрузка в DWH.

Процесс ETL определен заданием dwh_fw.kjb.<br/>
Задание состоит из выполнения 9 трансформаций (рис.job_fw.jpeg):
1.	dim_passengers, выполняет трансформацию dwh_fw_passengers.ktr
2.	dim_aircrafts, выполняет трансформацию dwh_fw_aircrafts.ktr
3.	dim_airports, выполняет трансформацию dwh_fw_airports.ktr
4.	dim_tariff, выполняет трансформацию dwh_fw_tariff.ktr
5.	fact, выполняет трансформацию dwh_fw_fact.ktr
6.	dim_passengers_csv, выполняет трансформацию dwh_fw_passenger_csv.ktr
7.	dim_aircrafts_csv, выполняет трансформацию dwh_fw_aircrafts_csv.ktr
8.	dim_airports_csv, выполняет трансформацию dwh_fw_airports_csv.ktr
9.	dim_tariff_csv, выполняет трансформацию dwh_fw_tariff_csv.ktr

Порядок выполнения соответствует списку.

## Описание трансформаций:

### 1. dwh_fw_passengers.ktr
Процесс извлечения данных о пасажирах из таблицы bookings.tickets, обогащение, проверка качества и загрузка в таблицу измерений bookings.dim_passengers.
Состоит из 8-ми шагов, рис.dim_passengers.jpeg:
1.	input passenger, извлекает необходимые данные, имеет подключение к БД источнику 'bd_in'.
2.	select row, выбирает необходимые данные.
3.	Get today date, получение сегодняшний даты, в формате 'date'
4.	Join dt, обогащение данных по пассажирам сегодняшней датой
5.	Filter rows, фильтрация качества данных
6.	upload dim passengers, выгрузка данных в таблицу измерений dim_passengers, имеет подключение к БД назначения 'bd_out'
7.	Select rejected row, выбирает необходимые строки для отклоненных данных
8.	upload dim rejected passengers, выгрузка отклоненных данных в таблицу измерений dim_passengers, имеет подключение к БД назначения 'bd_out'

#### Входные данные:

##### Таблица bookings.ticket.
Таблица bookings.ticket содержит идентификатор пассажира (passenger_id) — номер документа,
удостоверяющего личность, — его фамилию и имя (passenger_name) и контактную
информацию (contact_date).
Ни идентификатор пассажира, ни имя не являются постоянными (можно поменять паспорт,
можно сменить фамилию), поэтому однозначно найти все билеты одного и того же пассажира
невозможно. <br/>
Столбец,Тип, Модификаторы, Описание:<br/>
ticket_no, char(13), NOT NULL, Номер билета <br/>
book_ref, char(6), NOT NULL, Номер бронирования <br/>
passenger_id, varchar(20), NOT NULL, Идентификатор пассажира <br/>
passenger_name, text, NOT NULL, Имя пассажира <br/>
contact_data, jsonb, NULL, Контактные данные пассажира <br/>
Индексы: <br/>
PRIMARY KEY, btree (ticket_no) <br/>
Ограничения внешнего ключа: <br/>
FOREIGN KEY (book_ref) REFERENCES bookings(book_ref) <br/>
Ссылки извне: <br/>
TABLE "ticket_flights" FOREIGN KEY (ticket_no) REFERENCES tickets(ticket_no) <br/>

##### Запрос SQL:
select row_number () over () as id_key,<br/>
t2.passenger_name,<br/>
t2.passenger_id,<br/>
t2.contact_data ->> 'phone' as phone,<br/>
t2.contact_data ->> 'email' as email<br/>
FROM bookings.tickets t2 <br/>
group by t2.passenger_name, t2.passenger_id, t2.contact_data;

#### Выходные данные:
 
##### Таблица bookings.dim_passengers
Таблица измерений bookings.dim_passengers относится к медлено изменяемому измерению второго типа.
Содержит идентификатор записи (id), ключ пассажира (passenger_key), индификатор пассажира (passport) — номер документа,
удостоверяющего личность, его фамилию и имя (passenger_name) и контактную
информацию (phone, email), начало и конец версии записи (start_ts, end_ts), статус записи (is_current),
дата создания записи (create_ts), дата обновления записи (update_ts)<br/>
Столбец, Тип, Модификаторы, Описание<br/>
id, serial, NOT NULL, Технический ключ<br/>
passenger_key, int, NULL, Суррогатный ключ<br/>
passenger_name, varchar(100), NULL, Фамилия и имя пассажира<br/>
passport, varchar(20), NULL, Индификационный номер пассажира<br/>
email, varchar(100), NULL, Почта пассажира<br/>
phone, varchar(100), NULL, Телефон пассажира<br/>
start_ts, date, NULL, Начало версии записи<br/>
end_ts, date, NULL, Конец версии записи<br/>
is_current, bool, NULL default true, Текущий статус версии записи <br/>
create_ts, timestamp, NULL default current_timestamp, Запись создана<br/>
update_ts, timestamp, NULL default current_timestamp, Запись обновлена<br/>
"version", int, NULL default 1, Версия записи <br/>

Индексы:<br/>
PRIMARY KEY, dim_passengers_pkey (id),<br/>
idx_dim_passengers_lookup (passenger_key),<br/>
idx_dim_passengers_passport (passport)<br/>

Ссылки извне:<br/>
TABLE fact_flights" FOREIGN KEY (passenger_key) REFERENCES dim_passengers(id)<br/>

##### Таблица bookings.dim_rejected_passengers
Таблица отклоненных измерений bookings.dim_rejected_passengers относится к медленно изменяемому измерению второго типа.
И имеет идентичную структуру, с bookings.dim_passengers.

#### Проверка качества данных:
1.	 Номер телефона начинается с '+'

---

### 2. dwh_fw_aircrafts.ktr
Процесс извлечения данных о самолетах из таблицы bookings.aircrafts, обогащение, проверка качества и загрузка в таблицу измерений bookings.dim_aircrafts.
Состоит из 8-ми шагов, рис.dim_aircrafts.jpeg:
1.	input aircrafts, извлекает необходимые данные, имеет подключение к БД источнику 'bd_in'.
2.	select row, выбирает необходимые данные.
3.	Get today date, получение сегодняшней даты, в формате 'date'
4.	Join dt, обогащение данных по пассажирам сегодняшней датой
5.	Filter rows, фильтрация качества данных
6.	upload dim aircrafts, выгрузка данных в таблицу измерений dim_aircrafts, имеет подключение к БД назначения 'bd_out'
7.	Select rejected row, выбирает необходимые строки для отклоненных данных
8.	upload dim rejected aircrafts, выгрузка отклоненных данных в таблицу измерений dim_aircrafts, имеет подключение к БД назначения 'bd_out'

#### Входные данные:

##### Таблица bookings.aircrafts.
Каждая модель воздушного судна идентифицируется своим трехзначным кодом
(aircraft_code). Указывается также название модели (model) и максимальная дальность полета
в километрах (range).<br/>
Столбец, Тип, Модификаторы, Описание<br/>
aircraft_code, char(3), NOT NULL, Код самолета, IATA<br/>
model, text, NOT NULL, Модель самолета<br/>
range, integer, NOT NULL, Максимальная дальность полета, км<br/>
Индексы:<br/>
PRIMARY KEY, btree (aircraft_code)<br/>
Ограничения-проверки:<br/>
CHECK (range > 0)<br/>
Ссылки извне:<br/>
TABLE "flights" FOREIGN KEY (aircraft_code)<br/>
REFERENCES aircrafts(aircraft_code)<br/>
TABLE "seats" FOREIGN KEY (aircraft_code)<br/>
REFERENCES aircrafts(aircraft_code) ON DELETE CASCADE<br/>

##### Запрос SQL:
SELECT row_number () over () as id_key, *<br/>
FROM bookings.aircrafts a;

#### Выходные данные:
 
##### Таблица bookings.dim_aircrafts
Таблица измерений bookings.dim_passengers относится к медленно изменяемому измерению второго типа.
Содержит идентификатор записи (id), ключ самолета (aircraft_key), каждая модель воздушного судна идентифицируется своим трехзначным кодом
(aircraft_code). Указывается также название модели (model) и максимальная дальность полета
в километрах (range), начало и конец версии записи (start_ts, end_ts), статус записи (is_current),
дата создания записи (create_ts), дата обновления записи (update_ts)<br/>
Столбец, Тип, Модификаторы, Описание<br/>
id, serial, NOT NULL, Технический ключ<br/>
aircraft_key, int, NULL, Суррогатный ключ<br/>
aircrafts_code, bpchar(3), NULL, Код самолета<br/>
model, varchar(50), NULL, модель самолета<br/>
range, int, NULL, Максимальная дальность полета<br/>
start_ts, date, NULL, Начало версии записи<br/>
end_ts, date, NULL, Конец версии записи<br/>
is_current, bool, NULL default true, Текущий статус версии записи <br/>
create_ts, timestamp, NULL default current_timestamp, Запись создана<br/>
update_ts, timestamp, NULL default current_timestamp, Запись обновлена<br/>
"version", int, NULL default 1, Версия записи <br/>

Индексы:<br/>
PRIMARY KEY, dim_aircrafts_pkey (id),<br/>
idx_dim_aircrafts_lookup (aircraft_key)

Ссылки извне:<br/>
TABLE fact_flights" FOREIGN KEY (aircraft_key) REFERENCES dim_aircrafts(id)

##### Таблица bookings.dim_rejected_aircrafts
Таблица отклоненных измерений bookings.dim_rejected_aircrafts относится к медленно изменяемому измерению второго типа.
И имеет идентичную структуру, с bookings.dim_dim_aircrafts.

#### Проверка качества даных:
1.	 Дальность полета больше '0' км
---
### 3. dwh_fw_airports.ktr
Процесс извлечения данных о аэропортах из таблицы bookings.airports, обогащение, проверка качества и загрузка в таблицу измерений bookings.dim_airports.
Состоит из 8-ми шагов, рис.dim_airports.jpeg:
1.	input airports, извлекает необходимые данные, имеет подключение к БД источнику 'bd_in'.
2.	select row, выбирает необходимые данные.
3.	Get today date, получение сегодняшний даты, в формате 'date'
4.	Join dt, обогащение данных по пассажирам сегодняшней датой
5.	Filter rows, фильтрация качества данных
6.	upload dim airports, выгрузка данных в таблицу измерений dim_airports, имеет подключение к БД назначения 'bd_out'
7.	Select rejected row, выбирает необходимые строки для отклоненных данных
8.	upload dim rejected aircrafts, выгрузка отклоненных данных в таблицу измерений dim_airports, имеет подключение к БД назначения 'bd_out'

#### Входные данные:

##### Таблица bookings.airports. <br/>
Аэропорт идентифицируется трехбуквенным кодом (airport_code) и имеет свое имя
(airport_name). Для города не предусмотрено отдельной сущности, но название (city) указывается и может
служить для того, чтобы определить аэропорты одного города. Также указывается широта
(longitude), долгота (latitude) и часовой пояс (timezone).<br/>
Столбец, Тип, Модификаторы, Описание<br/>
airport_code, char(3), NOT NULL, Код аэропорта<br/>
airport_name, text, NOT NULL, Название аэропорта<br/>
city, text, NOT NULL, Город<br/>
longitude, float, NOT NULL, Координаты аэропорта: долгота<br/>
latitude, float, NOT NULL, Координаты аэропорта: широта<br/>
timezone, text, NOT NULL,  Временная зона аэропорта<br/>
Индексы:<br/>
PRIMARY KEY, btree (airport_code)<br/>
Ссылки извне:<br/>
TABLE "flights" FOREIGN KEY (arrival_airport) REFERENCES airports(airport_code)<br/>
TABLE "flights" FOREIGN KEY (departure_airport) REFERENCES airports(airport_code)

##### Запрос SQL:
SELECT row_number () over () as id_key, * <br/>
FROM bookings.airports a;

#### Выходные данные:
 
##### Таблица bookings.dim_airports
Таблица измерений bookings.dim_airports относится к медленно изменяемому измерению второго типа.
Содержит идентификатор записи (id), ключ аэропорта (airports_key), аэропорт идентифицируется трехбуквенным кодом (airport_code) и имеет свое имя
(airport_name). Для города не предусмотрено отдельной сущности, но название (city) указывается и может
служить для того, чтобы определить аэропорты одного города. Также указывается широта
(longitude), долгота (latitude) и часовой пояс (timezone), начало и конец версии записи (start_ts, end_ts), статус записи (is_current),
дата создания записи (create_ts), дата обновления записи (update_ts)<br/>
Столбец, Тип, Модификаторы, Описание<br/>
id, serial, NOT NULL, Технический ключ<br/>
airports_key, int, NULL, Суррогатный ключ<br/>
airport_code, bpchar(3) NULL, код аэропорта<br/>
airport_name, varchar(100) NULL, название аэропорта <br/>
city varchar(250), NULL, город <br/>
longitude float8, NULL, долгота <br/>
latitude float8, NULL, широта <br/>
start_ts, date, NULL, Начало версии записи<br/>
end_ts, date, NULL, Конец версии записи<br/>
is_current, bool, NULL default true, Текущий статус версии записи <br/>
create_ts, timestamp, NULL default current_timestamp, Запись создана<br/>
update_ts, timestamp, NULL default current_timestamp, Запись обновлена<br/>
"version", int, NULL default 1, Версия записи <br/>
Индексы:<br/>
PRIMARY KEY, dim_airports_pkey (id),<br/>
idx_dim_airports_lookup (airports_key),<br/>
Ссылки извне:<br/>
TABLE fact_flights" FOREIGN KEY (departure_airports_key) REFERENCES dim_airports(id)<br/>
TABLE fact_flights" FOREIGN KEY (arrival_airports_key) REFERENCES dim_airports(id)

##### Таблица bookings.dim_rejected_airports
Таблица отклоненных измерений bookings.dim_rejected_airports относится к медленно изменяемому измерению второго типа.
И имеет идентичную структуру, с bookings.dim_airports.

#### Проверка качества данных:
1.	Долгота больше или равна -180
2.	Долгота меньше или равна 180
3.	Широта больше или равна -90
4.	Широта меньше или равна 90

---
### 3. dwh_fw_tariff.ktr
Процесс извлечения данных о аэропортах из таблицы bookings.taruff, обогащение, проверка качества и загрузка в таблицу измерений bookings.dim_tariff.
Состоит из 8-ми шагов, рис.dim_tariff.jpeg:
1.	input tariff, извлекает необходимые данные, имеет подключение к БД источнику 'bd_in'.
2.	select row, выбирает необходимые данные.
3.	Get today date, получение сегодняшний даты, в формате 'date'
4.	Join dt, обогащение данных по пассажирам сегодняшней датой
5.	upload dim tariff, выгрузка данных в таблицу измерений dim_tariff, имеет подключение к БД назначения 'bd_out'


#### Входные данные:

##### Таблица bookings.ticket_flights.
Перелет соединяет билет с рейсом и идентифицируется их номерами.
Для каждого перелета указываются его стоимость (amount) и класс обслуживания
(fare_conditions).<br/>
Столбец, Тип, Модификаторы, Описание<br/>
ticket_no, char(13) | NOT NULL | Номер билета<br/>
flight_id, integer, NOT NULL, Идентификатор рейса<br/>
fare_conditions, varchar(10), NOT NULL, Класс обслуживания<br/>
amount, numeric(10,2), NOT NULL, Стоимость перелета<br/>
Индексы:<br/>
PRIMARY KEY, btree (ticket_no, flight_id)<br/>
Ограничения-проверки:<br/>
CHECK (amount >= 0)<br/>
CHECK (fare_conditions IN ('Economy', 'Comfort', 'Business'))<br/>
Ограничения внешнего ключа:<br/>
FOREIGN KEY (flight_id) REFERENCES flights(flight_id)<br/>
FOREIGN KEY (ticket_no) REFERENCES tickets(ticket_no)<br/>
Ссылки извне:<br/>
TABLE "boarding_passes" FOREIGN KEY (ticket_no, flight_id)<br/>
REFERENCES ticket_flights(ticket_no, flight_id)

##### Запрос SQL:
select row_number () over () as id_key,<br/>
t_f.fare_conditions<br/> 
from bookings.ticket_flights t_f<br/>
group by t_f.fare_conditions<br/>

#### Выходные данные:
 
##### Таблица bookings.dim_tariff
Таблица измерений bookings.dim_tariff относится к медленно изменяемому измерению второго типа.
Содержит идентификатор записи (id), ключ аэропорта (tariff_key), класс обслуживания
(name), начало и конец версии записи (start_ts, end_ts), статус записи (is_curent),
дата создания записи (create_ts), дата обновления записи (update_ts)<br/>
Столбец, Тип, Модификаторы, Описание<br/>
id, serial, NOT NULL, Технический ключ<br/>
airports_key, int, NULL, Суррогатный ключ<br/>
name, varchar(10) NULL, название тарифа<br/>
start_ts, date, NULL, Начало версии записи<br/>
end_ts, date, NULL, Конец версии записи<br/>
is_curent, bool, NULL default true, Текущий статус версии записи <br/>
create_ts, timestamp, NULL default current_timestamp, Запись создана<br/>
update_ts, timestamp, NULL default current_timestamp, Запись обновлена<br/>
"version", int, NULL default 1, Версия записи <br/>
Индексы:<br/>
PRIMARY KEY, dim_tariff_pkey (id),<br/>
idx_dim_airports_lookup (tariff_key),<br/>
Ссылки извне:<br/>
TABLE fact_flights" FOREIGN KEY (tariff_key) REFERENCES dim_tariff(id)

##### Таблица bookings.dim_rejected_tariff
Таблица отклоненных измерений bookings.dim_rejected_tariff относится к медленно изменяемому измерению второго типа.
И имеет идентичную структуру, с bookings.dim_tariff.

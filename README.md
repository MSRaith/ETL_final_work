# Процесс извлечения данных из базы данных по авиаперевозкам, преобразование, обогащение и выгрузка в DWH.

Процесс ETL определен заданием dwh_fw.kjb.<br/>
Задание состоит из выполнения 5 трансформаций (рис.job_fw.jpeg):
1.	dim_passengers, выполняет трансформацию dwh_fw_passengers.ktr
2.	dim_aircrafts, выполняет трансформацию dwh_fw_aircrafts.ktr
3.	dim_airports, выполняет трансформацию dwh_fw_airports.ktr
4.	dim_tariff, выполняет трансформацию dwh_fw_tariff.ktr
5.	fact, выполняет трансформацию dwh_fw_fact.ktr

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

##### Таблица bookings.airports. 
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
Состоит из 5-и шагов, рис.dim_tariff.jpeg:
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

### 5. dwh_fw_fact.ktr 
Процесс извлечения данных совершенных из основного источника данных bookings.flight, обогащение из дополнительных источников данных
проверка качества и загрузка в таблицу фактов bookings.dim_fact_flight.
Состоит из 42-х шагов, рис.dim_fact_flight.jpeg:
1.	input dep_airports_key, получение данных из таблицы измерений bookings.dim_airports
2.	input flights arrived, получение данныех из таблицы bookings.flights, имеет подключение к БД источнику 'bd_in'.
3.	Get now, получения данных сегодняшней даты
4.	input ticket_flights, получение  данных из таблицы bookings.ticket_flight, имеет подключение к БД источнику 'bd_in'
5.	input aircraft_key, получение данных из таблицы bookings.dim_aircrafts, имеет подключение к БД назначения 'bd_out'
6.	delay arrival and departure, вычисление задержки вылета и прибытия самалета 
7.	Select flight_id выбирает из потока стобец flight_id
8.	Sort rows by flight 2, сортирует поток данных по flight_id
9.	input passenger_key, получение данных из таблицы bookings.dim_passengers, имеет подключение к БД назначения 'bd_out'
10.	Sort rows dep_airport, сортирует поток данных по departure_airport
11.	input tickets, получение вспомогательных данных из таблиц bookings.tickets, имеет подключение к БД источнику 'bd_in'
12.	Arrival flights join, добавляет к потоку данные о билетах, из таблицы  bookings.tickets_flight
13.	Remove flight_id_1, удаляет из потока столбец flight_id_1
14.	input arr_airports_key, получение данных о аэропортах из таблицы dim_airports, имеет подключение к БД назначения 'bd_out'
15.	dep_airport_key_join, добовляет к потоку данные о аэропортах прибытия.
16.	Remove and rename row, удаление из потока вспогательных данных о аэропортах и времени вылета и прибытия рейса, переименование столбца id аэропорта в ключ 
17.	passenger_key_join, добовляет к потоку данные о пассажирах из таблицы bookings.dim_passengers
18.	Sort row ticket, сортирует поток данных по ticket_no
19.	input tariff_key, получение данных из таблицы bookings.dim_tariff, имеет подключение к БД назначения 'bd_out'
20.	Remove row id, удаляет вспомогательный столбец id
21.	Sort row ticket 2, сортирует поток данных по ticket_no
22.	Ticket join, добовляет к потоку данные о билетах.
23.	Remove ticket row and rename id, удаление вспомогательных данных о билетах, и переменование стобца id пассажира в ключ
24.	Sort rows tariff, сортирует поток данных по fare_conditions
25.	Tariff join, добовляет к потоку данные о тарифах.
26.	Sort rows arr_airport, сортирует поток данных по arrival_airport
27.	arr_airport_key_join, добовляет к потоку, данные о аэропортах
28.	Remove and rename  arr_airports, удаление из потока вспогательных данных о аэропортах, переименование столбца id аэропорта в ключ
29.	Sort rows aircraft, сортирует поток данных по aircraft_code
30.	aircraft_key_join, добаляет к потоку данные о самалетах
31.	Remove  aircrafts, удаляет вспомогательные данные о самалетах
32.	Remove tariff row rename id, удаляет вспомогательные данные о тарифах, переименование столбца id тарифа в ключ,
33.	Sort rows by flight 3, сортирует поток данных по flight_id
34.	Sort rows by flight, сортирует поток данных по flight_id
35.	Flights join, соеденяет потоки по соотношению flight_id
36.	Remove flight_id, удаляет вспомогательные данные
37.	Join dt, добавляет к потоку дату
38.	Filter rows, проверка качества данных 
39.	Select values checked, выбор данных прошедших проверку качества
40.	Table output fact, выгружает данные в таблицу фатов bookings.fact_flight, имеет подключение к БД назначения 'bd_out'
41.	Select values rejected, выбор отклоненных данных
42.	Table output rejected fact, выгрузка данных не прошедших проверку в таблицу bookings.rejected_fact_flights

#### Входные данные:

##### Таблица bookings.bookings.dim_airports
Описание выше.
##### Запрос SQL:
select da.id, da.airport_code<br/>
from bookings.dim_airports da<br/>
where da.is_curent is true<br/>
order by da.airport_code;<br/>
##### Таблица bookings.flight
Естественный ключ таблицы рейсов состоит из двух полей — номера рейса (flight_no) и даты
отправления (scheduled_departure). Чтобы сделать внешние ключи на эту таблицу компактнее,
в качестве первичного используется суррогатный ключ (flight_id).
Рейс всегда соединяет две точки — аэропорты вылета (departure_airport) и прибытия
(arrival_airport). Такое понятие, как «рейс с пересадками» отсутствует: если из одного
аэропорта до другого нет прямого рейса, в билет просто включаются несколько необходимых
рейсов.
У каждого рейса есть запланированные дата и время вылета (scheduled_departure) и прибытия
(scheduled_arrival). Реальные время вылета (actual_departure) и прибытия (actual_arrival)
могут отличаться: обычно не сильно, но иногда и на несколько часов, если рейс задержан.
Статус рейса (status) может принимать одно из следующих значений:
* Scheduled Рейс доступен для бронирования. Это происходит за месяц до плановой даты вылета; до этого запись о рейсе не существует в базе данных.
* On Time Рейс доступен для регистрации (за сутки до плановой даты вылета) и не задержан.
* Delayed Рейс доступен для регистрации (за сутки до плановой даты вылета), но задержан.
* Departed Самолет уже вылетел и находится в воздухе.
* Arrived Самолет прибыл в пункт назначения.
* Cancelled Рейс отменен.<br/>
Столбец, Тип, Модификаторы, Описание<br/>
flight_id, serial, NOT NULL, Идентификатор рейса<br/>
flight_no, char(6), NOT NULL, Номер рейса<br/>
scheduled_departure, timestamptz, NOT NULL, Время вылета по расписанию<br/>
scheduled_arrival, timestamptz, NOT NULL, Время прилёта по расписанию<br/>
departure_airport, char(3), NOT NULL, Аэропорт отправления<br/>
arrival_airport, char(3), NOT NULL, Аэропорт прибытия<br/>
status, varchar(20), NOT NULL, Статус рейса<br/>
aircraft_code, char(3), NOT NULL, Код самолета, IATA<br/>
actual_departure,timestamptz, NULL, Фактическое время вылета<br/>
actual_arrival, timestamptz, NULL, Фактическое время прилёта<br/>
Индексы:<br/>
PRIMARY KEY, btree (flight_id)<br/>
UNIQUE CONSTRAINT, btree (flight_no, scheduled_departure)<br/>
Ограничения-проверки:<br/>
CHECK (scheduled_arrival > scheduled_departure)<br/>
CHECK ((actual_arrival IS NULL)  OR ((actual_departure IS NOT NULL AND actual_arrival IS NOT NULL) AND (actual_arrival > actual_departure)))<br/>
CHECK (status IN ('On Time', 'Delayed', 'Departed','Arrived', 'Scheduled', 'Cancelled'))<br/>
Ограничения внешнего ключа:<br/>
FOREIGN KEY (aircraft_code) REFERENCES aircrafts(aircraft_code)<br/>
FOREIGN KEY (arrival_airport) REFERENCES airports(airport_code)<br/>
FOREIGN KEY (departure_airport) REFERENCES airports(airport_code)<br/>
Ссылки извне:<br/>
TABLE "ticket_flights" FOREIGN KEY (flight_id)<br/>
REFERENCES flights(flight_id)<br/>
##### Запрос SQL:
select <br/>
	f.flight_id,<br/>
	f.actual_departure, <br/>
	f.actual_arrival,<br/>
	(replace(((f.actual_departure)::date)::varchar, '-', ''))::int as dt_dep_key,<br/>
	(replace(((f.actual_arrival)::date)::varchar, '-', ''))::int as dt_arr_key,<br/>
	f.scheduled_departure,<br/>
	f.scheduled_arrival,<br/>
	f.departure_airport,<br/>
	f.arrival_airport,<br/>
	f.aircraft_code<br/>
from bookings.flights f <br/>
where f.status = 'Arrived' <br/> 
	and f.actual_arrival > (select<br/>
	case <br/>
		when max(ff.actual_arrival) is not null <br/>
			then max(ff.actual_arrival)<br/>
		else '1900/01/01 02:00:00.000'<br/>
	end<br/>
	from bookings.fact_flights ff);<br/>
##### Таблица bookings.ticket_flight
Описание выше.
##### Запрос SQL:
select * from bookings.ticket_flights tf order by tf.flight_id;
##### Таблица bookings.dim_aircrafts
Описание выше.
##### Запрос SQL:
select da.id, da.aircraft_code<br/>
from bookings.dim_aircrafts da<br/>
where da.is_curent is true<br/>
order by da.aircraft_code;<br/>
##### Таблица bookings.dim_passengers
Описание выше.
##### Запрос SQL:
select dp.id, dp.passport<br/>
from bookings.dim_passengers dp<br/>
where dp.is_curent is true<br/>
order by dp.passport;<br/>
##### Таблица bookings.tickets
Описание выше.
##### Запрос SQL:
select t.ticket_no, t.passenger_id<br/>
from bookings.tickets t<br/>
order by t.passenger_id;<br/>
##### Таблица bookings.dim_tariff
Описание выше.
##### Запрос SQL:
select dt.id, dt."name"<br/>
from bookings.dim_tariff dt<br/>
where dt.is_curent is true<br/>
order by dt."name";<br/>

#### Выходные данные:


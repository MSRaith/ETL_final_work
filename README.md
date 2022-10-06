Процесс извлечения данных из базы даных по авиаперевозкам, преобразование, дополнение и выгрузка в DWH.

Процесс ETL определен заданием dwh_fw.kjb. 
Задание состоит из выполнения 9 трансформаций и стартового модуля рис.job_fw.jpeg:
	1. dim_passengers, выполняет трансформацию dwh_fw_passengers.ktr
	2. dim_aircrafts, выполняет трансформацию dwh_fw_aircrafts.ktr
	3. dim_airports, выполняет трансформацию dwh_fw_airports.ktr
	4. dim_tariff, выполняет трансформацию dwh_fw_tariff.ktr
	5. fact, выполняет трансформацию dwh_fw_fact.ktr
	6. dim_passengers_csv, выполняет трансформацию dwh_fw_passenger_csv.ktr
	7. dim_aircrafts_csv, выполняет трансформацию dwh_fw_aircrafts_csv.ktr
	8. dim_airports_csv, выполняет трансформацию dwh_fw_airports_csv.ktr
	9. dim_tariff_csv, выполняет трансформацию dwh_fw_tariff_csv.ktr
Порядок выполнения соответствует списку.

Описание трансформаций:

 1. dwh_fw_passengers.ktr
 Процесс извлечения данных о пасажирах из таблицы bookings.tickets, обогощение, проверка качества и загрузка в таблицу измерений bookings.dim_passengers.
 Состоит из 8-ми шагов, рис.dim_passengers.jpeg:
	1.input passenger, извлекает необходимые данные, имеет подключение к БД источнику 'bd_in'.
	2.select row, выбирает неоходимые данные.
	3.Get today date, получение сегоднешней даты, в формате 'date'
	4.Join dt, обогощение данных по пассажирам сегодняшней датой
	5.Filter rows, фильтрация качественых данных
	6.upload dim passengers, выгрузка данных в таблицу измерений dim_passengers
	7.Select rejected row, выбирает необходимые строки для отклоненых данных
	8.upload dim rejected passengers, выгрузка отклоненых данных в таблицу измерений dim_passengers

 Входные данные:

 Таблица bookings.ticket.
Таблица bookings.ticket содержит идентификатор пассажира (passenger_id) — номер документа,
удостоверяющего личность, — его фамилию и имя (passenger_name) и контактную
информацию (contact_date).
Ни идентификатор пассажира, ни имя не являются постоянными (можно поменять паспорт,
можно сменить фамилию), поэтому однозначно найти все билеты одного и того же пассажира
невозможно.
 Столбец        | Тип         | Модификаторы | Описание
----------------+-------------+--------------+-----------------------------
 ticket_no      | char(13)    | NOT NULL     | Номер билета
 book_ref       | char(6)     | NOT NULL     | Номер бронирования
 passenger_id   | varchar(20) | NOT NULL     | Идентификатор пассажира
 passenger_name | text        | NOT NULL     | Имя пассажира
 contact_data   | jsonb       |              | Контактные данные пассажира
Индексы:
 PRIMARY KEY, btree (ticket_no)
Ограничения внешнего ключа:
 FOREIGN KEY (book_ref) REFERENCES bookings(book_ref)
Ссылки извне:
 TABLE "ticket_flights" FOREIGN KEY (ticket_no) REFERENCES tickets(ticket_no)

 Запрос SQL 
select row_number () over () as id_key,
t2.passenger_name,
t2.passenger_id,
t2.contact_data ->> 'phone' as phone,
t2.contact_data ->> 'email' as email
FROM bookings.tickets t2 
group by t2.passenger_name, t2.passenger_id, t2.contact_data;

 Выходные данные:
 
 Таблица bookings.dim_passengers
Таблица измерений bookings.dim_passengers относится к медлено изменяемому измерению второго типа.
Содержит идентификатор записи (id), ключ пассажира (passenger_key), пассажира (паспорт) — номер документа,
удостоверяющего личность, — его фамилию и имя (passenger_name) и контактную
информацию (phone, email), начало и конец версии записи (start_ts, end_ts), статус записи (is_curent),
дата создания записи (create_ts), дата обновления записи (update_ts)

 Столбец        | Тип         | Модификаторы | Описание
----------------+-------------+--------------+-----------------------------

id              |serial       | NOT NULL     | Технический ключ    
passenger_key   |int          | NULL         | Cурогатный ключ
passenger_name  |varchar(100) | NULL         | Фамилия и имя пассажира
passport        |varchar(20)  | NULL         | Индификационый номер пассажира
email           |varchar(100) | NULL         | Почта пассажира
phone           |varchar(100) | NULL         | Телефон пассажира
start_ts        |date         | NULL         | Начало версии записи
end_ts          |date         | NULL         | Конец версии записи
is_curent       |bool         | NULL         | Текущий статус версии записи, default true
create_ts       |timestamp    | NULL         | Запись сздана, default current_timestamp
update_ts       |timestamp    | NULL         | Запись обнавлена, default current_timestamp
"version"       |int          | NULL         | Версия записи, default 1 

Индексы:
 PRIMARY KEY, dim_passengers_pkey (id),
 idx_dim_passengers_lookup (passenger_key),
 idx_dim_passengers_passport (passport)

Ссылки извне:
 TABLE "fact_flights" FOREIGN KEY (passenger_key) REFERENCES tickets(ticket_no)


Проверка качества даных:
	1. Номер телефона начинается с '+'













Описание DWH.

Таблица bookings.dim_aircrafts
Таблица измерений bookings.dim_aircrafts относится к медлено изменяемому измерению второго типа. 
Каждая модель воздушного судна идентифицируется своим трехзначным кодом
(aircraft_code). Указывается также название модели (model) и максимальная дальность полета
в километрах (range). 

 Столбец       | Тип             | Модификаторы | Описание
---------------+-----------------+--------------+-----------------------------------
 id            | serial          | NULL         | Уникальный идентификатор записи, первичный ключ таблицы
 aircraft_key  | integer         | NULL         | Индефикатор самалета, сурогатный ключ
 aircraft_code | char(3)         | NULL         | Код самолета, IATA
 model         | varchar(100)    | NULL         | Модель самолета
 range         | integer         | NULL         | Максимальная дальность полета, км
Индексы:
 PRIMARY KEY, serial (id)
 Сурогатный ключ, aircraft_key
## Работа с Hive через интерфейс Hue

csv-файлы обработаны [скриптом](https://github.com/PolarJaba/DE_1st_work_with_hive/blob/main/data/add_column.py). Добавлена колонка cust_group, разделяющая данные на 10 частей, наиболее близких к равным.

Для удобства дальнейшей работы создан также файл [age_groups.csv](https://github.com/PolarJaba/DE_1st_work_with_hive/blob/main/data/age_groups.csv), содержащий данные о возрастных категориях.

Полученные данные загружены в hdfs:

```
C:\Projects\4-5_hive>docker cp data 92e57d2c1a86:/usr/hive_practice/
```
```
C:\Projects\4-5_hive>docker exec -it 92e57d2c1a86 bash
```
```
root@92e57d2c1a86:/# hdfs dfs -put usr/hive_practice /user/polar_jabka/hive_practice
```
```
root@92e57d2c1a86:/# hdfs dfs -ls /user/polar_jabka/hive_practice
```
```
Found 6 items
-rw-r--r--   3 root polar_jabka       1460 2023-08-14 15:46 /user/polar_jabka/hive_practice/add_column.py
-rw-r--r--   3 root polar_jabka         95 2023-08-14 15:46 /user/polar_jabka/hive_practice/age_groups.csv
-rw-r--r--   3 root polar_jabka   17896460 2023-08-14 15:46 /user/polar_jabka/hive_practice/customers_groups.csv
-rw-r--r--   3 root polar_jabka   14250551 2023-08-14 15:46 /user/polar_jabka/hive_practice/orgs_groups.csv
-rw-r--r--   3 root polar_jabka   11847642 2023-08-14 15:46 /user/polar_jabka/hive_practice/people_groups.csv
drwxr-xr-x   - root polar_jabka          0 2023-08-14 15:46 /user/polar_jabka/hive_practice/raw_data
```

SQL-скрипты для создания таблиц и витринны данных находятся в файле [queries.sql](https://github.com/PolarJaba/DE_1st_work_with_hive/blob/main/queries.sql)

В результате получена таблица:

![result](https://github.com/PolarJaba/DE_1st_work_with_hive/blob/main/result.PNG)

Точность полученных данных проверена через вывод количества вхождений клиентов конкретной компании определенной возрастной группы в конктретном году:

![result_check](https://github.com/PolarJaba/DE_1st_work_with_hive/blob/main/result_check.PNG)

CREATE ROLE fin with encrypted password 'password';

CREATE DATABASE findb;

CREATE TABLE actual_finance (
    Series_reference        varchar(255) NOT NULL,
    Period       real,
    Data_value        real,
    Suppressed   boolean,
    STATUS        char(1),
    UNITS         varchar(255) NOT NULL
    Magnitude        integer,
    Subject        varchar(255) NOT NULL,
    Group        varchar(255) NOT NULL,
    Series_title_1        varchar(255) NOT NULL,
    Series_title_2        varchar(255) NOT NULL,
    Series_title_3        varchar(255) NOT NULL,
    Series_title_4        varchar(255) NOT NULL,
    Series_title_5        varchar(255) NOT NULL


);

CREATE TABLE Period_finance (
    Series_reference        varchar(255) NOT NULL,
    Period       real,
    Data_value        real,
    Suppressed   boolean,
    STATUS        char(1),
    UNITS         varchar(10) NOT NULL
    Magnitude        integer,
    Subject        varchar(255) NOT NULL,
    Group        varchar(255) NOT NULL,
    Series_title_1        varchar(255) NOT NULL,
    Series_title_2        varchar(255) NOT NULL,
    Series_title_3        varchar(255) NOT NULL,
    Series_title_4        varchar(255) NOT NULL,
    Series_title_5        varchar(255) NOT NULL


);

CREATE TABLE history_finance (
    Series_reference        varchar(255) NOT NULL,
    Period       real,
    Data_value        real,
    Suppressed   boolean,
    STATUS        char(1),
    UNITS         varchar(10) NOT NULL
    Magnitude        integer,
    Subject        varchar(255) NOT NULL,
    Group        varchar(255) NOT NULL,
    Series_title_1        varchar(255) NOT NULL,
    Series_title_2        varchar(255) NOT NULL,
    Series_title_3        varchar(255) NOT NULL,
    Series_title_4        varchar(255) NOT NULL,
    Series_title_5        varchar(255) NOT NULL


);

GRANT all privileges on database findb to fin;
GRANT ALL PRIVILEGES ON TABLE actual_finance TO fin;
GRANT ALL PRIVILEGES ON TABLE Period_finance TO fin;
GRANT ALL PRIVILEGES ON TABLE history_finance TO fin;




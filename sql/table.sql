CREATE TABLE users (
    code    char(5) CONSTRAINT firstkey PRIMARY KEY,
    email   varchar(40) NOT NULL,
    date    date
);

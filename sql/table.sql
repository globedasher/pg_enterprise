CREATE TABLE IF NOT EXISTS users (
    code    char(5) CONSTRAINT firstkey PRIMARY KEY,
    email   varchar(40) NOT NULL,
    date    date
);

INSERT INTO users (code, email, date) VALUES (32134, 'waste@mail.com', now()) ON CONFLICT DO NOTHING;
INSERT INTO users (code, email, date) VALUES (232, 'wind@mail.com', now()) ON CONFLICT DO NOTHING;
INSERT INTO users (code, email, date) VALUES (3234, 'will@mail.com', now()) ON CONFLICT DO NOTHING;
INSERT INTO users (code, email, date) VALUES (134, 'green@mail.com', now()) ON CONFLICT DO NOTHING;
INSERT INTO users (code, email, date) VALUES (555, 'oldman@mail.com', now()) ON CONFLICT DO NOTHING;

SELECT * FROM users;

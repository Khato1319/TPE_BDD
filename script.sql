CREATE TABLE continente
(
    idC     SERIAL,
    nombreC TEXT,
    PRIMARY KEY (idC),
    UNIQUE (nombreC)
);

CREATE TABLE region
(
    idR     SERIAL,
    nombreR TEXT,
    idC     INT,
    PRIMARY KEY (idR),
    UNIQUE (nombreR),
    FOREIGN KEY (idC) REFERENCES continente ON DELETE CASCADE
);

CREATE TABLE pais
(
    idP     SERIAL,
    nombreP TEXT NOT NULL,
    idR     INT  NOT NULL,
    PRIMARY KEY (idP),
    FOREIGN KEY (idR) REFERENCES region ON DELETE CASCADE
);

CREATE TABLE anio
(
    anio     INT     NOT NULL,
    bisiesto BOOLEAN NOT NULL,
    PRIMARY KEY (anio)
);

CREATE OR REPLACE FUNCTION isleapyear(year integer)
    RETURNS boolean AS
$$
BEGIN
    RETURN (year % 4 = 0) AND ((year % 100 <> 0) or (year % 400 = 0));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION inserta_anio() RETURNS trigger AS
$$
DECLARE
        qty INT;
BEGIN
    new.bisiesto := isleapyear(new.anio);
    SELECT count(*) from anio where anio.anio = new.anio INTO qty;
    IF (qty > 0) THEN
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertaanio
    BEFORE INSERT
    ON anio
    FOR EACH ROW
EXECUTE PROCEDURE inserta_anio();



CREATE TABLE turistas
(
    idP      INT NOT NULL,
    total    INT NOT NULL,
    aerea    INT NOT NULL,
    maritima INT NOT NULL,
    anio     INT NOT NULL,
    FOREIGN KEY (anio) REFERENCES anio (anio) ON DELETE CASCADE,
    FOREIGN KEY (idP) REFERENCES pais (idP) ON DELETE CASCADE,
    PRIMARY KEY (anio, idP)
);

CREATE OR REPLACE FUNCTION inserta_turista() RETURNS trigger AS
$$
BEGIN

END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER insertaturita
    BEFORE INSERT
    ON turistas
    FOR EACH ROW
EXECUTE PROCEDURE inserta_turista();

-- COPY turistas (pais, total, aerea, maritima, region, continente, anio) FROM './tourists-rj.csv'




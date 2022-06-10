CREATE TABLE continente
(
    idC     SERIAL,
    nombreC TEXT,
    PRIMARY KEY (idC),
    UNIQUE (nombreC)
);

CREATE OR REPLACE FUNCTION inserta_continente() RETURNS trigger AS
$$
DECLARE
        qty INT;
BEGIN
    SELECT count(*) from continente where continente.nombreC = new.nombreC INTO qty;
    IF (qty > 0) THEN
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertacontinente
    BEFORE INSERT
    ON continente
    FOR EACH ROW
EXECUTE PROCEDURE inserta_continente();


CREATE TABLE region
(
    idR     SERIAL,
    nombreR TEXT,
    idC     INT,
    PRIMARY KEY (idR),
    UNIQUE (nombreR),
    FOREIGN KEY (idC) REFERENCES continente ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION inserta_region() RETURNS trigger AS
$$
DECLARE
        qty INT;
BEGIN
    SELECT count(*) from region where region.nombreR = new.nombreR INTO qty;
    IF (qty > 0) THEN
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertaregion
    BEFORE INSERT
    ON region
    FOR EACH ROW
EXECUTE PROCEDURE inserta_region();

CREATE TABLE pais
(
    idP     SERIAL,
    nombreP TEXT NOT NULL,
    idR     INT  NOT NULL,
    PRIMARY KEY (idP),
    FOREIGN KEY (idR) REFERENCES region ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION inserta_pais() RETURNS trigger AS
$$
DECLARE
        qty INT;
BEGIN
    SELECT count(*) from pais where pais.nombreP = new.nombreP and pais.idR = new.idR INTO qty;
    IF (qty > 0) THEN
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertapais
    BEFORE INSERT
    ON pais
    FOR EACH ROW
EXECUTE PROCEDURE inserta_pais();

CREATE TABLE anio
(
    anio     INT     NOT NULL,
    bisiesto BOOLEAN NOT NULL DEFAULT false,
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
    pais TEXT,
        aerea    INT NOT NULL,
    maritima INT NOT NULL,
    total    INT NOT NULL,
    region TEXT,
    continente TEXT,
    anio     INT NOT NULL,
        idP      INT NOT NULL,
    FOREIGN KEY (anio) REFERENCES anio (anio) ON DELETE CASCADE,
    FOREIGN KEY (idP) REFERENCES pais (idP) ON DELETE CASCADE,
    PRIMARY KEY (anio, idP)
);

CREATE OR REPLACE FUNCTION inserta_turista() RETURNS trigger AS
$$
    DECLARE
        contId INT;
        regId INT;
        paisId INT;
BEGIN
    INSERT INTO anio(anio) values (new.anio);
    INSERT INTO continente(nombreC) values (new.continente) RETURNING idC INTO contId;
    IF contId IS NULL then
        SELECT idC FROM continente where nombreC = new.continente into contId;
    end if;
    INSERT INTO region(nombreR, idC) values (new.region, contId) RETURNING idR INTO regId;
    IF regId IS NULL then
        SELECT idR FROM region where nombreR = new.region into regId;
    end if;
    INSERT INTO pais(nombreP, idR) values (new.pais, regId) RETURNING idP INTO paisId;
    IF paisId IS NULL then
        SELECT idP FROM pais where nombreP = new.pais and idR = regId into paisId;
    end if;
    new.idP := paisId;
RETURN new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER insertaturita
    BEFORE INSERT
    ON turistas
    FOR EACH ROW
EXECUTE PROCEDURE inserta_turista();

CREATE OR REPLACE PROCEDURE copy_data()AS
$$

BEGIN
   copy turistas (pais, total, aerea, maritima, region, continente, anio) FROM 'C:\Users\khcat\DataGripProjects\TPE\tourists-rj.csv' csv header;

END;
$$ LANGUAGE plpgsql;

CALL copy_data();




ALTER TABLE turistas DROP COLUMN pais;
ALTER TABLE turistas DROP COLUMN region;
ALTER TABLE turistas DROP COLUMN continente;





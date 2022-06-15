CREATE TABLE continente (
    idC SERIAL,
    nombreC TEXT,
    PRIMARY KEY (idC),
    UNIQUE (nombreC)
);

CREATE OR REPLACE FUNCTION inserta_continente() RETURNS trigger AS $$
    DECLARE
        qty INT;

    BEGIN
        SELECT
            count(*)
        FROM
            continente
        WHERE
            continente.nombreC = new.nombreC INTO qty;
        IF (qty > 0) THEN
            RETURN NULL;
        ELSE
            RETURN new;
        END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertacontinente BEFORE INSERT ON continente FOR EACH ROW EXECUTE PROCEDURE inserta_continente();


CREATE TABLE region (
    idR SERIAL,
    nombreR TEXT,
    idC INT,
    PRIMARY KEY (idR),
    UNIQUE (nombreR, idC),
    FOREIGN KEY (idC) REFERENCES continente ON DELETE CASCADE
);


CREATE OR REPLACE FUNCTION inserta_region() RETURNS trigger AS $$
    DECLARE
        qty INT;
    BEGIN
        SELECT
            count(*)
        FROM
            region
        WHERE
            region.nombreR = new.nombreR AND region.idC = new.idC INTO qty;
        IF (qty > 0) THEN
            RETURN NULL;
        ELSE
            RETURN new;
        END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertaregion BEFORE INSERT ON region FOR EACH ROW EXECUTE PROCEDURE inserta_region();


CREATE TABLE pais (
    idP SERIAL,
    nombreP TEXT NOT NULL,
    idR INT NOT NULL,
    PRIMARY KEY (idP),
    UNIQUE (nombreP, idR),
    FOREIGN KEY (idR) REFERENCES region ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION inserta_pais() RETURNS trigger AS $$
    DECLARE
        qty INT;
    BEGIN
        SELECT
            count(*)
        FROM
            pais
        WHERE
            pais.nombreP = new.nombreP AND pais.idR = new.idR INTO qty;
        IF (qty > 0) THEN
            RETURN NULL;
        ELSE
            RETURN new;
        END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertapais BEFORE INSERT ON pais FOR EACH ROW EXECUTE PROCEDURE inserta_pais();


CREATE TABLE anio (
    anio INT NOT NULL,
    bisiesto BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (anio)
);

CREATE OR REPLACE FUNCTION isleapyear(year INT) RETURNS boolean AS $$
    BEGIN
        RETURN (year % 4 = 0) AND ((year % 100 <> 0) OR (year % 400 = 0));
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION inserta_anio() RETURNS trigger AS $$
    DECLARE
        qty INT;

    BEGIN
        new.bisiesto := isleapyear(new.anio);
        SELECT
            count(*)
        FROM
            anio
        WHERE
            anio.anio = new.anio INTO qty;

        IF (qty > 0) THEN
            RETURN NULL;
        ELSE
            RETURN new;
        END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertaanio BEFORE INSERT ON anio FOR EACH ROW EXECUTE PROCEDURE inserta_anio();


CREATE TABLE turista (
    aerea INT NOT NULL,
    maritima INT NOT NULL,
    total INT NOT NULL,
    anio INT NOT NULL,
    idP INT NOT NULL,
    FOREIGN KEY (anio) REFERENCES anio (anio) ON DELETE CASCADE,
    FOREIGN KEY (idP) REFERENCES pais (idP) ON DELETE CASCADE,
    PRIMARY KEY (anio, idP)
);

CREATE VIEW turistas AS
SELECT nombreP AS pais, total, aerea, maritima, nombreR AS region, nombreC AS continente, anio
FROM turista NATURAL JOIN pais NATURAL JOIN
region NATURAL JOIN continente;

CREATE OR REPLACE FUNCTION inserta_turista() RETURNS trigger AS $$
    DECLARE
        contId INT;
        regId INT;
        paisId INT;

    BEGIN
        INSERT INTO anio(anio) VALUES (new.anio);
        INSERT INTO continente(nombreC) VALUES (new.continente) RETURNING idC INTO contId;

        IF (contId IS NULL) THEN
            SELECT
                idC
            FROM
                continente
            WHERE
                nombreC = new.continente INTO contId;
        END IF;

        INSERT INTO region(nombreR, idC) VALUES (new.region, contId) RETURNING idR INTO regId;

        IF (regId IS NULL) THEN
            SELECT
                idR
            FROM
                region
            WHERE
                nombreR = new.region INTO regId;
        END IF;

        INSERT INTO pais(nombreP, idR) VALUES (new.pais, regId) RETURNING idP INTO paisId;

        IF (paisId IS NULL) THEN
            SELECT
                idP
            FROM
                pais
            WHERE
                nombreP = new.pais AND idR = regId INTO paisId;
        END IF;

        INSERT INTO turista(aerea, maritima, total, anio, idP) values(new.aerea, new.maritima, new.total, new.anio, paisId);

        RETURN null;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insertaturista INSTEAD OF INSERT ON turistas FOR EACH ROW EXECUTE PROCEDURE inserta_turista();

CREATE OR REPLACE FUNCTION AnalisisTransporte(
    IN anio INT,
    IN sumaAerea INT,
    IN sumaMaritima INT,
    IN sumaTotal INT
) RETURNS VOID AS $$
    DECLARE
        cantPaises INT := (
            SELECT
                count(idp)
            FROM
                turista
            WHERE
                turista.anio = AnalisisTransporte.anio
        );

    BEGIN
        IF anio = 0 THEN
            RETURN;
        END IF;
        IF cantPaises = 0 THEN
            cantPaises := 1;
        END IF;

        RAISE NOTICE '----   Transporte:   Aereo   %   %',
        sumaAerea,
        (sumaAerea / cantPaises) :: INT;

        RAISE NOTICE '----   Transporte:   Maritimo   %   %',
        sumaMaritima,
        (sumaMaritima / cantPaises) :: INT;

        RAISE NOTICE '----------------------------%   %',
        sumaTotal,
        ((sumaTotal) / cantPaises) :: INT;
    END;
$$ LANGUAGE plpgsql;

SELECT
    anio,
    nombreC,
    SUM(turista.total) AS total,
    AVG(turista.total) :: INT AS promedio,
    SUM(turista.aerea) AS aerea,
    SUM(turista.maritima) AS maritima
FROM
    turista NATURAL JOIN pais NATURAL JOIN region NATURAL JOIN continente
WHERE
    anio = 2007
GROUP BY
    nombrec, anio
ORDER BY
    anio;

CREATE OR REPLACE FUNCTION AnalisisConsolidado(IN qty INT) RETURNS VOID AS $$
    DECLARE
    fila RECORD;
    ultimoAnio INT := NULL;
    sumaAerea INT := 0;
    sumaMaritima INT := 0;
    sumaTotal INT := 0;
    flag boolean := false;

    cursor CURSOR FOR (
        SELECT
            anio,
            nombreC,
            SUM(turista.total) AS total,
            AVG(turista.total) :: INT AS promedio,
            SUM(turista.aerea) AS aerea,
            SUM(turista.maritima) AS maritima
        FROM
            turista NATURAL JOIN pais NATURAL JOIN region NATURAL JOIN continente
        GROUP BY
            nombrec, anio
        ORDER BY
            anio
    );
    printedYear BOOLEAN := FALSE;

    BEGIN
        IF (qty < 0) THEN
            RAISE EXCEPTION 'LA CANTIDAD NO PUEDE SER NEGATIVA' USING ERRCODE = 'PP111';
        END IF;
        IF (qty = 0) THEN
            RETURN;
        END IF;

        RAISE NOTICE '--------------------------------------------';
        RAISE NOTICE '-------CONSOLIDATED TOURIST REPORT----------';
        RAISE NOTICE '--------------------------------------------';
        RAISE NOTICE 'Year---Category--------------Total---Average';
        RAISE NOTICE '--------------------------------------------';
        OPEN cursor;
        LOOP
            FETCH cursor INTO fila;
            EXIT
            WHEN NOT FOUND;

            IF (QTY <=0) THEN
                EXIT;
            END IF;

            IF ((ultimoAnio IS NOT NULL) AND (fila.anio <> ultimoAnio)) THEN
                PERFORM AnalisisTransporte(ultimoAnio, sumaAerea, sumaMaritima, sumaTotal);
                sumaMaritima := 0;
                sumaAerea := 0;
                sumaTotal := 0;
                printedYear := FALSE;
                qty := qty - 1;
                IF (qty <= 0) THEN
                    flag := true;
                     EXIT;
                END IF;
            END IF;

            sumaAerea := sumaAerea + fila.aerea;
            sumaMaritima := sumaMaritima + fila.maritima;
            sumaTotal := sumaTotal + fila.total;
            ultimoAnio := fila.anio;

            IF (printedYear) THEN
                RAISE NOTICE '----   Continente: %   %   %',
                fila.nombreC,
                fila.total,
                fila.promedio;

            ELSE
                RAISE NOTICE '%   Continente: %   %   %',
                fila.anio,
                fila.nombreC,
                fila.total,
                fila.promedio;
                printedYear := TRUE;
            END IF;
        END LOOP;

        IF (NOT flag AND ultimoAnio IS NOT NULL) THEN
                PERFORM AnalisisTransporte(ultimoAnio, sumaAerea, sumaMaritima, sumaTotal);
        END IF;
    END;
$$ LANGUAGE plpgsql;

SELECT AnalisisConsolidado(2);

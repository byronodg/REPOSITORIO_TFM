create or replace  PROCEDURE generacion_declaraciones (
    nombre_provincia   IN varchar2 DEFAULT NULL )
IS
    var_ANIO               NUMBER := 2022;
    var_MES                NUMBER;
    var_CODIGO_SECTOR_N1   VARCHAR2 (10);
    var_PROVINCIA          VARCHAR2 (100);
    var_CANTON             VARCHAR2 (100);
    var_VENTAS_NETAS_12    NUMBER (24,2);
    var_VENTAS_NETAS_0     NUMBER (24,2);
    var_EXPORTACIONES      NUMBER (24,2);
    var_COMPRAS_NETAS_12    NUMBER (24,2);
    var_COMPRAS_NETAS_0    NUMBER (24,2);
    var_IMPORTACIONES      NUMBER (24,2);
    var_COMPRAS_RISE       NUMBER (24,2);
    var_TOTAL_COMPRAS      NUMBER (24,2);
    var_TOTAL_VENTAS       NUMBER (24,2);
BEGIN
 -- GENERACION DE UN VALOR RANDÓMICO PARA EL MES 
 var_mes := ROUND (DBMS_RANDOM.VALUE (1, 12));
  /*OBTENCION DE UN VALOR RANDÓMICO DE PROVINCIA  REALIZANDO 
        PREVIAMENTE LA VALIDACION SI SE INVOCÓ AL PROCEDIMIENTO
        CON VALOR DE PROVINCIA EN ESTADO NULL, SE OBTIENE PROVINCIA Y CANTÓN
        DE FORMA RANDÓMICA*/ 
        -- GENERACION DEL CÓDIGO DE SECTOR CON VALORES ENTRE A y X
        var_CODIGO_SECTOR_N1:=chr(DBMS_RANDOM.VALUE (65, 88));
        -- GENERACION DE VALORES RANDOMICOS PARA LOS CAMPOS TIPO NUMÉRICO
        var_VENTAS_NETAS_12 := ROUND (DBMS_RANDOM.VALUE (10000, 100000), 2);
       /* var_VENTAS_NETAS_0 := ROUND (DBMS_RANDOM.VALUE (1, 100000), 2);
        var_EXPORTACIONES := ROUND (DBMS_RANDOM.VALUE (1, 100000), 2);
        var_COMPRAS_NETAS_12 := ROUND (DBMS_RANDOM.VALUE (1, 100000), 2);
        var_COMPRAS_NETAS_0 := ROUND (DBMS_RANDOM.VALUE (1, 100000), 2);
        var_IMPORTACIONES := ROUND (DBMS_RANDOM.VALUE (1, 100000), 2);
        var_COMPRAS_RISE := ROUND (DBMS_RANDOM.VALUE (1, 100000), 2);*/
        var_COMPRAS_NETAS_12 := ROUND (DBMS_RANDOM.VALUE (10000, 100000), 2);

        var_VENTAS_NETAS_0:=var_VENTAS_NETAS_12*1.1;
        var_EXPORTACIONES:=var_VENTAS_NETAS_12*1.2;
        var_COMPRAS_NETAS_0:=var_COMPRAS_NETAS_12*1.1;
        var_IMPORTACIONES:=var_COMPRAS_NETAS_12*1.5;
        var_COMPRAS_RISE:=var_COMPRAS_NETAS_12*0.05;

        --OBTENCIÓN DE LOS TOTALES DE COMPRAS Y VENTAS
        var_TOTAL_COMPRAS :=var_COMPRAS_NETAS_12 + var_COMPRAS_NETAS_0 + var_COMPRAS_RISE;
        var_TOTAL_VENTAS := var_VENTAS_NETAS_12 + var_VENTAS_NETAS_0;


    FOR i IN 1 .. 1500
    LOOP


        if nombre_provincia is null then 
                SELECT provincia, canton into var_provincia, var_canton
            FROM   (
            SELECT *
            FROM   geografica
            ORDER BY DBMS_RANDOM.RANDOM)
            WHERE  
        rownum =1;

        else  --SI LA PROVINCIA NO ES NULO SE OBTIENE UN CANTÓN DE FORMA RANDÓMICA
         SELECT  canton into  var_canton
            FROM   (
            SELECT *
            FROM   geografica WHERE  PROVINCIA=nombre_provincia 
            ORDER BY DBMS_RANDOM.RANDOM)
            WHERE   
        rownum =1;
        var_provincia:=nombre_provincia;
        end if; 


        --INSERCIÓN EN LA TABLA DE DECLARACIONES 
        INSERT /*+ append */ INTO declaraciones nologging (anio,
                                   mes,provincia, canton, CODIGO_SECTOR_N1,
                                   VENTAS_NETAS_12,
                                   VENTAS_NETAS_0,
                                   EXPORTACIONES,
                                   COMPRAS_NETAS_12,
                                   COMPRAS_NETAS_0,
                                   IMPORTACIONES,
                                   COMPRAS_RISE,
                                   TOTAL_COMPRAS,
                                   TOTAL_VENTAS)
             VALUES (var_anio,
                     var_mes,var_provincia, var_canton, var_CODIGO_SECTOR_N1,
                     var_VENTAS_NETAS_12,
                     var_VENTAS_NETAS_0,
                     var_EXPORTACIONES,
                     var_COMPRAS_NETAS_12,
                     var_COMPRAS_NETAS_0,
                     var_IMPORTACIONES,
                     var_COMPRAS_RISE,
                     var_TOTAL_COMPRAS,
                     var_TOTAL_VENTAS);
    END LOOP;
    COMMIT;
END;

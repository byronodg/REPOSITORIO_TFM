/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package javaapplication1;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.sql.*;
/**
 *
 * @author bodg010715
 */

/*
create table declaracion_detalle (
AgnO	number,
MES	number,
CODIGO_SECTOR_N1	Varchar2(1),
PROVINCIA	Varchar2(100),
CANTON	Varchar2(100),
Ventas_netas12	number,
Ventas_netas0	number,
EXPORTACIONES	number,
Compras_netas12	number,
Compras_netas0	number,
IMPORTACIONES	number,
COMPRAS_RISE	number,
TOTAL_COMPRAS	number,
TOTAL_VENTAS	number)

 int getRandomValue = ThreadLocalRandom.current().nextInt(10, 20);
        System.out.println(getRandomValue);

import java.util.concurrent.ThreadLocalRandom;
 */
import java.util.Random;
import java.lang.*;

public class hilo_provincia {

    int numero_hilo = 0;
    int contador_select = 0;
    public boolean ejecuta = true;
    String provincia = new String();
    // Elementos de conexión a la base de datos
    String dbURL1 = "jdbc:oracle:thin:dwh/dwh@192.168.1.52:1521:XE";
    Connection conn1 = null;
    String sentencia = new String();
    Random rand = new Random();

    public hilo_provincia(int numero_hilo) {
        this.numero_hilo = numero_hilo;

    }

    public void decodifica_provincia(int numero_hilo) {
        switch (numero_hilo) {
            case 1:
                provincia = "AZUAY";
                break;
            case 2:
                provincia = "BOLIVAR";
                break;
            case 3:
                provincia = "CARCHI";
                break;
            case 4:
                provincia = "CAÑAR";
                break;
            case 5:
                provincia = "CHIMBORAZO";
                break;
            case 6:
                provincia = "COTOPAXI";
                break;
            case 7:
                provincia = "EL ORO";
                break;
            case 8:
                provincia = "ESMERALDAS";
                break;
            case 9:
                provincia = "GALAPAGOS";
                break;
            case 10:
                provincia = "GUAYAS";
                break;
            case 11:
                provincia = "IMBABURA";
                break;
            case 12:
                provincia = "LOJA";
                break;
            case 13:
                provincia = "LOS RIOS";
                break;
            case 14:
                provincia = "MANABI";
                break;
            case 15:
                provincia = "MORONA SANTIAGO";
                break;
            case 16:
                provincia = "NAPO";
                break;
            case 17:
                provincia = "ORELLANA";
                break;
            case 18:
                provincia = "PASTAZA";
                break;
            case 19:
                provincia = "PICHINCHA";
                break;
            case 20:
                provincia = "SANTA ELENA";
                break;
            case 21:
                provincia = "SANTO DOMINGO DE LOS TSACHILAS";
                break;
            case 22:
                provincia = "SUCUMBIOS";
                break;
            case 23:
                provincia = "TUNGURAHUA";
                break;
            case 24:
                provincia = "ZAMORA CHINCHIPE";
                break;
            default:
                provincia = null;
        }
    }

    public Thread thread = new Thread("Hilo Maneja Provincia") {
        public boolean ejecuta = true;

        public void run() {
            while (ejecuta == true) {
                inserta_datos_base();
            }
        }
    };

    public void activa_hilo() {
        decodifica_provincia(numero_hilo);
        if (contador_select == 0) {
            this.thread.start();

        } else {
            this.thread.resume();
        }
        contador_select = 1;
    }

    public void desactiva_hilo() {
        this.thread.suspend();
    }

    public void inserta_datos_base() {

        try {
            Class.forName("oracle.jdbc.OracleDriver");

            conn1 = DriverManager.getConnection(dbURL1);
            sentencia = "CALL generacion_declaraciones";
            CallableStatement pstmt = conn1.prepareCall("{CALL generacion_declaraciones(?)}");
            pstmt.setString(1, provincia);
            pstmt.executeUpdate();
            conn1.close();

            try {
                this.thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(hilo_provincia.class.getName()).log(Level.SEVERE, null, ex);
            }

        } catch (SQLException ex) {
            Logger.getLogger(hilo_provincia.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(hilo_provincia.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}

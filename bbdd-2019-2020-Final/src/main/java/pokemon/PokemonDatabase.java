package pokemon;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


public class PokemonDatabase {

    private static final Logger logger = Logger.getLogger(PokemonDatabase.class.getName());
    private Connection connection;

    public PokemonDatabase() {

    }

    public boolean connect() {
        try {
            if (connection != null && !connection.isClosed()) {
                return true;
            }

            Properties connectionProps = new Properties();
            connectionProps.put("user", "pokemon_user");
            connectionProps.put("password", "pokemon_pass");
            connectionProps.put("serverTimezone", "UTC");
            String serverAddress = "localhost:3306";
            String database = "pokemon";
            String url = "jdbc:mysql://" + serverAddress + "/" + database;

            connection = DriverManager.getConnection(url, connectionProps);

            logger.info("Connection established successfully");
            return true;

        } catch (SQLException e) {
            generateExceptionMessage(e);
            return false;
        }
    }

    public boolean disconnect() {
        try {
            if (connection == null || connection.isClosed()) return false;

            connection.close();
            logger.info("Connection closed successfully");
            return true;

        } catch (SQLException e) {
            generateExceptionMessage(e);
            return false;
        }
    }

    public boolean createTableAprende() {
        if (!connect()) return false;                                               //comprobamos la conexión con la BBDD
        try (PreparedStatement pst = connection.prepareStatement("CREATE TABLE aprende (" +
                "N_Pokedex INT," +
                "ID_Ataque INT," +
                "Nivel INT," +
                "PRIMARY KEY (N_Pokedex,ID_Ataque)," +
                "FOREIGN KEY (N_Pokedex) REFERENCES especie (N_Pokedex)," +
                "FOREIGN KEY (ID_Ataque) REFERENCES ataque (ID_Ataque)" +
                "ON DELETE CASCADE ON UPDATE CASCADE);")) {
            pst.execute();
            return true;

        } catch (SQLException e) {
            generateExceptionMessage(e);
            return false;
        }
    }

    public boolean createTableConoce() {
        if (!connect()) return false;                                               //comprobamos la conexión con la BBDD
        try (PreparedStatement pst = connection.prepareStatement("CREATE TABLE conoce (" +
                "N_Encuentro INT," +
                "N_Pokedex INT," +
                "ID_Ataque INT," +
                "PRIMARY KEY (N_Pokedex,N_Encuentro,ID_Ataque)," +
                "FOREIGN KEY (N_Pokedex,N_Encuentro) REFERENCES ejemplar (n_Pokedex,n_encuentro)," +
                "FOREIGN KEY  (ID_Ataque) REFERENCES  ataque (ID_Ataque)" +
                "ON DELETE CASCADE ON UPDATE CASCADE);")) {
            pst.execute();
            return true;

        } catch (SQLException e) {
            generateExceptionMessage(e);
            return false;
        }
    }

    public int loadAprende(String fileName) {
        if (!connect()) return 0;                                                   //comprobamos la conexión con la BBDD
        ArrayList<Aprende> listaAprende = Aprende.readData(fileName);               //ArrayList del tipo Aprende donde cargamos la información de Aprende csv
        if (listaAprende.isEmpty()) return 0;                                       // Si el ArrayList está vacio devolvemos 0, no se ha cargado elementos
        int res = 0;                                                                // Variable contador auxiliar para manejar el número de elementos cargados
        for (Aprende var : listaAprende) {                                          // Bucle para recorrer todos los elementos del tipo Aprende cargados en el ArrayList listaAprende
            try (PreparedStatement pst = connection.prepareStatement("INSERT INTO aprende (N_pokedex, ID_Ataque, Nivel) " + //Insertamos los elementos en la BBDD
                    "VALUES (?,?,?);")) {
                pst.setInt(1, var.getId_especie());
                pst.setInt(2, var.getId_ataque());
                pst.setInt(3, var.getNivel());
                res = res + pst.executeUpdate();                                    //aumentamos la variable contador de elementos
            } catch (SQLException e) {
                generateExceptionMessage(e);
            }
        }
        return res;                                                                 // Devolvemos el número de elementos cargados
    }

    public int loadConoce(String fileName) {
        if (!connect()) return 0;                                                   //comprobamos la conexión con la BBDD
        ArrayList<Conoce> listaConoce = Conoce.readData(fileName);                  //ArrayList del tipo Conoce donde cargamos la información de Conoce csv
        if (listaConoce.isEmpty()) return 0;                                        // Si el ArrayList está vacio devolvemos 0, no se ha cargado elementos
        int res = 0;                                                                // Variable contador auxiliar para manejar el número de elementos cargados
        try (PreparedStatement pst = connection.prepareStatement("INSERT INTO conoce (N_Encuentro, N_Pokedex, ID_Ataque) " + //Insertamos los elementos en la BBDD
                "VALUES (?,?,?);")) {
            connection.setAutoCommit(false);                                        // Tratamos la creación y carga de todos los datos como si fuera una única transacción
            for (Conoce var : listaConoce) {                                        // Bucle para recorrer todos los elementos del tipo Conoce cargados en el ArrayList listaConoce
                pst.setInt(1, var.getN_encuentro());
                pst.setInt(2, var.getId_especie());
                pst.setInt(3, var.getId_ataque());
                res = res + pst.executeUpdate();                                    //aumentamos la variable contador de elementos
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException throwables) {
            generateExceptionMessage(throwables);
            try {                                                                   // Cualquier fallo intermedio de lugar a deshacer por completo los cambios anteriores
                connection.rollback();
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                generateExceptionMessage(e);
                return 0;                                                           // Devolvemos 0 elementos cargados ya que ha habido un error y cualquier fallo intermedio debe deshacer los cambios anteriores
            }                                                                       // por lo que no se ha cargado ningún elemento en la BBDD
            return 0;                                                               // Devolvemos 0 elementos cargados ya que ha habido un error y cualquier fallo intermedio debe deshacer los cambios anteriores
        }                                                                           // por lo que no se ha cargado ningún elemento en la BBDD
        return res;                                                                 // Devolvemos el número de elementos cargados
    }

    public ArrayList<Especie> pokedex() {
        if (!connect()) return null;                                                //comprobamos la conexión con la BBDD
        ArrayList<Especie> res = new ArrayList<>();                                 // Arraylist auxiliar donde guardaremos los elementos de la tabla especie, los elementos serán del tipo Especie
        try (PreparedStatement pst = connection.prepareStatement("SELECT * FROM especie;"); // Obtenemos todos los elementos de la tabla especie
             ResultSet rst = pst.executeQuery()) {
            while (rst.next()) {
                Especie var = new Especie();                                        // Variable del tipo Especie
                var.setN_pokedex(rst.getInt("N_pokedex"));
                var.setNombre(rst.getString("Nombre"));
                var.setDescripcion(rst.getString("descripcion"));
                var.setEvoluciona(rst.getInt("evoluciona"));
                res.add(var);                                                       // Añadimos el elemento del tipo Especie con su información al ArrayList
            }
            return res;                                                             // Devolvemos el ArrayList con los elementos
        } catch (SQLException throwables) {
            generateExceptionMessage(throwables);
            return null;
        }
    }

    public ArrayList<Ejemplar> getEjemplares() {

        if (!connect()) return null;                                                // Comprobamos la conexión con la BBDD

        ArrayList<Ejemplar> resultado = new ArrayList<>();                          // Arraylist auxiliar donde guardaremos los elementos de la tabla ejemplar, los elementos serán del tipo Ejemplar
        try (PreparedStatement pst = connection.prepareStatement("SELECT * FROM ejemplar ORDER BY N_Pokedex, N_Encuentro"); //seleccionamos todos ejemplares ordenados por N_Pokedex y N_Encuentro (en caso de empate), ambos ASC (opción por defecto)
             ResultSet rst = pst.executeQuery()) {

            while (rst.next()) {
                Ejemplar var = new Ejemplar();                                      // Variable del tipo Ejemplar
                var.setN_pokedex(rst.getInt("N_Pokedex"));
                var.setN_encuentro(rst.getInt("N_Encuentro"));
                var.setApodo(rst.getString("Apodo"));
                var.setSexo(rst.getString("Sexo").toCharArray()[0]);     // Paso de formato string a formato char requerido
                var.setNivel(rst.getInt("Nivel"));
                var.setInfectado(rst.getInt("Infectado"));
                resultado.add(var);                                                 // Añadimos el elemento del tipo Ejemplar con su información al ArrayList

            }
            return resultado;                                                       // Devolvemos el ArrayList con los elementos
        } catch (SQLException throwables) {

            generateExceptionMessage(throwables);
            return null;

        }
    }

    public int coronapokerus(ArrayList<Ejemplar> ejemplares, int dias) {
        if (!connect()) return 0;                                                   // Comprobamos la conexión con la BBDD
        int i = 0;                                                                  // variable contador auxiliar para manejar los días introducidos en la función
        int res = 0;                                                                // variable auxiliar para devolver el número de pokemon contagiados
        while (i < dias) {
            try (PreparedStatement pst = connection.prepareStatement("SELECT COUNT(*) AS infectados FROM ejemplar WHERE infectado = 1;");
                 ResultSet rst = pst.executeQuery()) {
                while (rst.next()) {
                    connection.setAutoCommit(false);
                    if (rst.getInt("infectados") == 0) {                  // Significa que no hay ningún pokemon contagiado
                        Ejemplar var = Ejemplar.ejemplarRandom(ejemplares);         // Infectamos al primer pokemon, de manera aleatoria
                        var.setInfectado(1);                                        // El valor de infectado será 1 (para diferenciar de un pokemon no contagiado), 0 igual a no infectado, 1 igual a infectado
                        spread(var);                                                // Llamada a función auxiliar donde infectamos al pokemon
                        res = res + 1;                                              // Aumentamos el número de pokemon contagiados
                    } else {
                        for (int j = rst.getInt("infectados"); j > 0; j--) { // Bucle para infectar a los pokemon, j es igual al número de pokemon que hay que contagiar ese día
                            Ejemplar var = Ejemplar.ejemplarRandom(ejemplares);     // Infectamos un pokemon de forma aleatoria
                            if (var.getInfectado() == 1) continue;                  // Si el pokemon ya está contagiado saltamos los procedimientos posteriores y volvemos al bucle
                            var.setInfectado(1);                                    // El valor de infectado será 1 (para diferenciar de un pokemon no contagiado), 0 igual a no infectado, 1 igual a infectado
                            spread(var);                                            // Llamada a función auxiliar donde infectamos al pokemon
                            res = res + 1;                                          // Aumentamos el número de pokemon contagiados
                        }
                    }
                    connection.commit();
                }
            } catch (SQLException throwables) {
                generateExceptionMessage(throwables);
            }
            i++;                                                                    // Aumentamos la variable contador días, un dia más
        }
        return res;                                                                 // Devolvemos el número de pokemon contagiados
    }

    public boolean getSprite(int n_pokedex, String filename) {
        if (!connect()) return false;                                               // Comprobamos la conexión con la BBDD
        ResultSet rst = null;
        FileOutputStream fileOutputStream = null;
        try (PreparedStatement pst = connection.prepareStatement("SELECT sprite FROM especie WHERE n_pokedex = ?;")) {
            pst.setInt(1, n_pokedex);
            rst = pst.executeQuery();                                               //Buscamos en la base de datos el sprite que queremos
            byte[] data = null;                                                     
            Blob blob;
            while (rst.next()) {
                blob = rst.getBlob(1);
                if (blob == null) return false;                                     //Comprobamos que la base de datos no ha devuelto null (Esto significaría que no hay sprite para ese pokemon)
                data = blob.getBytes(1, (int) blob.length());
            }
            assert data != null;
            fileOutputStream = new FileOutputStream(filename);                      //Guardamos el sprite usando fileOutputStream
            fileOutputStream.write(data);

        } catch (SQLException | IOException throwables) {
            generateExceptionMessage(throwables);
            return false;
        } finally {
            try {
                assert rst != null;
                rst.close();                                                        //Cerramos el resultSet y el outputStream
                if (fileOutputStream != null) fileOutputStream.close();
            } catch (SQLException | IOException throwables) {
                generateExceptionMessage(throwables);
            }
        }
        return true;
    }

    // Función auxiliar para la función coronapokerus, esta función realiza la actualización para cuando un pokemon ha sido contagiado
    private void spread(Ejemplar ejemplar) {
        try (PreparedStatement pst1 = connection.prepareStatement("UPDATE ejemplar SET infectado = ? " + //Actualizamos el valor de infectado del pokemon
                "WHERE n_encuentro = ? AND n_pokedex = ?;")) {
            pst1.setInt(1, ejemplar.getInfectado());
            pst1.setInt(2, ejemplar.getN_encuentro());
            pst1.setInt(3, ejemplar.getN_pokedex());
            pst1.executeUpdate();
        } catch (SQLException throwables) {
            generateExceptionMessage(throwables);
        }
    }

    private void generateExceptionMessage(SQLException e) {
        logger.log(Level.WARNING, "Message: " + e.getMessage());
        logger.log(Level.WARNING, "Code: " + e.getErrorCode());
        logger.log(Level.WARNING, "SQL State: " + e.getSQLState());
    }

    private void generateExceptionMessage(Exception throwables) {
        logger.log(Level.WARNING, "Message: " + throwables.getMessage());
    }

}

package pokemon;

import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;


public class Conoce {
	
	private int id_especie;
	private int n_encuentro;
	private int id_ataque;
	
	public Conoce(int id_especie, int n_encuentro, int id_ataque) {
		this.id_especie = id_especie;
		this.n_encuentro = n_encuentro;
		this.id_ataque = id_ataque;
	}

	public int getId_especie() {
		return id_especie;
	}

	public void setId_especie(int id_especie) {
		this.id_especie = id_especie;
	}

	public int getN_encuentro() {
		return n_encuentro;
	}

	public void setN_encuentro(int n_encuentro) {
		this.n_encuentro = n_encuentro;
	}

	public int getId_ataque() {
		return id_ataque;
	}

	public void setId_ataque(int id_ataque) {
		this.id_ataque = id_ataque;
	}
	
	public static ArrayList<Conoce> readData(String fileName) {
		File f = new File(fileName);
		ArrayList<Conoce> result =  new ArrayList<Conoce>();
		
		try {
			Scanner sc_file = new Scanner(f);
			
			while(sc_file.hasNextLine()) {
				String[] fields = sc_file.nextLine().split(";");
				Conoce row = new Conoce(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), Integer.valueOf(fields[2]));
				result.add(row);
			}
			
			sc_file.close();
		} catch (Exception e) {
			System.err.println("Error al leer el fichero.");
		}
		
		return result;
	}

}

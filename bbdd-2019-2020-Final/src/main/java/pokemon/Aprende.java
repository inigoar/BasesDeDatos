package pokemon;

import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;


public class Aprende {
	
	private int id_especie;
	private int id_ataque;
	private int nivel;
	
	public Aprende(int id_especie, int id_ataque, int nivel) {
		this.id_especie = id_especie;
		this.id_ataque = id_ataque;
		this.nivel = nivel;
	}

	public int getId_especie() {
		return id_especie;
	}

	public void setId_especie(int id_especie) {
		this.id_especie = id_especie;
	}

	public int getId_ataque() {
		return id_ataque;
	}

	public void setId_ataque(int id_ataque) {
		this.id_ataque = id_ataque;
	}

	public int getNivel() {
		return nivel;
	}

	public void setNivel(int nivel) {
		this.nivel = nivel;
	}
	
	public static ArrayList<Aprende> readData(String fileName) {
		File f = new File(fileName);
		ArrayList<Aprende> result =  new ArrayList<Aprende>();
		
		try {
			Scanner sc_file = new Scanner(f);
			
			while(sc_file.hasNextLine()) {
				String[] fields = sc_file.nextLine().split(";");
				Aprende row = new Aprende(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), Integer.valueOf(fields[2]));
				result.add(row);
			}
			
			sc_file.close();
		} catch (Exception e) {
			System.err.println("Error al leer el fichero.");
		}
		
		return result;
	}

}

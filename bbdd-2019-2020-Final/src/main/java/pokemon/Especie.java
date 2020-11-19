package pokemon;

import java.util.ArrayList;


public class Especie {

	private int n_pokedex;
	private String nombre;
	private String descripcion;
	private int evoluciona;

	public Especie() {

	}

	public Especie(int n_pokedex, String nombre, String descripcion, int evoluciona) {
		this.n_pokedex = n_pokedex;
		this.nombre = nombre;
		this.descripcion = descripcion;
		this.evoluciona = evoluciona;
	}

	public int getN_pokedex() {
		return n_pokedex;
	}

	public void setN_pokedex(int n_pokedex) {
		this.n_pokedex = n_pokedex;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getDescripcion() {
		return descripcion;
	}

	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}

	public int getEvoluciona() {
		return evoluciona;
	}

	public void setEvoluciona(int evoluciona) {
		this.evoluciona = evoluciona;
	}
	
	public static String toStringArrayList(ArrayList<Especie> especies) {
		String result = "Número - Nombre - Descripción - Evoluciona de\n";
		
		for (Especie e: especies) {
			result += e.getN_pokedex() + " - ";
			result += e.getNombre() + " - ";
			result += e.getDescripcion() + " - ";
			result += e.getEvoluciona() + "\n";
		}
		
		return result;
	}

}

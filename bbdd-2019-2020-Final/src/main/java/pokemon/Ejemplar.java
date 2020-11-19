package pokemon;

import java.util.ArrayList;
import java.util.Random;


public class Ejemplar {
	
	private int n_pokedex;
	private int n_encuentro;
	private String apodo;
	private char sexo;
	private int nivel;
	private int infectado;
	private static Random generator;
	
	public Ejemplar() {
		generator = new Random(1);
	}
	
	public Ejemplar(int n_pokedex, int n_encuentro, String apodo, char sexo, int nivel, int infectado) {
		this.n_pokedex = n_pokedex;
		this.n_encuentro = n_encuentro;
		this.apodo = apodo;
		this.sexo = sexo;
		this.nivel = nivel;
		this.infectado = infectado;
		generator = new Random(1);
	}

	public int getN_pokedex() {
		return n_pokedex;
	}

	public void setN_pokedex(int n_pokedex) {
		this.n_pokedex = n_pokedex;
	}

	public int getN_encuentro() {
		return n_encuentro;
	}

	public void setN_encuentro(int n_encuentro) {
		this.n_encuentro = n_encuentro;
	}

	public String getApodo() {
		return apodo;
	}

	public void setApodo(String apodo) {
		this.apodo = apodo;
	}

	public char getSexo() {
		return sexo;
	}

	public void setSexo(char sexo) {
		this.sexo = sexo;
	}

	public int getNivel() {
		return nivel;
	}

	public void setNivel(int nivel) {
		this.nivel = nivel;
	}

	public int getInfectado() {
		return infectado;
	}

	public void setInfectado(int infectado) {
		this.infectado = infectado;
	}
	
	public static Ejemplar ejemplarRandom(ArrayList<Ejemplar> l) {
		return l.get((int)(generator.nextDouble() * l.size()));
	}

}

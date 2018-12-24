package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.hsqldb.Server;

public class InitHSQL {

	static final String URL = "jdbc:hsqldb:hsql://localhost/bd_veiculos";
	static final String DB_PATH = "hsql/bd_veiculos";
	static final String DB_NAME = "bd_veiculos";
	static final String TABELA_VEICULO = "veiculo";
	static final String TABELA_MARCA = "marca";
	static final String TABELA_PAISES = "apps_countries";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		setUpDatabase();
	}

	private static void setUpDatabase() {

		Server server = new Server();
		server.setDatabasePath(0, DB_PATH);
		server.setDatabaseName(0, DB_NAME);
		server.start();
		configurarBancoDeDados();
		System.out.println("Digite CTRL+C para encerrar...");
		aguardandoEncerramento();
		server.shutdown();
		System.exit(0);
	}

	private static void configurarBancoDeDados() {
		// TODO Auto-generated method stub
		try (Connection conn = DriverManager.getConnection(URL)) {

			DatabaseMetaData meta = conn.getMetaData();
			String[] tipo = new String[] { "TABLE" };
			ResultSet rs = meta.getTables(null, null, "%", tipo);

			if (rs.next()) {
				System.out.println("----------- Servidor HSQL rodando: " + DB_NAME);
			} else {
				System.out.printf("---------- criando tabelas %s e %s...", TABELA_MARCA, TABELA_VEICULO, TABELA_PAISES);
				criarTabelas(conn);
				System.out.printf("---------- inserindo registros... ");
				preencherTabelas(conn);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void preencherTabelas(Connection conn) throws SQLException, IOException {
		// http://www.autosaur.com/car-brands-complete-list/

		inserirFabricante(1, "Alfa Romeo", getDataAleatoria(), "Italia", "99999999999999", conn);
		inserirFabricante(2, "Audi", getDataAleatoria(), "Alemanha", "99999999999999", conn);
		inserirFabricante(3, "BMW", getDataAleatoria(), "Alemanha", "99999999999999", conn);
		inserirFabricante(4, "Chery", getDataAleatoria(), "China", "99999999999999", conn);
		inserirFabricante(5, "Chevrolet", getDataAleatoria(), "Estados Unidos", "99999999999999", conn);
		inserirFabricante(6, "Citroen", getDataAleatoria(), "França", "99999999999999", conn);
		inserirFabricante(7, "Ford", getDataAleatoria(), "Estados Unidos", "99999999999999", conn);
		inserirFabricante(8, "Honda", getDataAleatoria(), "Japão", "99999999999999", conn);
		inserirFabricante(9, "Hyunday", getDataAleatoria(), "Coréia do Sul","99999999999999", conn);
		inserirFabricante(10, "Jeep", getDataAleatoria(), "Estados Unidos", "99999999999999", conn);
		inserirFabricante(11, "Volvo", getDataAleatoria(), "Suécia", "99999999999999", conn);
		inserirFabricante(12, "Subaru", getDataAleatoria(), "Coreia do Sul", "99999999999999", conn);

		insertVeiculo(1, "Stelvio", 2018, 42_195.0, "SUV", "azul", 1, conn);
		insertVeiculo(2, "4C Spider", 2017, 55_900.0, "Esportivo", "preto", 1, conn);
		insertVeiculo(3, "A3", 2016, 31_950.0, "Sedan", "branco", 2, conn);
		insertVeiculo(4, "A6", 2017, 44_500.0, "Sedan", "branco", 2, conn);
		insertVeiculo(5, "RS7", 2015, 113_900.0, "Esportivo", "azul", 2, conn);
		insertVeiculo(6, "X4", 2010, 51_350.0, "SUV", "azul", 3, conn);
		insertVeiculo(7, "i8", 2015, 60_100.0, "Esportivo", "preto", 3, conn);
		insertVeiculo(8, "Tigo 7", 2011, 20_000.5, "SUV", "grafite", 4, conn);
		insertVeiculo(9, "Cruze", 2016, 22_500.0, "Sedan", "branco", 5, conn);
		insertVeiculo(10, "Malibu", 2015, 26_000.5, "Sedan", "vermelho", 5, conn);
		insertVeiculo(11, "C3 JCC", 2017, 15_500.0, "Compacto", "branco", 6, conn);
		insertVeiculo(12, "Mustang", 2018, 26_100.4, "Esportivo", "vermelho", 7, conn);
		insertVeiculo(13, "Taurus", 2017, 28_000.5, "Sedan", "preto", 7, conn);
		insertVeiculo(14, "Fusion", 2018, 22_800.0, "Sedan", "azul", 7, conn);
		insertVeiculo(15, "Civic", 2016, 17_400.0, "Sedan", "branco", 8, conn);
		insertVeiculo(16, "Accord", 2018, 19_400.0, "Sedan", "vermelho", 8, conn);
		insertVeiculo(17, "Outback", 2014, 25_100.0, "SUV", "preto", 12, conn);
		insertVeiculo(18, "V90 Cross Country", 2018, 41_000.0, "Cross Over", "branco", 11, conn);
		insertVeiculo(19, "All-New S60", 2011, 35_800.0, "Sedan", "grafite", 11, conn);
		insertVeiculo(20, "V60", 2017, 42_100.0, "Cross Over", "amarelo", 11, conn);

		try (Stream<String> linhas = Files.lines(Paths.get("src/inserts_pais.sql"))) {
			linhas.forEach(sql -> inserirPais(sql, conn));
		}

	}

	private static String getDataAleatoria() {
		// TODO Auto-generated method stub
		int yyyy = ThreadLocalRandom.current().nextInt(1900, 2000);
		int MM = ThreadLocalRandom.current().nextInt(1, 12);
		int dd = ThreadLocalRandom.current().nextInt(1, 28);
		LocalDate ld = LocalDate.of(yyyy, MM, dd);
		DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return f.format(ld);
	}

	private static void insertVeiculo(int id, String modelo, int ano, double valor, String tipo, String cor,
			int idFabricante, Connection conn) throws SQLException {

		PreparedStatement st = conn.prepareStatement("INSERT INTO " + TABELA_VEICULO + " VALUES (?,?,?,?,?,?,?)");
		st.setInt(1, id);
		st.setString(2, modelo);
		st.setInt(3, ano);
		st.setDouble(4, valor);
		st.setString(5, tipo);
		st.setString(6, cor);
		st.setInt(7, idFabricante);
		st.executeUpdate();
		st.close();
	}

	private static void inserirFabricante(int id, String nome, String fundacao, String sede, String cnpj, Connection conn)
			throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement st = conn.prepareStatement("INSERT INTO " + TABELA_MARCA + " VALUES (?,?,?,?,?)");
		st.setInt(1, id);
		st.setString(2, nome);
		st.setString(3, fundacao);
		st.setString(4, sede);
		st.setString(5, cnpj);
		st.executeUpdate();
		st.close();
	}

	private static void inserirPais(String sql, Connection conn) {
		// TODO Auto-generated method stub
		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.executeUpdate();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void criarTabelas(Connection conn) throws SQLException {

		String createTable1 = "CREATE TABLE " + TABELA_MARCA + " (" + " ID NUMERIC PRIMARY KEY," + " NOME VARCHAR(255),"
				+ " FUNDACAO DATE," + " SEDE VARCHAR(255), CNPJ VARCHAR(255));";

		String createTable2 = "CREATE TABLE " + TABELA_VEICULO + " (" + " ID NUMERIC PRIMARY KEY,"
				+ " MODELO VARCHAR(255)," + " ANO INT," + " VALOR DECIMAL(10,2)," + " TIPO VARCHAR (255),"
				+ " COR VARCHAR (255)," + " FAB_ID NUMERIC," + " FOREIGN KEY (FAB_ID) REFERENCES " + TABELA_MARCA
				+ "(ID));";

		String createTable3 = "CREATE TABLE " + TABELA_PAISES + "(" + " id numeric IDENTITY PRIMARY KEY,"
				+ " country_code varchar(2)," + " country_name varchar(100));";

		conn.createStatement().executeUpdate(createTable1);
		conn.createStatement().executeUpdate(createTable2);
		conn.createStatement().executeUpdate(createTable3);
		System.out.println("============ tabelas criadas");
	}

	private static void aguardandoEncerramento() {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			String userInput = reader.readLine();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

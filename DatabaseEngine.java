import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Arrays;


/**
 *  @author Jiafeng Liao
 *  @version 1.0
 */
public class DatabaseEngine {

	/* This can be changed to whatever you like */
	static String prompt = "\n>>> ";
	static String version = "v1.0";
	static String copyright = "©2016 Chris Irwin Davis \nUniversity of Texas at Dallas";
	static boolean isExit = false;
	/*
	 * Page size for alll files is 512 bytes by default.
	 */
	static long pageSize = 512; 

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {
    	
    	init();
		splashScreen();
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			
			userCommand = scanner.next().replace("\n", " ").replace("\r", " ").trim().toLowerCase();
			userCommand = userCommand.replace("\'","");
			parseUserCommand(userCommand);
		}
		System.out.println("\nExiting...");
	}

	/** ***********************************************************************
	 *  Method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("+",100));
        System.out.println("Welcome to My Database Engine!"); 
		System.out.println("My Database Engine Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands and detail instructions.");
		System.out.println(line("+",100));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	

	public static void help() {
		System.out.println(line("+",100));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		// bonus
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| INIT;                                            Reset the whole database.               |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| SHOW DATABASES;                                  Display a list of all databases.        |");
		System.out.println("\t+"+line("-",90)+"+");	
		System.out.println("\t| CREATE DATABASE database_name;                   Create a new blank database.            |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| USE database_name;                               Use database, default used when created.|");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| DROP DATABASE database_name;                     Remove database and directory.          |");
		// DDL
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| SHOW TABLES;                                     Display a list of all tables.           |");
		System.out.println("\t+"+line("-",90)+"+");	
		System.out.println("\t| CREATE TABLE table_name;                         Create a new table schema.              |");
		System.out.println("\t|      EXAMPLE: CREATE TABLE table_name (                                                  |");
		System.out.println("\t|                      row_id  INT  PRIMARY KEY,                                           |");
		System.out.println("\t|                      column1 TEXT NOT NULL,                                              |");
		System.out.println("\t|                      column2 DATE                                                        |");
		System.out.println("\t|                      );                                                                  |");	
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| DROP TABLE table_name;                           Remove table data and its schema.       |");

		// DML
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| INSERT INTO table_name (list) VALUES (list);     Insert a single record into a table.    |");
		System.out.println("\t|      EXAMPLE: INSERT INTO table_name (col1,col2,col3) VALUES (val1,val2,val3);           |");
		System.out.println("\t|      EXAMPLE: INSERT INTO table_name VALUES (val1,val2,val3);                            |");
		System.out.println("\t|      NOTE: if no col list provided, data will map to first n columns.                    |");
		System.out.println("\t"+"+"+line("-",90)+"+");
		System.out.println("\t| DELETE FROM table_name WHERE column = value;     Delete records from a table.            |");
		System.out.println("\t|      NOTE: all records in table match the condition will be deleted.                     |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| UPDATE table_name SET col=val WHERE condition;   Modify one or more records in a table.  |");
		System.out.println("\t|      EXAMPLE: UPDATE table_name SET col1=val1,col2=val2 WHERE col3=val3;                 |");
		System.out.println("\t|      NOTE: all records in table match the condition will be updated.                     |");
		System.out.println("\t+"+line("-",90)+"+");
		// VDL
		System.out.println("\t| SELECT * FROM table_name;                        Display all records in the table.       |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| SELECT columns FROM table_name WHERE condition;  Display selected columns of records.    |");
		System.out.println("\t|      EXAMPLE: SELECT col1,col2,col3 FROM table_name WHERE col4=val4;                     |");
		System.out.println("\t|      NOTE: columns will show in an order col1,col2,col3.                                 |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| VERSION;                                         Show the program version.               |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| HELP;                                            Show this help information.             |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println("\t| EXIT;                                            Exit the program                        |");
		System.out.println("\t+"+line("-",90)+"+");
		System.out.println();
		System.out.println(line("+",100));
	}
	
	// check catalog and init catalog
	public static void init() {
		try{
			File dataDir = new File("data");
			File catalog = new File(dataDir,"catalog");
			dataDir.mkdir();
			catalog.mkdir();
			String[] existCatalog = catalog.list();	
			boolean checkTables = false, checkColumns = false, checkDatabases = false;
			for(int i=0;i<existCatalog.length;i++){
				if(existCatalog[i].equals("tables.tbl"))	checkTables = true;
				if(existCatalog[i].equals("columns.tbl"))	checkColumns = true;
				if(existCatalog[i].equals("databases.tbl"))	checkDatabases = true;
			}
			if(checkTables && checkColumns && checkDatabases){
				return;
			}else{
				System.out.println();
				System.out.println("\nMissing database catalog, initializing database...... ");
				Database.initDatabase();
			}	
		}
		catch(SecurityException e){
			System.out.println("\nError: cannot initialize the database engine.");
			System.out.println(e);
		}	
	}
	

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("My Database Engine Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		try{
			switch (commandTokens.get(0)) {
				case "init":
					Database.initDatabase();
					break;
				case "show":
					if(commandTokens.get(1).equals("databases"))	Database.showDatabases();
					else if(commandTokens.get(1).equals("tables"))	Table.showTables();
					else System.out.println("\nError: please verify your input.");
					break;
				case "use":
					parseUseDatabase(userCommand);
					break;
				case "create":
					if(commandTokens.get(1).equals("database"))		parseCreateDatabase(userCommand);
					else if(commandTokens.get(1).equals("table"))	parseCreateTable(userCommand);
					else System.out.println("\nError: please verify your input.");
					break;
				case "drop":
					if(commandTokens.get(1).equals("database"))		dropDatabase(userCommand);
					else if(commandTokens.get(1).equals("table"))	dropTable(userCommand);
					else	System.out.println("\nError: please verify your input.");
					break;
				case "select":
					parseQueryString(userCommand);
					break;
				case "insert":
					parseInsertRow(userCommand);
					break;	
				case "delete":
					parseDeleteRow(userCommand);
					break;
				case "update":
					parseUpdateRow(userCommand);
					break;
				case "help":
					help();
					break;
				case "version":
					displayVersion();
					break;
				case "exit":
					isExit = true;
					break;
				case "quit":
					isExit = true;
				default:
					System.out.println("\nI didn't understand the command: \"" + userCommand + "\"");
					break;
			}
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}
	


	public static void dropTable(String userCommand) {
		try{
			String tableName = userCommand.split(" ")[2].trim();
			if(!Database.checkTable(tableName)){
				System.out.println("\nTable ["+tableName+"] does not exist.");
			}else{
				Table.dropTable(tableName);
			}
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}
	
	public static void dropDatabase(String userCommand){
		try{
			String databaseName = userCommand.split(" ")[2];
			if(!Database.checkDatabase(databaseName)){
				System.out.println("\nError: table ["+databaseName+"] does not exist.");
			}else{
				Database.dropDatabase(databaseName);
			}
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 */
	public static void parseQueryString(String queryString) {
		String tableName;
		String[] columns;
		String[] condition = new String[]{"","",""};
		try{
			String temp = queryString.split("select")[1].trim();
			String columnsTemp = temp.split("from")[0].trim();
			if(columnsTemp.equals("*")){
				columns = new String[]{"*"};
			}else{
				int count = columnsTemp.split(",").length;
				columns = new String[count];
				for(int i=0;i<count;i++){
					columns[i] = columnsTemp.split(",")[i].trim();
				}
			}
			
			temp = queryString.split("from")[1].trim();
			if(temp.split(" ").length == 1){
				tableName = temp;
			}else{
				tableName = temp.split(" ")[0].trim();
				String conditionTemp = queryString.split("where")[1].trim();
				condition = getCondition(conditionTemp);
				if(condition[1].equals("")){
					System.out.println("\nError: please verify your input.");
					return;
				}	
			}
			if(!Database.checkTable(tableName)){
				System.out.println("\nError: table ["+tableName+"] does not exist.");
			}else{
				Table.query(tableName,columns,condition);
			}	
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}
	
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 */
	public static void parseCreateTable(String createTableString) {
		try{
			String[] createTableTokens = createTableString.split(" ");
			String tableName = createTableTokens[2];
			
			String temp1 = createTableString.split("\\(")[1];
			String temp2 = temp1.split("\\)")[0].trim();
			String columns[] = temp2.split(",");
			for(int i=0;i<columns.length;i++){
				columns[i] = columns[i].trim();
			}
			if(Database.checkTable(tableName)){
				System.out.println("\nTable [" + tableName + "] already exists.");
				return;
			}
			Table.createTable(tableName,columns);
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
			e.printStackTrace();
		}
		return;
	}
	
	public static void parseCreateDatabase(String userCommand){
		try{
			String databaseName = userCommand.split(" ")[2].trim();
			if(Database.checkDatabase(databaseName)){
				System.out.println("\nDatabase [" + databaseName + "] already exists.");
				return;
			}
			Database.createDatabase(databaseName);
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}

	public static void parseUseDatabase(String userCommand){
		try{
			String databaseName = userCommand.split(" ")[1].trim();
			if(!Database.checkDatabase(databaseName)){
				System.out.println("\nDatabase ["+databaseName+"] does not exist.");
				return;
			}
			String onUse = Database.getDatabaseName();
			Database.notUse(onUse);
			Database.use(databaseName);
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}
	
	public static void parseInsertRow(String userCommand){
		String tableName;
		String[] columns;
		String[] values;
		String databaseName = Database.getDatabaseName();
		try{
			tableName = userCommand.split(" ")[2];
			if(!Database.checkTable(tableName)){
				System.out.println("\nTable "+tableName+" does not exist.");
				return;
			}
			String temp1 = userCommand.split("values")[1].trim();
			String[] valueTemp = temp1.substring(1, temp1.length()-1).split(",");
			values = new String[valueTemp.length];
			for(int i=0;i<valueTemp.length;i++){
				values[i] = valueTemp[i].trim();
			}
			if(userCommand.split(" ")[3].equals("values")){
				columns = new  String[values.length];
				String[] columnNames = Table.getColumnNames(tableName);
				for(int i=0;i<columns.length;i++)	columns[i] = columnNames[i];
			}else{
				String temp2 = userCommand.split(tableName)[1].trim();
				String temp3 = temp2.split("values")[0].trim();
				String[] columnTemp = temp3.substring(1,temp3.length()-1).split(",");
				columns = new String[columnTemp.length];
				for(int i=0;i<columnTemp.length;i++){
					columns[i] = columnTemp[i].trim();
				}
				if(columns.length != values.length){
					System.out.println("\nError: please verify your input.");
					return;
				}
			}
			RandomAccessFile file = new RandomAccessFile("data/"+databaseName+"/"+tableName+".tbl","rw");
			Table.insertInto(file, tableName, columns, values);
			file.close();
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
			e.printStackTrace();
		}
	}
	
	public static void parseUpdateRow(String userCommand){
		String tableName;
		String[] columnNames;
		String[] values;
		String[] condition;
		try{
			tableName = userCommand.split(" ")[1].trim();
			String conditionTemp = userCommand.split("where")[1].trim();
			String temp1 = userCommand.split("set")[1].trim();
			String temp2 = temp1.split("where")[0].trim();
			String[] update = temp2.split(",");
			columnNames = new String[update.length];
			values = new String[update.length];
			for(int i=0;i<update.length;i++){
				String clause = update[i].trim();
				String[] pair = getCondition(clause);
				if(!pair[1].equals("=")){
					System.out.println("\nError: please verify your input.");
					return;
				}
				columnNames[i] = pair[0];
				values[i] = pair[2];	
			}
			condition = getCondition(conditionTemp);
			if(!Database.checkTable(tableName)){
				System.out.println("\nTable ["+tableName+"] does not exist.");
			}else if(condition[1].equals("")){
				System.out.println("\nError: please verify the condition.");
			}else{
				Table.updateRow(tableName,columnNames,values,condition);
			}
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}
	
	public static void parseDeleteRow(String userCommand){
		try{
			String temp = userCommand.split("where")[0].trim();
			String[] temp2 = temp.split(" "); 
			String tableName = temp2[temp2.length-1];
			String conditionTemp = userCommand.split("where")[1].trim();
			String condition[] = getCondition(conditionTemp);
			if(!Database.checkTable(tableName)){
				System.out.println("\nTable ["+tableName+"] does not exist.");
			}else if(condition[1].equals("")){
				System.out.println("\nError: please verify the condition.");
			}else{
				Table.deleteRow(tableName,condition);
			}
		}
		catch(Exception e){
			System.out.println("\nError: please verify your input.");
		}
	}

	public static String[] getCondition(String string){
		String[] condition = {"","",""};
		if(string.contains("=")){
			condition[0] = string.split("=")[0].trim();
			condition[1] = "=";
			condition[2] = string.split("=")[1].trim();
		}
		if(string.contains("<")){
			condition[0] = string.split("<")[0].trim();
			condition[1] = "<";
			condition[2] = string.split("<")[1].trim();
		}
		if(string.contains("<=")){
			condition[0] = string.split("<=")[0].trim();
			condition[1] = "<=";
			condition[2] = string.split("<=")[1].trim();
		}
		if(string.contains(">")){
			condition[0] = string.split(">")[0].trim();
			condition[1] = ">";
			condition[2] = string.split(">")[1].trim();
		}
		if(string.contains(">=")){
			condition[0] = string.split(">=")[0].trim();
			condition[1] = ">=";
			condition[2] = string.split(">=")[1].trim();
		}
		if(string.contains("<>")){
			condition[0] = string.split("<>")[0].trim();
			condition[1] = "<>";
			condition[2] = string.split("<>")[1].trim();
		}
		return condition;
	}
}
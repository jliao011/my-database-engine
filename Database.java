import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;

public class Database {
	static int pageSize = 512;
	private static RandomAccessFile tablesCatalog;
	private static RandomAccessFile columnsCatalog;
	private static RandomAccessFile databasesCatalog;
	
	public static void main(String[] args){

	}
	
	public static void initDatabase(){
		try{
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] existDatabases= dataDir.list((dir, name) -> !name.equals(".DS_Store"));
			String[] existMetas = new String[0];
			for(int i=0;i<existDatabases.length;i++){
				File existDatabase= new File(dataDir,existDatabases[i]);
				existMetas= existDatabase.list((dir, name) -> !name.equals(".DS_Store"));
				if(existMetas != null){
					for(int j=0;j<existMetas.length;j++){
						File existMeta= new File(existDatabase,existMetas[j]);
						existMeta.delete();
					}
				}
				existDatabase.delete();
				
			}
			File catalog = new File(dataDir,"catalog");
			catalog.mkdir();
		}
		catch(SecurityException e){
			System.out.println("\nError: cannot create the directory.");
			e.printStackTrace();
		}
		// create tables
		try {
			tablesCatalog = new RandomAccessFile("data/catalog/tables.tbl","rw");
			Page.createLeafPage(tablesCatalog);
			
			String[] data = {"1","tables"};
			String[] dataType = {"int","text"};
			short payload = Page.getPayload(data, dataType);
			short size = (short)(payload + 6);
			byte[] stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(tablesCatalog, 1, size, stc, data);
			
			data = new String[]{"2","columns"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(tablesCatalog, 1, size, stc, data);		
		} catch (Exception e) {
			System.out.println("\nError: cannot create the tables catalog.");
			e.printStackTrace();
		}
		// create columns
		try {
			columnsCatalog = new RandomAccessFile("data/catalog/columns.tbl","rw");
			Page.createLeafPage(columnsCatalog);

			String[] data = new String[]{"1","tables","rowid","int","1","no"};
			String[] dataType = new String[]{"int","text","text","text","tinyint","text"};
			short payload = Page.getPayload(data, dataType);
			short size = (short)(payload + 6);
			byte[] stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"2","tables","table_name","text","2","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"3","columns","rowid","int","1","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"4","columns","table_name","text","2","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"5","columns","column_name","text","3","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"6","columns","data_type","text","4","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"7","columns","ordinal_position","tinyint","5","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"8","columns","is_nullable","text","6","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
	
		} catch (Exception e) {
			System.out.println("\nError: cannot create the columns catalog.");
			e.printStackTrace();
		}	
		// create database.tbl
		try{
			databasesCatalog = new RandomAccessFile("data/catalog/databases.tbl","rw");
			Page.createLeafPage(databasesCatalog);
			// update tables
			String[] data = new String[]{"3","databases"};
			String[] dataType = new String[]{"int","text"};
			short payload = Page.getPayload(data, dataType);
			short size = (short)(payload + 6);
			byte[] stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(tablesCatalog, 1, size, stc, data);
			// update columns
			data = new String[]{"9","databases","rowid","int","1","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
	
			data = new String[]{"10","databases","on_use","tinyint","2","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"11","databases","database_name","text","3","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			// update databases
			data = new String[]{"1","1","catalog"};
			dataType = new String[]{"int","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = Table.stcArrayEncode(data,dataType);
			Page.insertLeafCell(databasesCatalog, 1, size, stc, data);
			
		}
		catch(Exception e){
			System.out.println("\nError: cannot create the database catalog.");
			e.printStackTrace();
		}
	
	}


	public static void notUse(String databaseName){
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/databases.tbl","rw");
			short page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int countCells = Page.countCells(file, page);
				for(int j=0;j<countCells;j++ ){
					String[] value = Page.fetchCell(file,page,j);
					String name = value[2];
					if(name.equals(databaseName)){
						short offset = Page.getCellOffset(file, page, j);
						file.seek((page-1)*pageSize + offset + 14);
						file.writeByte(0);
						file.close();
						return;
					}
				}
				page = Page.getRightPagePointer(file, page);
			}
			file.close();
		}
		catch(Exception e){
			System.out.println("\nError: not use database.");
		}
	}

	public static void use(String databaseName){
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/databases.tbl","rw");
			short page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int countCells = Page.countCells(file, page);
				for(int j=0;j<countCells;j++ ){
					String[] value = Page.fetchCell(file,page,j);
					String name = value[2];
					if(name.equals(databaseName)){
						short offset = Page.getCellOffset(file, page, j);
						file.seek((page-1)*pageSize + offset + 14);
						file.writeByte(1);
						file.close();
						return;
					}
				}
				page = Page.getRightPagePointer(file, page);
			}
			file.close();
		}
		catch(Exception e){
			System.out.println("\nError: use database");
		}
	}
	
	public static String getDatabaseName(){
		String databaseName = "";
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/databases.tbl","rw");
			short page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int countCells = Page.countCells(file, page);
				for(int j=0;j<countCells;j++ ){
					String[] value = Page.fetchCell(file,page,j);
					int onUse = Integer.parseInt(value[1]);
					if(onUse == 1) databaseName = value[2];
				}
				page = Page.getRightPagePointer(file, page);
			}
			file.close();
		}
		catch(Exception e){
			System.out.println("\nError: cannot get current database name.");
		}
		return databaseName;

	}

	public static boolean checkDatabase(String databaseName){
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/databases.tbl","rw");
			short page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int countCells = Page.countCells(file, page);
				for(int j=0;j<countCells;j++ ){
					String[] value = Page.fetchCell(file,page,j);
					if(value[2].equals(databaseName)){
						file.close();
						return true;
					}
				}
				page = Page.getRightPagePointer(file, page);
			}
			file.close();
		}
		catch(Exception e){
			System.out.println("\nError: cannot get current database id.");
		}
		return false;
	}
	
	public static boolean checkTable(String table){
		String databaseName = getDatabaseName();
		try{
			RandomAccessFile file = new RandomAccessFile("data/"+databaseName+"/tables.tbl","rw");
			short page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int countCells = Page.countCells(file, page);
				for(int j=0;j<countCells;j++ ){
					String[] value = Page.fetchCell(file,page,j);
					if(value[1].equals(table)){
						file.close();
						return true;
					}
				}
				page = Page.getRightPagePointer(file, page);
			}
			file.close();
		}
		catch(Exception e){
			System.out.println("\nError when check table ["+table+"] in database["+databaseName+"].");
		}
		return false;	
	}
	
	public static void createDatabase(String databaseName){
		try{
			String onUse = getDatabaseName();
			notUse(onUse);
			use("catalog");
			
			RandomAccessFile databaseMeta = new RandomAccessFile("data/catalog/databases.tbl","rw");
			String[] condition = new String[]{"","",""};
			String[] columnNames = Table.getColumnNames("databases");
			HashMap<Integer, String[]> buffer = Table.fetchCells(databaseMeta,condition,columnNames);
			ArrayList<Integer> keySet = new ArrayList<Integer>(buffer.keySet());
			int databaseID = keySet.get(keySet.size()-1)+1;
			String[] databaseData = {Integer.toString(databaseID),"0",databaseName};
			columnNames = new String[]{"rowid","on_use","database_name"};
			Table.insertInto(databaseMeta,"databases",columnNames,databaseData);
			notUse("catalog");
			use(databaseName);
			Table.initMeta();
			
			File database = new File("data/"+databaseName);
			database.mkdir();
			initDatabase(databaseName);
			databaseMeta.close();
			
			System.out.println("\nDatabase ["+databaseName+"] created. \nUSE "+databaseName+";");
		}
		catch(Exception e){
			System.out.println("\nError: cannot create database "+databaseName+".");
		}
	}
	
	public static void dropDatabase(String databaseName){
		try{
			if(databaseName.equals("catalog")){
				System.out.println("\nError: you cannot drop the catalog.");
				return;
			}
			String onUse = getDatabaseName();
			notUse(onUse);
			use("catalog");	
			
			// modify tables.tbl
			String[] condition = new String[]{"database_name","=",databaseName};
			Table.deleteRow("databases",condition);
			
			File dataDir = new File("data");
			String[] existDatabases= dataDir.list((dir, name) -> !name.equals(".DS_Store"));
			String[] existMetas = new String[0];
			for(int i=0;i<existDatabases.length;i++){
				if(existDatabases[i].equals(databaseName)){
					File existDatabase= new File(dataDir,existDatabases[i]);	
					existMetas= existDatabase.list((dir, name) -> !name.equals(".DS_Store"));
					for(int j=0;j<existMetas.length;j++){
						File existMeta= new File(existDatabase,existMetas[j]);
						existMeta.delete();
					}
					existDatabase.delete();
					System.out.println("\nDatabase ["+databaseName+"] is dropped.");
					return;
				}
			}	
			
		}
		catch(Exception e){
			System.out.println("\nError: cannot drop database["+databaseName+"].");
		}		
	}

	public static void initDatabase(String databaseName){
		try{
			File database = new File("data/"+databaseName);
			database.mkdir();
			RandomAccessFile tables = new RandomAccessFile("data/"+databaseName+"/tables.tbl","rw");
			Page.createLeafPage(tables);
			tables.close();
			RandomAccessFile columns = new RandomAccessFile("data/"+databaseName+"/columns.tbl","rw");
			Page.createLeafPage(columns);
			columns.close();
		}
		catch(Exception e){
			System.out.println("\nError: cannot initial database["+databaseName+"].");
		}
	}
	
	public static void showDatabases(){
		try{	
			String onUse = getDatabaseName();
			notUse(onUse);
			use("catalog");	
			String[] columnNames = new String[]{"database_name"};	
			String[] condition = new String[]{"","",""};
			Table.query("databases",columnNames,condition);
			notUse("catalog");
			use(onUse);
			
		}
		catch(Exception e){
			System.out.println("\nError: cannot show databases.");
		}
	}

	public static int countPage(RandomAccessFile file){
		int count = 0;
		try {
			count = (int)file.length()/pageSize;
		} catch (Exception e) {
			System.out.println("\nError at countPage.");
			e.printStackTrace();
		}
		return count;
	}

}

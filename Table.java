import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Table {

	static int pageSize = 512;
	private static RandomAccessFile tablesCatalog;
	private static RandomAccessFile columnsCatalog;
	
	

	public static void main(String[] args){
	}
	
	public static void showTables(){
		try {
			String[] columnNames = new String[]{"table_name"};
			String[] condition = new String[]{"","",""};
			query("tables",columnNames,condition);
		} catch (Exception e) {
			System.out.println("\nError: cannot show tables.");
			e.printStackTrace();
		}
	}

	public static String[] getColumnNames(String tableName){
		String[] columnNames = {"rowid","table_name","column_name","data_type","ordinal_position","is_nullable"};
		try{
			String databaseName = Database.getDatabaseName();
			RandomAccessFile columnMeta = new RandomAccessFile("data/"+databaseName+"/columns.tbl", "rw");
			String[] condition = {"table_name","=",tableName};
			HashMap<Integer, String[]> buffer = fetchCells(columnMeta,condition,columnNames);	
			columnNames = new String[buffer.size()];
			ArrayList<Integer> temp = new ArrayList<Integer>(buffer.keySet());
			for(int i=0;i<temp.size();i++){
				columnNames[i] = buffer.get(temp.get(i))[2];
			}
			columnMeta.close();
		}
		catch(Exception e){
			System.out.println("\nError: cannot get column names from table ["+tableName+"].");
			e.printStackTrace();
		}
		return columnNames;
	}
	
	public static String[] getDataType(String tableName){
		String[] columnNames = {"rowid","table_name","column_name","data_type","ordinal_position","is_nullable"};
		String[] dataType = new String[0];
		try{
			String databaseName = Database.getDatabaseName();
			RandomAccessFile columnMeta = new RandomAccessFile("data/"+databaseName+"/columns.tbl", "rw");
			String[] condition = {"table_name","=",tableName};
			HashMap<Integer, String[]> buffer = fetchCells(columnMeta,condition,columnNames);	
			dataType = new String[buffer.size()];
			ArrayList<Integer> temp = new ArrayList<Integer>(buffer.keySet());
			for(int i=0;i<temp.size();i++){
				dataType[i] = buffer.get(temp.get(i))[3];
			}
			columnMeta.close();
		}
		catch(Exception e){
			System.out.println("\nError: cannot get data type from ["+tableName+"].");
			e.printStackTrace();
		}
		return dataType;
	}

	public static String[] getNullable(String tableName){
		String[] columnNames = {"rowid","table_name","column_name","data_type","ordinal_position","is_nullable"};
		String[] nullable = new String[0];
		try{
			String databaseName = Database.getDatabaseName();
			RandomAccessFile columnMeta = new RandomAccessFile("data/"+databaseName+"/columns.tbl", "rw");
			String[] condition = {"table_name","=",tableName};
			HashMap<Integer, String[]> buffer = fetchCells(columnMeta,condition,columnNames);	
			nullable = new String[buffer.size()];
			ArrayList<Integer> temp = new ArrayList<Integer>(buffer.keySet());
			for(int i=0;i<temp.size();i++){
				nullable[i] = buffer.get(temp.get(i))[5];
			}
			columnMeta.close();
		}
		catch(Exception e){
			System.out.println("\nError: cannot get nullable from ["+tableName+"].");
			e.printStackTrace();
		}
		return nullable;
	}
	
	public static void query(String tableName,String[] columns,String[] condition){
		try{
			String databaseName = Database.getDatabaseName();
			RandomAccessFile file = new RandomAccessFile("data/"+databaseName+"/"+tableName+".tbl","rw");
			String[] columnNames = getColumnNames(tableName);
			int[] ordinals = new int[0];
			if(columns[0].equals("*"))	ordinals = getOrdinals(columnNames,columnNames);
			else	ordinals = getOrdinals(columns,columnNames);			
			HashMap<Integer, String[]> buffer = fetchCells(file,condition,columnNames);	

			printBuffer(buffer, columnNames,ordinals);
		}
		catch(Exception e){
			System.out.println("\nError: cannot query from ["+tableName+"].");
			e.printStackTrace();
		}
	}
	
	public static void deleteRow(String tableName,String[] condition){
		try{
			String databaseName = Database.getDatabaseName();
			RandomAccessFile file = new RandomAccessFile("data/"+databaseName+"/"+tableName+".tbl","rw");
			String[] columnNames = getColumnNames(tableName);
			HashMap<Integer, String[]> buffer = fetchCells(file,condition,columnNames);	
			ArrayList<Integer> keyArray = new ArrayList<Integer>(buffer.keySet());
			for(int i=0;i<keyArray.size();i++){
				int key = keyArray.get(i);
				int pageIndex = containsKey(file,key);
				int id = Page.getID(file, pageIndex, key);
				Page.deleteLeafCell(file, pageIndex, id);	
			}
		}
		catch(Exception e){
			System.out.println("\nError: cannot delete a row from ["+tableName+"].");
		}
	}
	
	public static void updateRow(String tableName,String[] columns,String[] values,String[] condition){
		try{
			String databaseName = Database.getDatabaseName();
			RandomAccessFile file = new RandomAccessFile("data/"+databaseName+"/"+tableName+".tbl","rw");
			String[] columnNames = getColumnNames(tableName);
			int[] ordinals = getOrdinals(columns,columnNames);
			HashMap<Integer, String[]> buffer = fetchCells(file,condition,columnNames);	
			ArrayList<Integer> keyArray = new ArrayList<Integer>(buffer.keySet());

			for(int i=0;i<keyArray.size();i++){
				int key = keyArray.get(i);
				String[] row = buffer.get(key);
				String[] newData = row.clone();
				for(int j=0;j<ordinals.length;j++){
					newData[ordinals[j]] = values[j];
				}
				String[] deleteCondition = {columnNames[0],"=",Integer.toString(key)};
				deleteRow(tableName,deleteCondition);
				insertInto(file,tableName,columnNames,newData);
			}
		}
		catch(Exception e){
			System.out.println("\nError: cannot update rows in ["+tableName+"].");
		}
	}
	
	public static void dropTable(String tableName){
		try{
			// delete meta
			String[] condition = new String[]{"table_name","=",tableName};
			deleteRow("tables",condition);
			deleteRow("columns",condition);
			// delete file
			String databaseName = Database.getDatabaseName();
			File table = new File("data/"+databaseName,tableName+".tbl");
			table.delete();

			System.out.println("Table ["+tableName+"] dropped.");
		}
		catch(Exception e){
			System.out.println("\nError: cannot drop table ["+tableName+"].");
		}
	}
	
	public static LinkedHashMap<Integer, String[]> fetchCells(RandomAccessFile file,String[] condition,String[] columnNames){
		LinkedHashMap<Integer, String[]> buffer = new LinkedHashMap<Integer, String[]>();
		try{
			String column = condition[0];
			String cmp = condition[1];
			String object = condition[2];
			int ordinal = 0;
			if(!column.equals(""))	ordinal = getOrdinalPosition(column,columnNames);
			short page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int countCells = Page.countCells(file, page);
				for(int j=0;j<countCells;j++ ){
					String[] value = Page.fetchCell(file,page,j);
					if(compare(value[ordinal],cmp,object)){
						buffer.put(Integer.parseInt(value[0]), value);
					}
				}
				page = Page.getRightPagePointer(file, page);
			}
	
		}
		catch(Exception e){
			System.out.println("\nError: cannot fetch cells.");
			e.printStackTrace();
		}
		return buffer;
	}
	
	public static boolean compare(String subject, String cmp, String object){
		boolean result = false;
		switch(cmp){
			case "=":
				if(subject.equals(object))	result = true;
				else	result = false;
				break;
			case ">":
				if(Float.parseFloat(subject) > Float.parseFloat(object))	result = true;
				else	result = false;
				break;
			case ">=":
				if(Float.parseFloat(subject) >= Float.parseFloat(object))	result = true;
				else	result = false;
				break;
			case "<":
				if(Float.parseFloat(subject) < Float.parseFloat(object))	result = true;
				else	result = false;
				break;
			case "<=":
				if(Float.parseFloat(subject) <= Float.parseFloat(object))	result = true;
				else	result = false;
				break;
			case "<>":
				if(!subject.equals(object))	result = true;
				else	result = false;
				break;
			default:
				result = true;
		}
		return result;
	}
	
	public static int findInsertPage(RandomAccessFile file,int key){
		int pageIndex = 0;
		try {
			int page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int[] keyArray = Page.getKeyArray(file,page);
				int siblingPage = Page.getRightPagePointer(file,page);
				if(keyArray.length!=0){
					if(siblingPage!=0){
						int[] siblingKeyArray = Page.getKeyArray(file, siblingPage);
						if(keyArray[0]<key && key<keyArray[keyArray.length-1])	return page;
						if(siblingKeyArray.length!=0 && key<siblingKeyArray[0]) return page;
					}
					else if(siblingPage==0 && keyArray[keyArray.length-1]<key) {	
						return page;	
					}
				}
				else{
					if(siblingPage!=0){
						int[] siblingKeyArray = Page.getKeyArray(file, siblingPage);
						if(siblingKeyArray.length!=0 && key<siblingKeyArray[0])	return page;
					}
				}
				pageIndex = page;
				page = Page.getRightPagePointer(file, page);
			}		

		} catch (Exception e) {
			System.out.println("\nError: cannot find insert leaf page.");
			e.printStackTrace();
		}
		return pageIndex;
	}
	
	// return page index if contains key
	public static int containsKey(RandomAccessFile file,int key){
		try {
			int page = 1;
			while(page!=0){
				file.seek((page-1)*pageSize);
				int[] keyArray = Page.getKeyArray(file,page);
				for(int j=0;j<keyArray.length;j++){
					if(keyArray[j] == key)	return page;
				}
				page = Page.getRightPagePointer(file, page);
			}
			
		} catch (Exception e) {
			System.out.println("\nError at check contains key.");
			e.printStackTrace();
		}
		return 0;
	}
	
	public static void createTable(String tableName,String[] columns){
		try{
			// create table file
			String databaseName = Database.getDatabaseName();
			RandomAccessFile file = new RandomAccessFile("data/"+databaseName+"/"+tableName+".tbl","rw");
			Page.createLeafPage(file);
			file.close();
			// update tables.tbl
			String[] columnNames = new String[0];
			String[] condition = new String[]{"","",""};
			
			RandomAccessFile tableMeta = new RandomAccessFile("data/"+databaseName+"/tables.tbl","rw");
			columnNames = getColumnNames("tables");
			HashMap<Integer, String[]> buffer = fetchCells(tableMeta,condition,columnNames);
			ArrayList<Integer> keySet = new ArrayList<Integer>(buffer.keySet());	
			int tableID = keySet.get(keySet.size()-1)+1;
			String[] tableData = {Integer.toString(tableID),tableName};
			columnNames = new String[]{"rowid","table_name"};
			insertInto(tableMeta,"tables",columnNames,tableData);

			// update columns.tbl
			RandomAccessFile columnMeta = new RandomAccessFile("data/"+databaseName+"/columns.tbl","rw");
			columnNames = getColumnNames("columns");
			condition = new String[]{"","",""};
			buffer = fetchCells(columnMeta,condition,columnNames);	
			keySet = new ArrayList<Integer>(buffer.keySet());
			int columnID = keySet.get(keySet.size()-1)+1;
			
			for(int i=0;i<columns.length;i++){
				String[] column = columns[i].split(" ");
				String nullable = "no";
				if(column.length < 4){
					if(i==0){
						System.out.println("\nError: first column should be primary key.");
						return;
					}else{
						nullable = "yes";
					}	
				}
				String columnName = column[0];
				String dataType = column[1];
				String[] columnData = {Integer.toString(columnID++),tableName,columnName,dataType,Integer.toString(i+1),nullable};
				columnNames = new String[]{"rowid","table_name","column_name","data_type","ordinal_position","is_nullable"};
				insertInto(columnMeta,"columns",columnNames,columnData);	
			}	
			
			tableMeta.close();
			columnMeta.close();
			System.out.println("\nTable ["+tableName+"] created.");
		}
		catch(Exception e){
			System.out.println("\nError: cannot create table [" + tableName + "].");
			e.printStackTrace();
		}	

	}
	
	
	public static void insertInto(RandomAccessFile file,String tableName, String[] columns, String[] data){
		int key = Integer.parseInt(data[0]);
		try{
			String[] columnNames = getColumnNames(tableName);
			String[] nullable = getNullable(tableName);
			String[] dataType = getDataType(tableName);
			// put data in corresponding location
			if(columns.length<columnNames.length){
				int[] ordinals = getOrdinals(columns,columnNames);
				String[] newData = new String[columnNames.length];
				for(int i=0;i<newData.length;i++)	newData[i] = "null";
				for(int i=0;i<columns.length;i++)	newData[ordinals[i]] = data[i];
				data = newData.clone();
			}
		
			// check primary key constraint
			if(containsKey(file,key) ==0){
				int pageIndex = findInsertPage(file,key);
				byte[] stc = stcArrayEncode(data,dataType);
				short payload = Page.getPayload(data,dataType);

				// check null constraint
				for(int i=0;i<data.length;i++){
					if(data[i].equals("null") && nullable[i].equals("no")){
						System.out.println("\nError: violate null constraint, column ["+columnNames[i]+"] cannot be null.");
						return;
					}	
				}
				short size = (short)(payload + 6);
				if(Page.checkContentCapacity(file,pageIndex,size)){
					Page.insertLeafCell(file, pageIndex, size, stc, data);
				}else{
					Page.splitLeaf(file, pageIndex);
					insertInto(file,tableName,columnNames,data);	
				}
			}else{
				System.out.println("\nError: violate primary key constraint.");
			}
		}
		catch(Exception e){
			System.out.println("\nError: cannot insert table [" + tableName + "].");
			e.printStackTrace();
		}

	}
	
	public static byte getOrdinalPosition(String column,String[] columnNames){
		int position = -1;
		for(int i=0;i<columnNames.length;i++){
			if(columnNames[i].equals(column))	position = i;
		}
		if(position == -1){
			System.out.println("\nError: does not contain column ["+column+"].");
		}
		return (byte) position;
	}
	
	public static int[] getOrdinals(String[] columns,String[] columnNames){
		int[] ordinals = new int[columns.length];
		for(int i=0;i<columns.length;i++){
			ordinals[i] = getOrdinalPosition(columns[i],columnNames);
		}
		return ordinals;
	}
	
	public static int countPage(RandomAccessFile file){
		int count = 0;
		try {
			count = (int)file.length()/pageSize;
		} catch (Exception e) {
			System.out.println("\nError at countPage");
			e.printStackTrace();
		}
		return count;
	}
	
	public static void initMeta(){
		// create directory
		String databaseName = Database.getDatabaseName();
		try{
			File dataDir = new File("data/"+databaseName);
			dataDir.mkdir();
			String[] existCatalog = dataDir.list();
			for(int i=0;i<existCatalog.length;i++){
				File existMata = new File(dataDir,existCatalog[i]);
				existMata.delete();
			}
		}
		catch(SecurityException e){
			System.out.println("\nError: cannot create the directory.");
			System.out.println(e);
		}
		
		// create catalog.tbl
		try {
			tablesCatalog = new RandomAccessFile("data/"+databaseName+"/tables.tbl","rw");
			Page.createLeafPage(tablesCatalog);
			String[] data = {"1","tables"};
			String[] dataType = {"int","text"};
			short payload = Page.getPayload(data, dataType);
			short size = (short)(payload + 6);
			byte[] stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(tablesCatalog, 1, size, stc, data);
			
			data = new String[]{"2","columns"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(tablesCatalog, 1, size, stc, data);
			tablesCatalog.close();			
		} catch (Exception e) {
			System.out.println("\nError: cannot create the tables catalog.");
			e.printStackTrace();
		}
		
		try {
			columnsCatalog = new RandomAccessFile("data/"+databaseName+"/columns.tbl","rw");
			Page.createLeafPage(columnsCatalog);
			
			String[] data = new String[]{"1","tables","rowid","int","1","no"};
			String[] dataType = new String[]{"int","text","text","text","tinyint","text"};
			short payload = Page.getPayload(data, dataType);
			short size = (short)(payload + 6);
			byte[] stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"2","tables","table_name","text","2","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"3","columns","rowid","int","1","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"4","columns","table_name","text","2","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"5","columns","column_name","text","3","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"6","columns","data_type","text","4","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"7","columns","ordinal_position","tinyint","5","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			
			data = new String[]{"8","columns","is_nullable","text","6","no"};
			dataType = new String[]{"int","text","text","text","tinyint","text"};
			payload = Page.getPayload(data, dataType);
			size = (short)(payload + 6);
			stc = stcArrayEncode(data,dataType);
			Page.insertLeafCell(columnsCatalog, 1, size, stc, data);
			columnsCatalog.close();

		} catch (Exception e) {
			System.out.println("\nError: cannot create the columns catalog.");
			e.printStackTrace();
		}	

	}
	
	// print query columns 
	public static void printBuffer(HashMap<Integer, String[]> buffer, String[] columnNames, int[] ordinals){
		String databaseName = Database.getDatabaseName();
		int[] width = new int[ordinals.length];
		
		for(int i=0;i<ordinals.length;i++){
			width[i] = columnNames[ordinals[i]].length();
		}
		for(int key: buffer.keySet()){
			String[] value = buffer.get(key);
			for(int i=0;i<ordinals.length;i++){
				if(value[ordinals[i]].length() > width[i])
					width[i] = value[ordinals[i]].length();
			}
		}
		System.out.println();
		for(int i=0;i<ordinals.length;i++){
			if(i==0)	System.out.print(" +--" + DatabaseEngine.line("-",width[i]) + "--+");
			else if(i==ordinals.length-1)	System.out.print(DatabaseEngine.line("-",width[i])+ "----+ ");
			else System.out.print(DatabaseEngine.line("-",width[i])+"----+");
		}
		System.out.println();
		for(int i=0;i<ordinals.length;i++){
			int margin = width[i] - columnNames[ordinals[i]].length();
			if(i==0)	System.out.print(" |  " + columnNames[ordinals[i]] +DatabaseEngine.line(" ",margin)+ "  |  ");
			else if(i==columnNames.length-1)	System.out.print(columnNames[ordinals[i]] +DatabaseEngine.line(" ",margin)+"  | ");
			else System.out.print(columnNames[ordinals[i]] + DatabaseEngine.line(" ",margin)+"  |  ");
		}
		System.out.println();
		for(int i=0;i<ordinals.length;i++){
			if(i==0)	System.out.print(" +--" + DatabaseEngine.line("-",width[i]) + "--+");
			else if(i==ordinals.length-1)	System.out.print(DatabaseEngine.line("-",width[i])+ "----+ ");
			else System.out.print(DatabaseEngine.line("-",width[i])+"----+");
		}
		System.out.println();
		for(int key: buffer.keySet()){
			// jump meta tables;
			if(!databaseName.equals("catalog")&&columnNames[ordinals[0]].equals("table_name")&&key<3) continue;
			String[] value = buffer.get(key);
			
			for(int i=0;i<ordinals.length;i++){
				int margin = width[i] - value[ordinals[i]].length();
				if(i==0)	System.out.print(" |  " + value[ordinals[i]]+DatabaseEngine.line(" ",margin) + "  |  ");
				else if(i==value.length-1)	System.out.print(value[ordinals[i]]+DatabaseEngine.line(" ",margin)+"  | ");
				else System.out.print(value[ordinals[i]]+DatabaseEngine.line(" ",margin)+"  |  ");
			}
			System.out.println();
			
		}
		for(int i=0;i<ordinals.length;i++){
			if(i==0)	System.out.print(" +--" + DatabaseEngine.line("-",width[i]) + "--+");
			else if(i==ordinals.length-1)	System.out.print(DatabaseEngine.line("-",width[i])+ "----+ ");
			else System.out.print(DatabaseEngine.line("-",width[i])+"----+");
		}
		System.out.println();
		
		
	}
	
	public static byte stcEncode(String data, String dataType){
		byte stc;
		dataType = dataType.toLowerCase();
		if(data.equals("null")){
			switch(dataType){
				case "tinyint":     stc = 0x00;	break;
				case "smallint":    stc = 0x01;	break;
				case "int":			stc = 0x02;	break;
				case "real":        stc = 0x02;	break;
				case "float":		stc = 0x03; break;
				case "double":      stc = 0x03;	break;
				case "datetime":    stc = 0x03;	break;
				case "date":        stc = 0x03;	break;	
				case "bigint":      stc = 0x03;	break;
				case "text":        stc = 0x03;	break;
				default:			stc = 0x00;	break;
			}							
		}else{
			switch(dataType){
				case "tinyint":     stc = 0x04;	break;
				case "smallint":    stc = 0x05;	break;
				case "int":			stc = 0x06;	break;
				case "bigint":      stc = 0x07;	break;
				case "real":        stc = 0x08;	break;
				case "float":		stc = 0x08; break;
				case "double":      stc = 0x09;	break;
				case "datetime":    stc = 0x0A;	break;
				case "date":        stc = 0x0B;	break;
				case "text":        stc = (byte)(0x0C + data.length());	break;
				default:			stc = 0x00;	break;
			}
		}
		return stc;
	}	

	public static byte[] stcArrayEncode(String[] data, String[] dataType){
		byte[] stcArray = new byte[data.length];
		for(int i=0;i<data.length;i++){
			stcArray[i] = stcEncode(data[i],dataType[i]);
		}
		return stcArray;
	}
	

	/**
	 * <p>This method is used for debugging and file analysis.
	 * @param raf is an instance of {@link RandomAccessFile}. 
	 * <p>This method will display the contents of the file to Stanard Out (stdout)
	 *    as hexadecimal byte values.
	 */
	static void displayBinaryHex(RandomAccessFile raf) {
		try {
			System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
			raf.seek(0);
			long size = raf.length();
			int row = 1;
			System.out.print("0000\t0x0000\t");
			while(raf.getFilePointer() < size) {
				System.out.print(String.format("%02X ", raf.readByte()));
				if(row % 16 == 0) {
					System.out.println();
					System.out.print(String.format("%04d\t0x%04X\t", row, row));
				}
				row++;
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
}
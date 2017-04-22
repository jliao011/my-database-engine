import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page {
	public static int pageSize = 512;
	public static String dataFormat = "yyyy-MM-dd_HH:mm:ss";
	
	public static void main(String[] args){
	}
	
	public static int createLeafPage(RandomAccessFile file){
		int countPage = 0;
		try {
			countPage = (int)(file.length()/(long)pageSize) + 1;
			file.setLength(pageSize * countPage);
			file.seek((countPage-1) * pageSize);
			file.writeByte(0x0D);
		} catch (Exception e) {
			System.out.println("\nError: cannot create a leaf page.");
			e.printStackTrace();
		}
		return countPage;
	}
	
	public static int createInteriorPage(RandomAccessFile file){
		int countPage = 0;
		try {
			countPage = (int)(file.length()/(long)pageSize) + 1;
			file.setLength(pageSize * countPage);
			file.seek((countPage-1) * pageSize);
			file.writeByte(0x05);
		} catch (Exception e) {
			System.out.println("\nError: cannot create an interior page.");
			e.printStackTrace();
		}
		return countPage;	
	}

	public static void allotCells(RandomAccessFile file, int pageIndex, byte num){
		try {
			file.seek((pageIndex-1) * pageSize + 1);
			file.writeByte(num);
		} catch (Exception e) {
			System.out.println("\nError: cannot allot number of cells.");
			e.printStackTrace();
		}	
	}
	
	public static byte countCells(RandomAccessFile file, int pageIndex){
		byte count = 0;
		try {
			file.seek((pageIndex-1) * pageSize + 1);
			count = file.readByte();
		} catch (Exception e) {
			System.out.println("\nError: cannot count number of cells.");
			e.printStackTrace();
		}
		return count;	
	}
	
	public static void setContentOffset(RandomAccessFile file, int pageIndex, short offset){
		try {
			file.seek((pageIndex-1) * pageSize + 2);
			if(offset == 0){
				file.writeShort(pageSize);
			}else{
				file.writeShort(offset);
			}
		} catch (IOException e) {
			System.out.println("\nError: cannot set start cell offset.");
			e.printStackTrace();
		}
	}
	
	public static short getContentOffset(RandomAccessFile file, int pageIndex){
		short offset = 0;
		try {
			file.seek((pageIndex-1) * pageSize + 2);
			offset = file.readShort();
			if(offset == 0)	offset = (short)pageSize;
		} catch (Exception e) {
			System.out.println("\nError: cannot get start cell offset.");
			e.printStackTrace();
		}
		return offset;
	}

	public static void setRightPagePointer(RandomAccessFile file,int pageIndex, int pointer){
		try {
			file.seek((pageIndex-1) * pageSize + 4);
			file.writeShort(pointer);
		} catch (Exception e) {
			System.out.println("\nError: cannot set right page pointer.");
			e.printStackTrace();
		}
	}

	public static short getRightPagePointer(RandomAccessFile file,int pageIndex){
		short pointer = 0;
		try {
			file.seek((pageIndex-1) * pageSize + 4);
			pointer = file.readShort();
		} catch (Exception e) {
			System.out.println("\nError: cannot get right page pointer.");
			e.printStackTrace();
		}
		return pointer;	
	}

	public static void setParentPage(RandomAccessFile file,int pageIndex, int parentPage){
		try {
			file.seek((pageIndex-1) * pageSize + 6);
			file.writeShort(parentPage);
		} catch (Exception e) {
			System.out.println("\nError: cannot set parent page.");
			e.printStackTrace();
		}	
	}
	
	public static short getParentPage(RandomAccessFile file,int pageIndex){
		short parentPage = 0;
		try {
			file.seek((pageIndex-1) * pageSize + 6);
			parentPage = file.readShort();
		} catch (Exception e) {
			System.out.println("\nError: cannot get parent page.");
			e.printStackTrace();
		}	
		return parentPage;
	}
	
	public static short[] getCellOffsetArray(RandomAccessFile file, int pageIndex){
		int count = countCells(file,pageIndex);
		short[] cellOffsetArray = new short[count];
		try {
			file.seek((pageIndex-1) * pageSize + 8);
			for(int i=0;i<count;i++){
				cellOffsetArray[i] = file.readShort();
			}
		} catch (Exception e) {
			System.out.println("\nError: cannot get cell offset array.");
			e.printStackTrace();
		}
		return cellOffsetArray;	
	}
	
	public static void setCellOffsetArray(RandomAccessFile file, int pageIndex, short[] offsetArray){
		try {
			file.seek((pageIndex-1) * pageSize + 8);
			for(int i=0;i<offsetArray.length;i++){
				file.writeShort(offsetArray[i]);
			}
		} catch (Exception e) {
			System.out.println("\nError: cannot set cell offset array.");
			e.printStackTrace();
		}
	}
	
	public static int[] getKeyArray(RandomAccessFile file, int pageIndex){
		int count = countCells(file,pageIndex);
		int[] keyArray = new int[count];
		short[] cellOffsetArray = getCellOffsetArray(file,pageIndex);
		byte offset = 0;
		try{
			file.seek((pageIndex-1) * pageSize);
			byte pageType = file.readByte();
			switch(pageType){
			case 0x0D:
				offset = 2;
				break;
			case 0x05:
				offset = 4;
				break;
			}
			for(int i=0;i<count;i++){
				file.seek((pageIndex-1) * pageSize + cellOffsetArray[i] + offset);
				keyArray[i] = file.readInt();
			}
		}
		catch (Exception e) {
			System.out.println("\nError: cannot get key array.");
			e.printStackTrace();
		}
		return keyArray;	
	}
	
	public static boolean containsKey(RandomAccessFile file, int pageIndex, int key){
		int[] keyArray = getKeyArray(file,pageIndex);
		for(int i=0;i<keyArray.length;i++)
			if(keyArray[i] == key)	return true;
		return false;
	}
	
	public static void sortCellOffsetArray(RandomAccessFile file, int pageIndex){
		int count = countCells(file,pageIndex);
		int[] keyArray = getKeyArray(file,pageIndex);
		short[] cellOffsetArray = getCellOffsetArray(file,pageIndex);
		for(int i=0;i<count;i++){
			for(int j=1;j<count-i;j++){
				if(keyArray[j-1]>keyArray[j]){
					int temp1 = keyArray[j-1];
					keyArray[j-1] = keyArray[j];
					keyArray[j] = temp1;
					short temp2 = cellOffsetArray[j-1];
					cellOffsetArray[j-1] = cellOffsetArray[j];
					cellOffsetArray[j] = temp2;
				}
			}
		}
		try{
			file.seek((pageIndex-1) * pageSize + 8);
			for(int i=0;i<count;i++)
				file.writeShort(cellOffsetArray[i]);
			
		}
		catch(Exception e){
			System.out.println("\nError: cannot sort cell offset array.");
			e.printStackTrace();	
		}
		
	}

	public static boolean checkContentCapacity(RandomAccessFile file, int pageIndex){
		int count = countCells(file,pageIndex); 
		if(count>=40)	return false;	// max capacity is 50
		return true;
	}
	
	public static boolean checkContentCapacity(RandomAccessFile file, int pageIndex, int cellSize){
		int contentOffset = getContentOffset(file,pageIndex);
		int count = countCells(file,pageIndex);	
		int minContentOffset = 8 + 2 * (count + 1);
		int insertContentOffset = contentOffset - cellSize;
		if(insertContentOffset < minContentOffset)
			return false;
		return true;	
	}

	public static int findMidKey(RandomAccessFile file, int pageIndex){
		int[] keyArray = getKeyArray(file,pageIndex);
		int mid = keyArray.length/2;
		return keyArray[mid];
	}
	

	
	public static short getCellOffset(RandomAccessFile file, int pageIndex, int id){
		sortCellOffsetArray(file,pageIndex);
		short offset = 0;
		try{
			file.seek((pageIndex-1)*pageSize + 8 + id * 2);
			offset = file.readShort();
		}
		catch(Exception e){
			System.out.println("\nError: cannot get cell offset.");
			e.printStackTrace();
		}	
		return offset;
		
	}
	
	public static int getPointerCell(RandomAccessFile file, int parentPage, int pageIndex){
		int id = 0;
		try{
			int count = countCells(file,parentPage);
			for(int i=0;i<count;i++){
				short offset = getCellOffset(file,parentPage,i);
				file.seek((parentPage-1)*pageSize + offset);
				int childPage = file.readInt();
				if(childPage == pageIndex)	id = i;
			}
		}
		catch(Exception e){
			System.out.println("\nError: cannot get pointer cell.");
			e.printStackTrace();	
		}
		return id;
	}

	public static void setPointerCell(RandomAccessFile file, int parentPage, int id, int pageIndex){
		try{
			if(id == 0){
				setRightPagePointer(file,parentPage,pageIndex);
			}else{
				short offset = getCellOffset(file,parentPage,id);
				file.seek((parentPage-1)*pageSize + offset);
				file.writeInt(pageIndex);
			}
		}
		catch(Exception e){
			System.out.println("\nError: cannot set pointer cell.");
			e.printStackTrace();	
		}
	}
	
	public static void removeLeafCells(RandomAccessFile file, int pageIndex, int newCount){
		try{
			ArrayList<byte[]> temp = new ArrayList<byte[]>();
			short[] cellOffsetArray = new short[newCount];
			short content = 512;
			for(int i=0;i<newCount;i++){
				short offset = getCellOffset(file,pageIndex,i);
				file.seek((pageIndex-1)*pageSize + offset);
				int size = file.readShort() + 6;
				file.seek((pageIndex-1)*pageSize + offset);
				byte[] cell = new byte[size];
				file.read(cell);
				temp.add(cell);
			}
			for(int i=0;i<temp.size();i++){
				content -= temp.get(i).length;
				file.seek((pageIndex-1)*pageSize + content);
				file.write(temp.get(i));
				cellOffsetArray[i] = content;
			}
			allotCells(file,pageIndex,(byte)(newCount));
			setContentOffset(file,pageIndex,content);
			setCellOffsetArray(file,pageIndex,cellOffsetArray);
		}
		catch(Exception e){
			System.out.println("\nError: cannot delete leaf cells.");
			e.printStackTrace();	
		}
	}
	public static int getID(RandomAccessFile file,int pageIndex,int key){
		int id = -1;
		int[] keyArray = getKeyArray(file,pageIndex);
		for(int i=0;i<keyArray.length;i++){
			if(keyArray[i] == key)	id = i;
		}
		return id;
	}
	
	public static void deleteLeafCell(RandomAccessFile file, int pageIndex, int id){
		try{
			byte count = countCells(file,pageIndex);
			ArrayList<byte[]> temp = new ArrayList<byte[]>();
			short[] cellOffsetArray = new short[count-1];
			short content = 512;
			for(int i=0;i<count;i++){
				if(i != id){
					short offset = getCellOffset(file,pageIndex,i);
					file.seek((pageIndex-1)*pageSize + offset);
					int size = file.readShort() + 6;
					file.seek((pageIndex-1)*pageSize + offset);
					byte[] cell = new byte[size];
					file.read(cell);
					temp.add(cell);
				}
			}
			for(int i=0;i<temp.size();i++){
				content -= temp.get(i).length;
				file.seek((pageIndex-1)*pageSize + content);
				file.write(temp.get(i));
				cellOffsetArray[i] = content;
			}
			allotCells(file,pageIndex,(byte)(count-1));
			setContentOffset(file,pageIndex,content);
			setCellOffsetArray(file,pageIndex,cellOffsetArray);
		}
		catch(Exception e){
			System.out.println("\nError: cannot delete leaf cells.");
			e.printStackTrace();	
		}
	}
	
	public static void splitLeafPage(RandomAccessFile file, int pageIndex, int newPageIndex){
		try{
			int count = countCells(file,pageIndex);
			int mid = count/2;
			int leftCount = mid, rightCount = count - leftCount;
			short content = (short)pageSize;
			short[] rightOffsetArray = new short[rightCount];
			// parse data to new page and update new page header
			for(int i=mid;i<count;i++){
				short offset = getCellOffset(file,pageIndex,i);
				file.seek((pageIndex-1)*pageSize + offset);
				int size = file.readShort() + 6;
				content -= size;
				file.seek((pageIndex-1)*pageSize + offset);
				byte[] cell = new byte[size];
				file.read(cell);
				file.seek((newPageIndex-1)*pageSize + content);
				file.write(cell);
				rightOffsetArray[i-mid] = content;
			}
			allotCells(file,newPageIndex,(byte)rightCount);
			setContentOffset(file,newPageIndex,content);
			setCellOffsetArray(file,newPageIndex,rightOffsetArray);
			// update right sibling
			int right = getRightPagePointer(file,pageIndex);
			setRightPagePointer(file,newPageIndex,right);
			setRightPagePointer(file,pageIndex,newPageIndex);
			// update parent
			short parent = getParentPage(file,pageIndex);
			setParentPage(file,newPageIndex,parent);
			// update current page
			removeLeafCells(file,pageIndex,leftCount);
			
		}
		catch(Exception e){
			System.out.println("\nError: cannot split leaf page.");
			e.printStackTrace();
		}
	}

	public static void removeInteriorCells(RandomAccessFile file, int pageIndex, int newCount){
		try{
			ArrayList<byte[]> temp = new ArrayList<byte[]>();
			short[] cellOffsetArray = new short[newCount];
			short content = 512;
			for(int i=0;i<newCount;i++){
				short offset = getCellOffset(file,pageIndex,i);
				file.seek((pageIndex-1)*pageSize + offset);
				int size = 8;
				byte[] cell = new byte[size];
				file.read(cell);
				temp.add(cell);
			}
			for(int i=0;i<temp.size();i++){
				content -= temp.get(i).length;
				file.seek((pageIndex-1)*pageSize + content);
				file.write(temp.get(i));
				cellOffsetArray[i] = content;
			}
			allotCells(file,pageIndex,(byte)(newCount));
			setContentOffset(file,pageIndex,content);
			setCellOffsetArray(file,pageIndex,cellOffsetArray);
		}
		catch(Exception e){
			System.out.println("\nError: cannot delete interior cells.");
			e.printStackTrace();	
		}
	}
	
	public static void splitInteriorPage(RandomAccessFile file, int pageIndex, int newPageIndex){
		try{
			int count = countCells(file,pageIndex);
			int mid = count/2;
			int leftCount = mid, rightCount = count - leftCount;
			short content = (short)pageSize;
			short[] rightOffsetArray = new short[rightCount];
			// parse data to new page and update new page header
			for(int i=mid;i<count;i++){
				short offset = getCellOffset(file,pageIndex,i);
				file.seek((pageIndex-1)*pageSize + offset);
				int size = 8;
				content -= size;
				byte[] cell = new byte[size];
				file.read(cell);
				file.seek((newPageIndex-1)*pageSize + content);
				file.write(cell);
				rightOffsetArray[i-mid] = content;
			}
			allotCells(file,newPageIndex,(byte)rightCount);
			setContentOffset(file,newPageIndex,content);
			setCellOffsetArray(file,newPageIndex,rightOffsetArray);
			// update right most page pointer
			int right = getRightPagePointer(file,pageIndex);
			setRightPagePointer(file,newPageIndex,right);
			short offset = getCellOffset(file,pageIndex,mid);
			file.seek((pageIndex-1)*pageSize + offset);
			int midPointer = file.readInt();
			setRightPagePointer(file,pageIndex,midPointer);
			// update parent
			short parent = getParentPage(file,pageIndex);
			setParentPage(file,newPageIndex,parent);
			// update current page
			removeInteriorCells(file,pageIndex,leftCount);			
		}
		catch(Exception e){
			System.out.println("\nError: cannot split interior page.");
			e.printStackTrace();
		}
	}
	
	public static void insertInteriorCell(RandomAccessFile file, int pageIndex, int child, int key){
		try{
			int count = countCells(file,pageIndex);
			int contentOffset = getContentOffset(file,pageIndex) - 8;
			file.seek((pageIndex-1)*pageSize + contentOffset);
			file.writeInt(child);
			file.writeInt(key);
			allotCells(file,pageIndex,(byte)(++count));
			setContentOffset(file,pageIndex,(short)contentOffset);
			file.seek((pageIndex-1)*pageSize +8 + 2*count);
			file.writeShort(contentOffset);	
			sortCellOffsetArray(file,pageIndex);
		}
		catch(Exception e){
			System.out.println("\nError: cannot insert into interior page.");
			e.printStackTrace();
		}	
		
	}

	public static void splitLeaf(RandomAccessFile file, int pageIndex){
		int newLeafPage = createLeafPage(file);
		int midKey = findMidKey(file,pageIndex);
		int parent = getParentPage(file,pageIndex);
		splitLeafPage(file,pageIndex,newLeafPage);
		if(parent == 0){	// create root
			int root = createInteriorPage(file);
			setParentPage(file,pageIndex,root);
			setParentPage(file,newLeafPage,root);
			setRightPagePointer(file,root,newLeafPage);
			insertInteriorCell(file,root,pageIndex,midKey);
		}else{
			int id = getPointerCell(file,parent,pageIndex);
			setPointerCell(file,parent,id,newLeafPage);
			insertInteriorCell(file,parent,pageIndex,midKey);
			while(!checkContentCapacity(file,parent)){
				parent = splitInterior(file,parent);
			}	
		}	
	}
	
	public static int splitInterior(RandomAccessFile file, int pageIndex){
		int newInteriorPage = createInteriorPage(file);
		int midKey = findMidKey(file,pageIndex);
		int parent = getParentPage(file,pageIndex);
		splitInteriorPage(file,pageIndex,newInteriorPage);
		if(parent == 0){
			int root = createInteriorPage(file);
			setParentPage(file,pageIndex,root);
			setParentPage(file,newInteriorPage,root);
			setRightPagePointer(file,root,newInteriorPage);
			insertInteriorCell(file,root,pageIndex,midKey);
			return root;
		}else{
			int id = getPointerCell(file,parent,pageIndex);
			setPointerCell(file,parent,id,newInteriorPage);
			insertInteriorCell(file,parent,pageIndex,midKey);
			return parent;
		}
	}

	public static void insertLeafCell(RandomAccessFile file, int pageIndex, short size, byte[] stc, String[] data){
		try{
			byte count = countCells(file,pageIndex);
			int contentOffset = getContentOffset(file,pageIndex) - size;
			int key = Integer.parseInt(data[0]);
			file.seek((pageIndex-1)*pageSize + contentOffset);
			file.writeShort(size-6);
			file.writeInt(key);
			int numCol = stc.length;
			file.writeByte(numCol);
			file.write(stc);
			for(int i=0;i<numCol;i++){
				switch(stc[i]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(data[i]));
						break;
					case 0x05:
						file.writeShort(new Short(data[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(data[i]));
						break;
					case 0x07:
						file.writeLong(new Long(data[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(data[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(data[i]));
						break;
					case 0x0A:
						Date date = new SimpleDateFormat(dataFormat).parse(data[i]);
						long time = date.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						Date date2 = new SimpleDateFormat(dataFormat).parse(data[i] + "_00:00:00");
						long time2 = date2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(data[i]);
						break;
				}
			}
			allotCells(file,pageIndex,++count);
			setContentOffset(file,pageIndex,(short)contentOffset);
			file.seek((pageIndex-1)*pageSize + 8 + 2*(count-1));
			file.writeShort(contentOffset);	
			sortCellOffsetArray(file,pageIndex);
		}
		catch(Exception e){
			System.out.println("\nError: cannot insert into leaf page.");
			e.printStackTrace();
		}
	}

	public static short getPayload(String[] data,String[] dataType){
		short payload = (short)(1 + dataType.length);
		for(int i=0;i<data.length;i++){
			switch(dataType[i]){
				case "tinyint":
					payload += 1;
					break;
				case "smallint":
					payload += 2;
					break;
				case "int":
					payload += 4;
					break;
				case "bigint":
					payload += 8;
					break;
				case "real":
					payload += 4;
					break;
				case "float":
					payload += 4;
					break;
				case "double":
					payload += 8;
					break;
				case "datetime":
					payload += 8;
					break;
				case "date":
					payload += 8;
					break;
				case "text":
					payload += data[i].length();
					break;
				default:
					break;
			}
		}
		return payload;
	}
	
	public static String[] fetchCell(RandomAccessFile file, int pageIndex, int id){
		String[] cell = new String[0];
		SimpleDateFormat dataString;
		try {
			short offset = getCellOffset(file,pageIndex,id);
			file.seek((pageIndex-1)*pageSize + offset);
			int payload = file.readShort();
			int key = file.readInt();
			int countColumn = file.readByte();
			cell = new String[countColumn];
			byte[] stc = new byte[countColumn];
			file.read(stc);
			for(int i=0;i<countColumn;i++){
				switch(stc[i]){
					case 0x00:
						file.readByte();
						cell[i] = "null";
						break;
					case 0x01:
						file.readShort();
						cell[i] = "null";
						break;
					case 0x02:
						file.readInt();
						cell[i] = "null";
						break;
					case 0x03:
						file.readLong();
						cell[i] = "null";
						break;
					case 0x04:
						cell[i] = Byte.toString(file.readByte());
						break;
					case 0x05:
						cell[i] = Short.toString(file.readShort());
						break;
					case 0x06:
						cell[i] = Integer.toString(file.readInt());
						break;
					case 0x07:
						cell[i] = Long.toString(file.readLong());
						break;
					case 0x08:
						cell[i] = String.valueOf(file.readFloat());
						break;
					case 0x09:
						cell[i] = String.valueOf(file.readDouble());
						break;
					case 0x0A:
						dataString = new SimpleDateFormat(dataFormat);
						Date dateTime = new Date(file.readLong());
						cell[i] = dataString.format(dateTime);
						break;
					case 0x0B:
						dataString = new SimpleDateFormat(dataFormat);
						Date date = new Date(file.readLong());
						cell[i] = dataString.format(date).substring(0,10);
						break;
					default:
						int length = stc[i]-0x0C;
						byte[] string = new byte[length];
						file.read(string);
						cell[i] = new String(string);
						break;	
				}
			}
		} catch (Exception e) {
			System.out.println("\nError: cannot fetch a cell.");
			e.printStackTrace();
		}
		return cell;
	}
	
}
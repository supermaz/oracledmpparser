
package com.jansensystems.oracledmpparser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author Matthias Jansen / Jansen-Systems
 */
public class DMPParser {
    private boolean afterInsertStatement = false;
    private String currentTable = null;
    private DMPTable currentTableObj = null;
    private List<DMPTable> tables = new ArrayList<>();
    
    public List<DMPTable> parseFile(InputStream in) throws IOException {
	ByteArrayOutputStream temp = new ByteArrayOutputStream();
	try {
	    int b = 0;
	    Queue<Integer> lastBytes = new LinkedList<>();
	    while ((b = in.read()) >= 0) {
		if (b == 0x0a) {
		    if (afterInsertStatement) {
			if (lastBytesEndOfInsertData(lastBytes)) {
			    temp.write(b);
			    temp.close();
			    parseLine(temp.toByteArray());
			    temp = new ByteArrayOutputStream();
			} else {
			    temp.write(b);
			    // nothing else to do, just add more bytes
			}
		    } else {
			// newline
			temp.close();
			parseLine(temp.toByteArray());
			temp = new ByteArrayOutputStream();
		    }
		} else {
		    temp.write(b);
		    lastBytes.add(b);
		    if (lastBytes.size() > 4) lastBytes.poll();
		}
	    }
	    parseLine(temp.toByteArray());
	} finally {
	    temp.close();
	}
	return tables;
    }
    
    private boolean lastBytesEndOfInsertData(Queue<Integer> lastBytes) {
	if (lastBytes.size() < 4) return false;
	Integer[] bytes = (Integer[]) lastBytes.toArray(new Integer[4]);
	int i = 0;
	// System.out.println("LastBytes: " + lastBytes.stream().map(x -> String.format("%02x", x)).collect(Collectors.joining(", ")));
	return bytes[i] == 0 && bytes[i+1] == 0 && (bytes[i+2] & 0xff) == 0xff && (bytes[i+3] & 0xff) == 0xff;
    }
    
    private void parseLine(byte[] bytes) {
	// test what it is
	if (afterInsertStatement) {
	    // should be data here
	    currentTableObj.dataRows = parseDataRow(bytes);
	}
	afterInsertStatement = false;
	String testString = new String(bytes);
	if (testString.startsWith("TABLE ")) {
	    currentTable = testString.substring(7, testString.length()-1);
	} else if (testString.startsWith("CREATE TABLE ")) {
	    currentTableObj = new DMPTable();
	    tables.add(currentTableObj);
	    currentTableObj.tableName = currentTable;
	    currentTableObj.createTableSQL = testString;
	} else if (testString.startsWith("INSERT INTO ")) {
	    afterInsertStatement = true;
	}
    }
    
    private List<DMPRow> parseDataRow(byte[] bytes) {
	List<DMPRow> rows = new ArrayList<>();
	DMPRow currentRow = null;
	DMPItem cur = null;

	int takeByteCount = 0;
	int nullCount = 0;
	boolean hasStarted = false;
	for (int i=0;i<bytes.length;i++) {
	    if (bytes[i] == 0) {
		nullCount++;
	    } else {
		nullCount = 0;
	    }
	    if (nullCount == 4) { hasStarted = true; }
	    else if (hasStarted) {
		if (i > 0 && bytes.length > i+4 && bytes[i] == 0 && bytes[i+1] == 0 && (bytes[i+2] & 0xff) == 0xff && (bytes[i+3] & 0xff) == 0xff) {
		    // the end
		    hasStarted = false;
		} else if (i > 0 && bytes[i] == 0 && bytes[i - 1] == 0) {
		    // we have a new row
		    cur = new DMPItem();
		    currentRow = new DMPRow();
		    rows.add(currentRow);
		    currentRow.items.add(cur);
		} else if (takeByteCount > 0) {
		    cur.bytes.add(bytes[i]);
		    takeByteCount--;
		    if (takeByteCount == 0) {
			// end
		    }
		} else if (bytes.length > (i+1) && ((bytes[i] & 0xff) == 0xfe) && (bytes[i+1] & 0xff) == 0xff) {
		    // NULL value
		    cur = new DMPItem();
		    currentRow.items.add(cur);
		    cur.itemType = DMPItemType.NULL;
		    cur.noOfbytes = 2;
		    cur.bytes.add(bytes[i]);
		    cur.bytes.add(bytes[i+1]);
		    i++;
		} else if (bytes.length > (i+1) && ((bytes[i] & 0xff) > 0) && (bytes[i+1] & 0xff) == 0) {
		    cur = new DMPItem();
		    currentRow.items.add(cur);

		    takeByteCount = (bytes[i] & 0xff);
		    cur.noOfbytes = takeByteCount;
		    // System.out.println("takeByteCount = " + takeByteCount);
		    i++;
		} else if (bytes[i] == 0) {
		    cur = new DMPItem();
		    currentRow.items.add(cur);
		} else {
		    cur.bytes.add(bytes[i]);
		}
	    }
	}
	for (com.jansensystems.oracledmpparser.DMPRow r : rows) {
	    for (com.jansensystems.oracledmpparser.DMPItem l : r.items) {
		System.out.print(String.format("%02x", l.noOfbytes) + ", " + l.bytes.stream().map(x -> String.format("%02x", x)).collect(Collectors.joining(", ")));
		if (l.itemType == DMPItemType.NULL) {
		    System.out.print(" -> NULL");
		} else if (l.bytes.size() > 1 && ((l.bytes.get(0) & 0xff) >= 0xc0 && (l.bytes.get(0) & 0xff) <= 0xcf)) {
		    // number
		    l.itemType = DMPItemType.NUMBER;
		    System.out.print(" -> ");
		    int intPart = (l.bytes.get(0) & 0xff) - 0xc0;
		    var sl = l.bytes.subList(1, l.bytes.size());
		    String v = "";
		    if (intPart > 0) {
			while (sl.size() < intPart) sl.add(Byte.valueOf((byte)1));  // TODO: correct?
			var ip = sl.subList(0, intPart);
			v = ip.stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining());

			if (sl.size() > intPart) {
			    var dp = sl.subList(intPart, sl.size());
			    v+= "." + dp.stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			}

		    } else {
			v = "0." + sl.stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining());
		    }
		    l.stringValue = v;
		    try {
			l.numberValue = Double.valueOf(v);
		    } catch (Exception ex) {
			ex.printStackTrace();
		    }
		    System.out.print(v);
		} else if (l.bytes.size() > 1 && ((l.bytes.get(0) & 0xff) == 0x80)) {
		    System.out.print(" -> 0");
		    l.itemType = DMPItemType.NUMBER;
		    l.numberValue = 0d;
		} else {
		    l.itemType = DMPItemType.STRING;
		    // string?
		    if (l.bytes.size() > 1) {
			System.out.print(" -> ");
			var sl = l.bytes.subList(1, l.bytes.size());
			var b1 = new byte[sl.size()];
			for (int i = 0;i<sl.size();i++) b1[i] = sl.get(i);
			String v = new String(b1);
			System.out.print(v);
			l.stringValue = v;
		    }
		}
		System.out.println();
	    }
	}
	System.out.println();
	return rows;
    }
}

package com.jansensystems.oracledmpparser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Matthias Jansen / Jansen-Systems
 */
public class DMPParser {
    private boolean afterInsertStatement = false;
    private String currentTable = null;
    private DMPTable currentTableObj = null;
    private List<DMPTable> tables = new ArrayList<>();
    private boolean finished = false;
    private boolean debugToStdout = false;
    
    private static SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter timestampFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss") // .parseLenient()
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    
    public void reset() {
	afterInsertStatement = false;
	finished = false;
	tables.clear();
	currentTable = null;
	currentTableObj = null;
    }
    
    public Stream<DMPTable> parseFileStream(InputStream in) throws IOException {
	return parseFileStream(in, x -> true);
    }
    
    public Stream<DMPTable> parseFileStream(InputStream in, List<String> tableNames) throws IOException {
	List<String> tableNamesLower = tableNames.stream().map(x -> x.toLowerCase()).toList();
	return parseFileStream(in, x -> tableNamesLower.contains(x.toLowerCase()));
    }
    
    public Stream<DMPTable> parseFileStream(InputStream in, Function<String, Boolean> filter) throws IOException {
	if (in == null) {
	    throw new IllegalArgumentException("The input stream must not be NULL");
	}
	final Function<String, Boolean> filter1 = (filter == null ? x -> true : filter);
	// if (filter == null) filter = x -> true;
	reset();
	Stream<DMPTable> iterated = Stream.iterate(null, s -> !finished, s -> {
	    // return null;
	    tables.clear();
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
				parseLine(temp.toByteArray(), filter1);
				return tables.get(0);
				// temp = new ByteArrayOutputStream();
			    } else {
				temp.write(b);
				// nothing else to do, just add more bytes
			    }
			} else {
			    // newline
			    temp.close();
			    parseLine(temp.toByteArray(), filter1);
			    temp = new ByteArrayOutputStream();
			}
		    } else {
			temp.write(b);
			lastBytes.add(b);
			if (lastBytes.size() > 4) lastBytes.poll();
		    }
		}
		parseLine(temp.toByteArray(), filter1);
		finished = true;
	    } catch (Exception ex) {
		// TODO
		throw new RuntimeException(ex);
		// ex.printStackTrace();
	    } finally {
		try {
		    temp.close();
		} catch (Exception ex) {}
	    }
	    return null;
	});
	
	return iterated.skip(1);
    }
    
    public List<DMPTable> parseFile(InputStream in, List<String> tableNames) throws IOException {
	List<String> tableNamesLower = tableNames.stream().map(x -> x.toLowerCase()).toList();
	return parseFile(in, x -> tableNamesLower.contains(x.toLowerCase()));
    }
    
    public List<DMPTable> parseFile(InputStream in) throws IOException {
	return parseFile(in, x -> true);
    }
    
    public List<DMPTable> parseFile(InputStream in, Function<String, Boolean> filter) throws IOException {
//	reset();
//	ByteArrayOutputStream temp = new ByteArrayOutputStream();
//	try {
//	    int b = 0;
//	    Queue<Integer> lastBytes = new LinkedList<>();
//	    while ((b = in.read()) >= 0) {
//		if (b == 0x0a) {
//		    if (afterInsertStatement) {
//			if (lastBytesEndOfInsertData(lastBytes)) {
//			    temp.write(b);
//			    temp.close();
//			    parseLine(temp.toByteArray(), filter);
//			    temp = new ByteArrayOutputStream();
//			} else {
//			    temp.write(b);
//			    // nothing else to do, just add more bytes
//			}
//		    } else {
//			// newline
//			temp.close();
//			parseLine(temp.toByteArray(), filter);
//			temp = new ByteArrayOutputStream();
//		    }
//		} else {
//		    temp.write(b);
//		    lastBytes.add(b);
//		    if (lastBytes.size() > 4) lastBytes.poll();
//		}
//	    }
//	    parseLine(temp.toByteArray(), filter);
//	} finally {
//	    temp.close();
//	}
//	return tables;
	return parseFileStream(in, filter).toList();
    }
    
    private boolean lastBytesEndOfInsertData(Queue<Integer> lastBytes) {
	if (lastBytes.size() < 4) return false;
	Integer[] bytes = (Integer[]) lastBytes.toArray(new Integer[4]);
	int i = 0;
	// System.out.println("LastBytes: " + lastBytes.stream().map(x -> String.format("%02x", x)).collect(Collectors.joining(", ")));
	return bytes[i] == 0 && bytes[i+1] == 0 && (bytes[i+2] & 0xff) == 0xff && (bytes[i+3] & 0xff) == 0xff;
    }
    
    private void parseLine(byte[] bytes, Function<String, Boolean> filter) {
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
	    if (filter.apply(currentTable)) {
		currentTableObj = new DMPTable();
		tables.add(currentTableObj);
		currentTableObj.tableName = currentTable;
		currentTableObj.createTableSQL = testString;
	    } else {
		currentTable = null;
	    }
	} else if (testString.startsWith("INSERT INTO ")) {
	    if (currentTable != null)
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
	if (debugToStdout) {
	    String bytes_hex =  byteArrayToString(bytes);
	    // System.out.println(bytes_hex);
	}
	if (bytes.length == 0) return rows;
	int fieldCount = (bytes[0] & 0xff);
	int skipBytes = fieldCount*4;
	for (int i=bytes.length-1;i>0;i--) {
	    if (i > 4 && bytes[i] == 0 && bytes[i-1] == 0 && bytes[i-2] == 0 && bytes[i-3] == 0) {
		skipBytes = i-4;
		break;
	    }
	}
	if (debugToStdout) {
	    System.out.println("Skipping " + skipBytes + " bytes");
	}
	
	DMPItemType[] columnTypes = new DMPItemType[fieldCount];
	int addSkip = 0;
	for (int i=0;i<fieldCount;i++) {
	    int fc = (bytes[i*4+2 + addSkip] & 0xff);
	    columnTypes[i] = switch (fc) {
		case 1 -> DMPItemType.STRING;
		case 2 -> DMPItemType.NUMBER;
		case 12 -> DMPItemType.DATE;
		case 180 -> DMPItemType.TIMESTAMP;
		case 113-> DMPItemType.BLOB;
		default -> null;
	    };
	    if (columnTypes[i] != null) {
		switch (columnTypes[i]) {
		    case STRING -> addSkip+=4;
		}
	    } else {
		if (debugToStdout) {
		    System.out.println("Missing type for code " + fc);
		}
	    }
	}
	
	// skip field defs
	for (int i=skipBytes;i<bytes.length;i++) {
	    if (bytes[i] == 0) {
		nullCount++;
	    } else {
		nullCount = 0;
	    }
	    if (nullCount == 4) { 
		if (i > 0 && bytes.length > i+4 && bytes[i] == 0 && bytes[i+1] == 0 && (bytes[i+2] & 0xff) == 0xff && (bytes[i+3] & 0xff) == 0xff && (bytes[i+4] & 0xff) == 0x0a) {
		    // zero rows
		    break;
		} else {
		    hasStarted = true;
		}
	    }
	    else if (hasStarted) {
		if (bytes.length > (i+1) && ((bytes[i] & 0xff) == 0xfe) && (bytes[i+1] & 0xff) == 0xff) {
		    // NULL value
		    cur = new DMPItem();
		    currentRow.items.add(cur);
		    cur.itemType = DMPItemType.NULL;
		    cur.noOfbytes = 2;
		    cur.bytes.add(bytes[i]);
		    cur.bytes.add(bytes[i+1]);
		    i++;
		} else if (i > 0 && bytes.length > i+4 && bytes[i] == 0 && bytes[i+1] == 0 && (bytes[i+2] & 0xff) == 0xff && (bytes[i+3] & 0xff) == 0xff) {
		    // the end
		    hasStarted = false;
		} else if (i > 0 && bytes[i] == 0 && bytes[i - 1] == 0) {
		    // we have a new row
		    currentRow = new DMPRow();
		    rows.add(currentRow);
//		    cur = new DMPItem();
//		    currentRow.items.add(cur);
		} else if (takeByteCount > 0) {
		    cur.bytes.add(bytes[i]);
		    takeByteCount--;
		    if (takeByteCount == 0) {
			// end
		    }
		} else if (bytes.length > (i+1) && ((bytes[i] & 0xff) > 0) && (bytes[i+1] & 0xff) == 0) {
		    cur = new DMPItem();
		    currentRow.items.add(cur);

		    takeByteCount = (bytes[i] & 0xff);
		    cur.noOfbytes = takeByteCount;
		    // System.out.println("takeByteCount = " + takeByteCount);
		    i++;
		} else if (bytes[i] == 0) {
//		    cur = new DMPItem();
//		    currentRow.items.add(cur);
		} else {
		    cur.bytes.add(bytes[i]);
		}
	    }
	}
	int row = 0;
	for (com.jansensystems.oracledmpparser.DMPRow r : rows) {
	    int col = 0;
	    for (com.jansensystems.oracledmpparser.DMPItem l : r.items) {
		if (debugToStdout) System.out.print(String.format("%02x", l.noOfbytes) + ", " + l.bytes.stream().map(x -> String.format("%02x", x)).collect(Collectors.joining(", ")));
		if (debugToStdout) System.out.print(" -> ");
		if (l.itemType == DMPItemType.NULL) {
		    if (debugToStdout) System.out.print("NULL");
		// } else if (l.bytes.size() > 1 && ((l.bytes.get(0) & 0xff) >= 0xc0 && (l.bytes.get(0) & 0xff) <= 0xcf)) {
		} else if (columnTypes[col] == DMPItemType.NUMBER && l.bytes.size() > 0 && ((l.bytes.get(0) & 0xff) == 0x80)) {
		    if (debugToStdout) System.out.print(" -> 0");
		    l.itemType = DMPItemType.NUMBER;
		    l.numberValue = 0d;
		} else if (columnTypes[col] == DMPItemType.NUMBER) {
		    // number
		    String v = "";
		    l.itemType = DMPItemType.NUMBER;
		    if ((l.bytes.get(l.bytes.size()-1) & 0xff) == 0x66) {
			// negative numbers
			var sl = l.bytes.subList(1, l.bytes.size()-1);
			int intPart = -1 * (int)((l.bytes.get(0) & 0xff) - 0x3f);
			if (intPart > 0) {
			    while (sl.size() < intPart) sl.add(Byte.valueOf((byte)0x63));  // TODO: correct?
			    var ip = sl.subList(0, intPart);
			    v = "-" + ip.stream().map(x -> -1*(x - 101)).map(x -> String.format("%02d", x)).collect(Collectors.joining());

			    if (sl.size() > intPart) {
				var dp = sl.subList(intPart, sl.size());
				v+= "." + dp.stream().map(x -> -1*(x - 101)).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			    }

			} else {
			    v = "-0." + ("00".repeat(-1 * intPart)) + sl.stream().map(x -> -1*(x - 101)).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			}
		    } else {
			int intPart = (l.bytes.get(0) & 0xff) - 0xc0;
			var sl = l.bytes.subList(1, l.bytes.size());
			if (intPart > 0) {
			    while (sl.size() < intPart) sl.add(Byte.valueOf((byte)1));  // TODO: correct?
			    var ip = sl.subList(0, intPart);
			    v = ip.stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining());

			    if (sl.size() > intPart) {
				var dp = sl.subList(intPart, sl.size());
				v+= "." + dp.stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			    }

			} else {
			    v = "0." + ("00".repeat(-1 * intPart)) + sl.stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			}
		    }
		    l.stringValue = v;
		    try {
			if (!v.isBlank()) {
			    l.numberValue = Double.valueOf(v);
			}
		    } catch (Exception ex) {
			ex.printStackTrace();
		    }
		    if (debugToStdout) System.out.print(v);
		} else if (columnTypes[col] == DMPItemType.STRING) {
		    l.itemType = DMPItemType.STRING;
		    // string?
		    if (l.bytes.size() > 1) {
			var sl = l.bytes.subList(0, l.bytes.size());
			var b1 = new byte[sl.size()];
			for (int i = 0;i<sl.size();i++) b1[i] = sl.get(i);
			String v = new String(b1);
			if (debugToStdout) System.out.print(v);
			l.stringValue = v;
		    }
		} else if (columnTypes[col] == DMPItemType.DATE) {
		    l.itemType = DMPItemType.DATE;
		    if (l.noOfbytes == 7) {
			String v = "";
			v = l.bytes.subList(0, 2).stream().map(x -> x - 100).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			v += "-" + l.bytes.subList(2, 4).stream().map(x -> x).map(x -> String.format("%02d", x)).collect(Collectors.joining("-"));
			v += " " + l.bytes.subList(4, 7).stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining(":"));
			
			l.stringValue = v;
			try {
			    l.dateValue = dateParser.parse(v);
			} catch (ParseException ex) {
			    if (debugToStdout) ex.printStackTrace();
			}
			if (debugToStdout) System.out.print(v);
		    } else {
			// should always be 7 bytes
		    }
		} else if (columnTypes[col] == DMPItemType.TIMESTAMP) {
		    l.itemType = DMPItemType.TIMESTAMP;
		    if (l.noOfbytes == 7 || l.noOfbytes == 11) {
			String v = "";
			v = l.bytes.subList(0, 2).stream().map(x -> x - 100).map(x -> String.format("%02d", x)).collect(Collectors.joining());
			v += "-" + l.bytes.subList(2, 4).stream().map(x -> x).map(x -> String.format("%02d", x)).collect(Collectors.joining("-"));
			v += " " + l.bytes.subList(4, 7).stream().map(x -> x-1).map(x -> String.format("%02d", x)).collect(Collectors.joining(":"));
			
			if (l.noOfbytes == 11) {
			    ByteBuffer bb = ByteBuffer.wrap(getByteSubArray(l.bytes, 7, 4));
			    int bv = bb.getInt();
			    v+="." + String.format("%09d", bv);	// TODO: can be less than 9 digits configured
			}
			
			l.stringValue = v;
			l.timestampValue = Timestamp.valueOf(v);
			if (debugToStdout) System.out.print(v);
		    } else {
			// should always be 7 bytes
		    }
		}
		if (debugToStdout) System.out.println();
		col++;
	    }
	    row++;
	    if (debugToStdout) System.out.println();
	}
	if (debugToStdout) System.out.println();
	return rows;
    }
    
    public static byte[] getByteSubArray(List<Byte> bytes, int start, int count) {
	byte[] ret = new byte[count];
	for (int i=0;i<count;i++) {
	    ret[i] = bytes.get(start + i);
	}
	return ret;
    }
    
    public static String byteArrayToString(byte[] bytes) {
	List<String> sb = new ArrayList<>();
	for (byte b : bytes) {
	    sb.add(String.format("%02x", b));
	}
	return sb.stream().collect(Collectors.joining(", "));
    }

    public void setDebugToStdout(boolean debugToStdout) {
	this.debugToStdout = debugToStdout;
    }
}

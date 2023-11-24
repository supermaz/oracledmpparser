
package com.jansensystems.oracledmpparser;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author Matthias Jansen / Jansen-Systems
 */
public class DMPItem {

    public int noOfbytes = 0;
    public List<Byte> bytes = new ArrayList<>();
    public DMPItemType itemType;
    public String stringValue = null;
    public Double numberValue = null;
    public Date dateValue = null;
    public Timestamp timestampValue = null;
    
    public String toString() {
	return switch (itemType) {
	    case BLOB -> bytes.stream().map(x -> String.format("%02x", x)).collect(Collectors.joining(", "));
	    case DATE -> dateValue != null ? dateValue.toString() : null;
	    case NULL -> null;
	    case NUMBER -> numberValue.toString();
	    case STRING -> stringValue;
	    case TIMESTAMP -> timestampValue.toString();
	};
    }
}

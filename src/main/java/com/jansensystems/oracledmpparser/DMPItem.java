
package com.jansensystems.oracledmpparser;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
}

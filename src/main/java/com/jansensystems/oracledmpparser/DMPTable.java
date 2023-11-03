package com.jansensystems.oracledmpparser;

import java.util.List;

/**
 *
 * @author Matthias Jansen / Jansen-Systems
 */
public class DMPTable {
    public String tableName = null;
    public String createTableSQL = null;
    public List<DMPRow> dataRows = null;

    @Override
    public String toString() {
	return "DMPTable{" + "tableName=" + tableName + ", dataRows=" + dataRows.size() + '}';
    }
    
}

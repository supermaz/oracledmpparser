
package com.jansensystems.oracledmpparser;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Matthias Jansen / Jansen-Systems
 */
class DMPItem {

    public int noOfbytes = 0;
    public List<Byte> bytes = new ArrayList<>();
    public DMPItemType itemType;
    public String stringValue = null;
    public Double numberValue = null;

}

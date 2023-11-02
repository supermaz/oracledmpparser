# oracledmpparser
Java library to parse oracle database dmp files created with exp command

## Example usage:


    try {	    
	var f = new File("example.dmp");
	DMPParser parser = new DMPParser();
	List<DMPTable> ret = parser.parseFile(new FileInputStream(f));
    } catch (Exception ex) {
	ex.printStackTrace();
    }
# oracledmpparser
Java library to parse oracle database dmp files created with exp command

## Maven config

    <dependency>
	<groupId>com.jansen-systems.oss</groupId>
	<artifactId>oracledmpparser</artifactId>
	<version>0.0.5</version>
    </dependency>

## Example usage:


    try {	    
	var f = new File("example.dmp");
	DMPParser parser = new DMPParser();
	
	try (var fin = new FileInputStream(f)) {
	    parser.parseFileStream(fin).forEach(System.out::println);
	}

	System.out.println("----------------------------------------------------------");

	try (var fin = new FileInputStream(f)) {
	    parser.parseFileStream(fin, List.of("TABLE1", "TABLE2")).forEach(System.out::println);
	}

	System.out.println("----------------------------------------------------------");

	try (var fin = new FileInputStream(f)) {
	    parser.parseFileStream(fin, x -> x.toLowerCase().startsWith("ta")).forEach(System.out::println);
	}
    } catch (Exception ex) {
	ex.printStackTrace();
    }
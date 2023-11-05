package com.jansensystems.oracledmpparser;


import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/*
 * Copyright 2023 maz.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 * @author maz
 */
public class ParserTest {
    
    public ParserTest() {
    }

    @org.junit.jupiter.api.BeforeAll
    public static void setUpClass() throws Exception {
    }

    @org.junit.jupiter.api.AfterAll
    public static void tearDownClass() throws Exception {
    }

    @org.junit.jupiter.api.BeforeEach
    public void setUp() throws Exception {
    }

    @org.junit.jupiter.api.AfterEach
    public void tearDown() throws Exception {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void testParser() throws IOException {
	DMPParser parser = new DMPParser();
	parser.setDebugToStdout(true);
	parser.parseFileStream(getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp")).forEach(tbl -> {
	    System.out.println(tbl);
	});
	// getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp");
    }
    
    @Test
    public void testParserResults1() throws IOException {
	DMPParser parser = new DMPParser();
	// parser.setDebugToStdout(true);
	DMPTable[] tables = parser.parseFile(getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp")).toArray(new DMPTable[0]);
	Assertions.assertEquals(tables.length, 4);
	Assertions.assertEquals(tables[0].tableName, "IGNORE");
	Assertions.assertEquals(tables[0].dataRows.size(), 0);
	Assertions.assertEquals(tables[1].tableName, "IGNORE2");
	Assertions.assertEquals(tables[1].dataRows.size(), 0);
	Assertions.assertEquals(tables[2].tableName, "TABLE1");
	Assertions.assertEquals(tables[2].dataRows.size(), 7);
	Assertions.assertEquals(tables[3].tableName, "TABLE2");
	Assertions.assertEquals(tables[3].dataRows.size(), 4);
	// getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp");
    }
    
    @Test
    public void testParserResults2() throws IOException {
	DMPParser parser = new DMPParser();
	// parser.setDebugToStdout(true);
	DMPTable[] tables = parser.parseFile(getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp"), List.of("TABLE1", "TABLE2")).toArray(new DMPTable[0]);
	Assertions.assertEquals(tables.length, 2);
	Assertions.assertEquals(tables[0].tableName, "TABLE1");
	Assertions.assertEquals(tables[0].dataRows.size(), 7);
	Assertions.assertEquals(tables[1].tableName, "TABLE2");
	Assertions.assertEquals(tables[1].dataRows.size(), 4);
	
	Date d = new Date(2022 - 1900, 00, 01, 01, 40, 11); // => '2022-01-01 01:40:11'
	Timestamp ts = Timestamp.valueOf("2022-06-02 08:45:40.346906000");
	
	Assertions.assertEquals(tables[0].dataRows.get(0).items.get(1).numberValue, 123);
	Assertions.assertNotEquals(tables[0].dataRows.get(0).items.get(1).numberValue, 123.1);
	Assertions.assertEquals(tables[0].dataRows.get(0).items.get(2).numberValue, 3.56789);
	Assertions.assertEquals(tables[0].dataRows.get(1).items.get(3).stringValue, "STRING 2 TEST sadf asdf asdf asdf asdf");
	Assertions.assertEquals(tables[0].dataRows.get(1).items.get(4).dateValue, d);
	Assertions.assertEquals(tables[0].dataRows.get(5).items.get(6).timestampValue, ts);
	// getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp");
    }
    
    @Test
    public void testParserFilter1() throws IOException {
	DMPParser parser = new DMPParser();
	// parser.setDebugToStdout(true);
	DMPTable[] tables = parser.parseFile(getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp"), List.of("TABLE1", "TABLE2")).toArray(new DMPTable[0]);
	Assertions.assertEquals(tables.length, 2);
	Assertions.assertEquals(tables[0].tableName, "TABLE1");
	Assertions.assertEquals(tables[0].dataRows.size(), 7);
	Assertions.assertEquals(tables[1].tableName, "TABLE2");
	Assertions.assertEquals(tables[1].dataRows.size(), 4);
	// getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp");
    }
    
    @Test
    public void testParserFilter2() throws IOException {
	DMPParser parser = new DMPParser();
	// parser.setDebugToStdout(true);
	DMPTable[] tables = parser.parseFile(getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp"), x -> !x.startsWith("IGNORE")).toArray(new DMPTable[0]);
	Assertions.assertEquals(tables.length, 2);
	Assertions.assertEquals(tables[0].tableName, "TABLE1");
	Assertions.assertEquals(tables[0].dataRows.size(), 7);
	Assertions.assertEquals(tables[1].tableName, "TABLE2");
	Assertions.assertEquals(tables[1].dataRows.size(), 4);
	// getClass().getClassLoader().getResourceAsStream("com/jansensystems/oracledmpparser/exptest.dmp");
    }
}

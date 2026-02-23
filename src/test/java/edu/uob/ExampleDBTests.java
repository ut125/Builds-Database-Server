package edu.uob;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Duration;

public class ExampleDBTests {

    private DBServer server;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // Random name generator - useful for testing "bare earth" queries (i.e. where tables don't previously exist)
    private String generateRandomName() {
        String randomName = "";
        for(int i=0; i<10 ;i++) randomName += (char)( 97 + (Math.random() * 25.0));
        return randomName;
    }

    private String sendCommandToServer(String command) {
        // Try to send a command to the server - this call will timeout if it takes too long (in case the server enters an infinite loop)
        return assertTimeoutPreemptively(Duration.ofMillis(1000), () -> { return server.handleCommand(command);},
        "Server took too long to respond (probably stuck in an infinite loop)");
    }

    // A basic test that creates a database, creates a table, inserts some test data, then queries it.
    // It then checks the response to see that a couple of the entries in the table are returned as expected
    @Test
    public void testBasicCreateAndQuery() {
        System.out.println("\n1st");
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        String response = sendCommandToServer("SELECT * FROM marks;");
        //System.out.println("Response: " + response);
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Simon"), "An attempt was made to add Simon to the table, but they were not returned by SELECT *");
        assertTrue(response.contains("Chris"), "An attempt was made to add Chris to the table, but they were not returned by SELECT *");
    }

    // A test to make sure that querying returns a valid ID (this test also implicitly checks the "==" condition)
    // (these IDs are used to create relations between tables, so it is essential that suitable IDs are being generated and returned !)
    @Test
    public void testQueryID() {
        System.out.println("\n2ed");
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT id FROM marks WHERE name == 'Simon';");
        // Convert multi-lined responses into just a single line
        System.out.println("Response: " + response);
        String singleLine = response.replace("\n"," ").trim();
        // Split the line on the space character
        String[] tokens = singleLine.split(" ");
        // Check that the very last token is a number (which should be the ID of the entry)
        String lastToken = tokens[tokens.length-1];
        try {
            Integer.parseInt(lastToken);
        } catch (NumberFormatException nfe) {
            fail("The last token returned by `SELECT id FROM marks WHERE name == 'Simon';` should have been an integer ID, but was " + lastToken);
        }
    }

    // A test to make sure that databases can be reopened after server restart
    @Test
    public void testTablePersistsAfterRestart() {
        System.out.println("\n3rd");
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        // Create a new server object
        server = new DBServer();
        sendCommandToServer("USE " + randomName + ";");
        String response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("Simon"), "Simon was added to a table and the server restarted - but Simon was not returned by SELECT *");
    }

    // Test to make sure that the [ERROR] tag is returned in the case of an error (and NOT the [OK] tag)
    @Test
    public void testForErrorTag() {
        System.out.println("\n4th");
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT * FROM libraryfines;");
        assertTrue(response.contains("[ERROR]"), "An attempt was made to access a non-existent table, however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An attempt was made to access a non-existent table, however an [OK] tag was returned");
    }

    @Test
    public void testSelectQueryColumnOrder() {
        System.out.println("\n5th");
        String randomName = generateRandomName();
        
        //create a database and use it
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        
        //create a table and insert some data
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Alice', 85, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Bob', 90, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Charlie', 70, FALSE);");

        //run a SELECT query and confirm the order
        String response = sendCommandToServer("SELECT name, id FROM marks;");
        System.out.println("Response: " + response);

        //check if the data is returned in order according to the result
        assertTrue(response.contains("Alice"), "Expected name 'Alice' to appear first in the results.");
        assertTrue(response.contains("Bob"), "Expected name 'Bob' to appear after 'Alice'.");
        assertTrue(response.contains("Charlie"), "Expected name 'Charlie' to appear last in the results.");
        
        //make sure the 'id' column is after the 'name' column
        assertTrue(response.indexOf("Alice") < response.indexOf("1"), "The 'id' column should appear after 'name' column.");
        assertTrue(response.indexOf("Bob") < response.indexOf("2"), "The 'id' column should appear after 'name' column.");
        assertTrue(response.indexOf("Charlie") < response.indexOf("3"), "The 'id' column should appear after 'name' column.");

        assertFalse(response.contains("[ERROR]"), "Unexpected error returned in response.");
        assertTrue(response.contains("[OK]"), "Expected [OK] tag in response.");
    }



    @Test
    void testUpdateCommand() {
        System.out.println("\n6th");
        sendCommandToServer("CREATE DATABASE testDB;");
        sendCommandToServer("USE testDB;");
        sendCommandToServer("CREATE TABLE users (name, mark, pass);");
        sendCommandToServer("INSERT INTO users VALUES ('Alice', 85, TRUE);");
        sendCommandToServer("INSERT INTO users VALUES ('Bob', 90, TRUE);");
    
        //UPDATE
        String response = sendCommandToServer("UPDATE users SET mark = 35 WHERE name == 'Alice';");
        assertFalse(response.toLowerCase().contains("error"), "UPDATE command succeed.");
        String queryResult = sendCommandToServer("SELECT mark FROM users WHERE name == 'Alice';");
        assertTrue(queryResult.contains("35"), "Alice age is updated to 35");
    }

    @Test
    void testDeleteCommand() {
        System.out.println("\n7th");
        System.out.println("Testing DELETE command");

        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        
        // create marks
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");

        // check 
        String beforeDeleteResponse = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(beforeDeleteResponse.contains("Simon"), "Simon is in table");
        assertTrue(beforeDeleteResponse.contains("Sion"), "Sion is in table");
        assertTrue(beforeDeleteResponse.contains("Rob"), "Rob is in table");
        assertTrue(beforeDeleteResponse.contains("Chris"), "Chris is in table");

        //DELETE
        String deleteResponse = sendCommandToServer("DELETE FROM marks WHERE mark < 40;");
        assertTrue(deleteResponse.contains("[OK]"), "DELETE command succeed");
        String afterDeleteResponse = sendCommandToServer("SELECT * FROM marks;");
        
        assertTrue(afterDeleteResponse.contains("Simon"), "Simon is in table");
        assertTrue(afterDeleteResponse.contains("Sion"), "Sion is in table");
        
        assertFalse(afterDeleteResponse.contains("Rob"), "Rob is delete");
        assertFalse(afterDeleteResponse.contains("Chris"), "Chris is delete");
    }

    @Test
    public void testDropDatabase() {
        System.out.println("\n8th");
        System.out.println("Running testDropDatabase");
        
        String dbName = generateRandomName();
        String createResponse = sendCommandToServer("CREATE DATABASE " + dbName + ";");
        System.out.println("Create DB Response: " + createResponse);
        assertTrue(createResponse.contains("[OK]"), "Failed to create database");

        String useResponse = sendCommandToServer("USE " + dbName + ";");
        System.out.println("Use DB Response: " + useResponse);
        assertTrue(useResponse.contains("[OK]"), "Failed to use database");

        //drop
        String dropResponse = sendCommandToServer("DROP DATABASE " + dbName + ";");
        System.out.println("Drop DB Response: " + dropResponse);
        assertTrue(dropResponse.contains("[OK]"), "Failed to drop database");

        //try again
        String useAfterDropResponse = sendCommandToServer("USE " + dbName + ";");
        System.out.println("Use After Drop Response: " + useAfterDropResponse);
        assertTrue(useAfterDropResponse.contains("[ERROR]"), "Dropped database should not be accessible");
    }

    @Test
    public void testDropTable() {
        System.out.println("\n9th");
        System.out.println("Running testDropTable");
        String dbName = generateRandomName();
        String tableName = "students";

        String createDbResponse = sendCommandToServer("CREATE DATABASE " + dbName + ";");
        System.out.println("Create DB Response: " + createDbResponse);
        assertTrue(createDbResponse.contains("[OK]"), "Failed to create database");

        String useDbResponse = sendCommandToServer("USE " + dbName + ";");
        System.out.println("Use DB Response: " + useDbResponse);
        assertTrue(useDbResponse.contains("[OK]"), "Failed to use database");

        String createTableResponse = sendCommandToServer("CREATE TABLE " + tableName + " (id, name, age);");
        System.out.println("Create Table Response: " + createTableResponse);
        assertTrue(createTableResponse.contains("[OK]"), "Failed to create table");

        String dropTableResponse = sendCommandToServer("DROP TABLE " + tableName + ";");
        System.out.println("Drop Table Response: " + dropTableResponse);
        assertTrue(dropTableResponse.contains("[OK]"), "Failed to drop table");

        String dropAgainResponse = sendCommandToServer("DROP TABLE " + tableName + ";");
        System.out.println("Drop Again Response: " + dropAgainResponse);
        assertTrue(dropAgainResponse.contains("[ERROR]"), "Table should not exist after being dropped");
    }

    @Test
    public void testAlterTableAddColumn() {
        System.out.println("\n10th");
        System.out.println("Running testAlterTableAddColumn");

        String dbName = generateRandomName();
        String tableName = "students";
        String newColumn = "grade";

        String createDbResponse = sendCommandToServer("CREATE DATABASE " + dbName + ";");
        System.out.println("Create DB Response: " + createDbResponse);
        assertTrue(createDbResponse.contains("[OK]"), "Failed to create database");

        String useDbResponse = sendCommandToServer("USE " + dbName + ";");
        System.out.println("Use DB Response: " + useDbResponse);
        assertTrue(useDbResponse.contains("[OK]"), "Failed to use database");

        String createTableResponse = sendCommandToServer("CREATE TABLE " + tableName + " (name, age);");
        System.out.println("Create Table Response: " + createTableResponse);
        assertTrue(createTableResponse.contains("[OK]"), "Failed to create table");

        String alterTableResponse = sendCommandToServer("ALTER TABLE " + tableName + " ADD " + newColumn + ";");
        System.out.println("Alter Table Response: " + alterTableResponse);
        assertTrue(alterTableResponse.contains("[OK]"), "Failed to alter table");

        String duplicateAlterTableResponse = sendCommandToServer("ALTER TABLE " + tableName + " ADD " + newColumn + ";");
        System.out.println("Duplicate Alter Table Response: " + duplicateAlterTableResponse);
        assertTrue(duplicateAlterTableResponse.contains("[ERROR]"), "Duplicate column should not be allowed");
    }

    @Test
    public void testAlterTableDropColumn() {
        System.out.println("\n11th");
        System.out.println("Running testAlterTableDropColumn");

        String dbName = generateRandomName();
        String tableName = "students";
        String columnToDrop = "age";

        String createDbResponse = sendCommandToServer("CREATE DATABASE " + dbName + ";");
        System.out.println("Create DB Response: " + createDbResponse);
        assertTrue(createDbResponse.contains("[OK]"), "Failed to create database");

        String useDbResponse = sendCommandToServer("USE " + dbName + ";");
        System.out.println("Use DB Response: " + useDbResponse);
        assertTrue(useDbResponse.contains("[OK]"), "Failed to use database");


        String createTableResponse = sendCommandToServer("CREATE TABLE " + tableName + " (name, age);");
        System.out.println("Create Table Response: " + createTableResponse);
        assertTrue(createTableResponse.contains("[OK]"), "Failed to create table");

        String alterTableResponse = sendCommandToServer("ALTER TABLE " + tableName + " DROP " + columnToDrop + ";");
        System.out.println("Alter Table Response: " + alterTableResponse);
        assertTrue(alterTableResponse.contains("[OK]"), "Failed to drop column");

        String duplicateAlterTableResponse = sendCommandToServer("ALTER TABLE " + tableName + " DROP " + columnToDrop + ";");
        System.out.println("Duplicate Alter Table Response: " + duplicateAlterTableResponse);
        assertTrue(duplicateAlterTableResponse.contains("[ERROR]"), "Dropping non-existent column should not be allowed");
    }

    @Test
    public void testJoinTables() {
        System.out.println("\n12th");
        System.out.println("Running testJoinTables");
    
        String dbName = generateRandomName();
        String table1 = "Employees";
        String table2 = "Departments";
    
        String createDbResponse = sendCommandToServer("CREATE DATABASE " + dbName + ";");
        System.out.println("Create DB Response: " + createDbResponse);
        assertTrue(createDbResponse.contains("[OK]"), "Failed to create database");
    
        String useDbResponse = sendCommandToServer("USE " + dbName + ";");
        System.out.println("Use DB Response: " + useDbResponse);
        assertTrue(useDbResponse.contains("[OK]"), "Failed to use database");
    
        String createTable1Response = sendCommandToServer("CREATE TABLE " + table1 + " (name department_id);");
        System.out.println("Create Table1 Response: " + createTable1Response);
        assertTrue(createTable1Response.contains("[OK]"), "Failed to create Employees table");
    
        sendCommandToServer("INSERT INTO " + table1 + " VALUES (Alice 101);");
        sendCommandToServer("INSERT INTO " + table1 + " VALUES (Bob 100);");
        sendCommandToServer("INSERT INTO " + table1 + " VALUES (Charlie 102);");
    
        String createTable2Response = sendCommandToServer("CREATE TABLE " + table2 + " (d_id department_name);");
        System.out.println("Create Table2 Response: " + createTable2Response);
        assertTrue(createTable2Response.contains("[OK]"), "Failed to create Departments table");
    
        sendCommandToServer("INSERT INTO " + table2 + " VALUES (100 IT);");
        sendCommandToServer("INSERT INTO " + table2 + " VALUES (101 HR);");
        sendCommandToServer("INSERT INTO " + table2 + " VALUES (102 Finance);");
    
        String joinResponse = sendCommandToServer("JOIN " + table1 + " AND " + table2 + " ON department_id AND d_id;");
        System.out.println("Join Response: " + joinResponse);
    
        assertTrue(joinResponse.contains("Alice") && joinResponse.contains("HR"), "Join result incorrect");
        assertTrue(joinResponse.contains("Bob") && joinResponse.contains("IT"), "Join result incorrect");
        assertTrue(joinResponse.contains("Charlie") && joinResponse.contains("Finance"), "Join result incorrect");
    
    }

    //WHERE several conditions
    @Test
    public void testSelectWithMultipleWhereConditions() {
        System.out.println("\n13rd");
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");

        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
    
        String response = sendCommandToServer("SELECT * FROM marks WHERE mark > 50 AND pass = TRUE;");
        System.out.println("Response: " + response);

        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
    
        assertTrue(response.contains("Simon"), "Simon should be included in the result since their mark is > 50 and pass is TRUE");
        assertTrue(response.contains("Sion"), "Sion should be included in the result since their mark is > 50 and pass is TRUE");
        
        assertFalse(response.contains("Chris"), "Chris should not be included in the result since their mark is <= 50");
        assertFalse(response.contains("Rob"), "Rob should not be included in the result since their mark is <= 50");
    }
    


}

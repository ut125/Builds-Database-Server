package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Handling {

    private String storageFolderPath;
    private String currentDatabase = null;

    // 傳遞 storageFolderPath
    public Handling(String storageFolderPath) {
        this.storageFolderPath = storageFolderPath;
    }

    //use
    public String handleUseCommand (SQLPrasing.UseCommand useCommand) {
        //System.out.println("\nTry to switch to the database: " + useCommand.DatabaseName);
        String dbName = useCommand.DatabaseName;
        File databaseFolder = new File(storageFolderPath, useCommand.DatabaseName);
        
        // check if database already exists
        if (!databaseFolder.exists()) {
            return "[ERROR] Couldn't find " + dbName;
        }

        currentDatabase = dbName;
        return "\n[OK] Database changed to " + dbName;
    }

    //create
    public String handleCreateDatabaseCommand(SQLPrasing.CreateDatabaseCommand createCommand) {
        //System.out.println("\nTry to create a database: " + createCommand.DatabaseName);
        
        // check if database already exists
        String dbName = createCommand.DatabaseName;
        File databaseFolder = new File(storageFolderPath, dbName);
        if (databaseFolder.exists()) {
            return "[ERROR] Database already exists";
        }
        // try to create a folder
        if (databaseFolder.mkdir()) {
            return "[OK] create database folder: " + createCommand.DatabaseName;
        } else {
            return "[ERROR] Failed to create database folder.";
        }
    }

    public String handleCreateTableCommand(SQLPrasing.CreateTableCommand createCommand) {
        
        //System.out.println("\nTry to create a table: " + createCommand.TableName);
        
        //check to see if there is a database
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        } 

        // check the table already exists
        String tableFileName = createCommand.TableName + ".tab";
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableFileName);
        File tableFile = tablePath.toFile();

        if (tableFile.exists()) {
            return "[ERROR] Table already exists: " + createCommand.TableName;
        } 

        try {
            // try to create a table
            BufferedWriter table = new BufferedWriter(new FileWriter(tableFile));
            
            // write in the first column "ID"
            table.write ("id");
            for (String column : createCommand.ColumnsData) {
                // ignore duplicate id
                if (column.equalsIgnoreCase("id")) {
                    continue;
                }
                //use tab to separate column
                table.write("\t" + column);
            }
            table.newLine();
            table.close();

        } catch (IOException ex) {
            return "[ERROR] Failed to create table file.";
        }
        return "\n[OK] Successful creation table: " + createCommand.TableName;
    }

    //insert
    public String handleInsertCommand(SQLPrasing.InsertCommand insertintoCommand) {
        
        //System.out.println("\nTry to insert something");
        
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        } 

        String tableFileName = insertintoCommand.TableName + ".tab";
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableFileName);
        File tableFile = tablePath.toFile();


        if (!tableFile.exists()) {
            return "[ERROR] Table " + insertintoCommand.TableName + " doesn't exist in database " + currentDatabase + ".";
        }   

        //use to store exist ID
        Set<Integer> idCount = new HashSet<>();
        //save all rows + header
        List<String> existRows = new ArrayList<>();
    
        // read exist data
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            //System.out.println("Reading" + insertintoCommand.TableName + " content now ");
            
            String headtitle = reader.readLine();
            //save the title -> write it back later
            existRows.add(headtitle);
    
            String line;
            while ((line = reader.readLine()) != null) {
                existRows.add(line);
                String[] parts = line.split("\\t");
                
                if (parts.length > 0) {
                    try {
                        //read id
                        int existId = Integer.parseInt(parts[0].trim());
                        idCount.add(existId);
                    } catch (NumberFormatException ex) {
                        return "[ERROR] Invalid id in table file.";
                    }
                }
            }
        } catch (IOException ex) {
            return "[ERROR] Can't read table file: "+ insertintoCommand.TableName;
        }
    
        //find a new id
        int id = 1;
        while (idCount.contains(id)) {
            id++;
        }
        //System.out.println("new id: " + id);
    
        // write data- append mode(true)
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true))) {
            String Row = id + "\t" + String.join("\t", insertintoCommand.ColumnsData);
            writer.write(Row);
            writer.newLine();
        } catch (IOException ex) {
            return "[ERROR] Can't write to table.";
        }
        return "\n[OK] Inserted successfully.";
    }

    //select
    public String handleSelectTableCommand(SQLPrasing.SelectTableCommand selectTableCommand) {
        //System.out.println("\nTry to select table");
        
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        } 

        String tableFileName = selectTableCommand.TableName + ".tab";
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableFileName);
        File tableFile = tablePath.toFile();

        if (!tableFile.exists()) {
            return "[ERROR] Table " + selectTableCommand.TableName + " doesn't exist in database " + currentDatabase + ".";
        }   
        
        StringBuilder resultData = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String line;
            
            // read the first line
            String[] columnNames = null;
            if ((line = reader.readLine()) != null) {
                columnNames = line.split("\\s+");
                // add header to result
                resultData.append(String.join("\t", columnNames)).append("\n");
            }
    
            // if has WHERE
            while ((line = reader.readLine()) != null) {
                if (selectTableCommand.WhereCondition == null || WhereCondition(line, columnNames, selectTableCommand.WhereCondition)) {
                    String[] values = line.split("\\s+");
                    resultData.append(String.join("\t", values)).append("\n");
                }
            }
            
        } catch (IOException ex) {
            return "[ERROR] Can't read table file.";
        }
    
        //System.out.println("SelectTable.Final result: " + resultData.toString());
        return "\n[OK]\n" + resultData.toString();
    }

    public String handleSelectDataCommand(SQLPrasing.SelectDataCommand selectDataCommand) {
        //System.out.println("\nTry to select data");

        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }

        Path tablePath = Paths.get(storageFolderPath, currentDatabase, selectDataCommand.TableName + ".tab");
        File tableFile = tablePath.toFile();

        if (!tableFile.exists()) {
            return "[ERROR] Table doesn't exist.";
        }

        StringBuilder resultData = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return "[ERROR] Table is empty.";
            }
    
            //become columnNames array
            String[] columnNames = headerLine.split("\\s+");
            //convert columnNames array to Set<String> check
            Set<String> columnSet = new HashSet<>(Arrays.asList(columnNames));
    
            //check
            String[] chooseColumns = selectDataCommand.ColumnsData;
            for (String column : chooseColumns) {
                if (!columnSet.contains(column)) {
                    return "[ERROR] Column '" + column + "' doesn't exist in table '" + selectDataCommand.TableName + "'.";
                }
            }
    
            //title
            resultData.append(ChooseColumns(headerLine, columnNames, chooseColumns)).append("\n");
    
            //where
            String line;
            while ((line = reader.readLine()) != null) {
                if (WhereCondition(line, columnNames, selectDataCommand.WhereCondition)) {
                    resultData.append(ChooseColumns(line, columnNames, chooseColumns)).append("\n");
                }
            }
    
        } catch (IOException ex) {
            return "[ERROR] Can't read table file.";
        }

        //System.out.println("SelectData.Final result: " + resultData.toString());
        return "\n[OK]\n" + resultData.toString();
    }
 
    //client choose some columns from SelectDataCommand
    private String ChooseColumns(String line, String[] columnNames, String[] chooseColumns) {
        //sepereate line
        String[] values = line.trim().split("\\s+");

        //store final result
        String result = "";

        //deal with each and every one
        for (String achooseColumn : chooseColumns) {
            //Find the correct header column
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equalsIgnoreCase(achooseColumn)) { 
                    //add in result
                    if (!result.equals("")) {
                        result += "\t";
                    }
                    
                    //check index doesn't over
                    if (i < values.length) {
                        result += values[i];
                    } else {
                        // 如果沒有該值，就填 NULL
                        result += "NULL";
                    }
                    break;
                }
            }
        }
        //System.out.println("\nChooseColumns result:\n " + result.toString());
        return result;
    }

    //where
    //use OR seperate sentence, then check AND first, check OR later 
    private boolean WhereCondition(String row, String[] columnNames, String whereCondition) {
        //System.out.println("\n !!!! Evaluating WHERE!!!");
        //System.out.println("row: " + row);
        //System.out.println("columnNames: " + Arrays.toString(columnNames));
        
        //no WHERE condition -> return true
        if (whereCondition == null || whereCondition.trim().isEmpty()) {
            //System.out.println("No WHERE condition, returning true.");
            return true;
        }
    
        String[] values = row.split("\\s+");
        //System.out.println("values: " + Arrays.toString(values));
    
        //remove()
        whereCondition = whereCondition.trim();
        if (whereCondition.startsWith("(") && whereCondition.endsWith(")")) {
            whereCondition = whereCondition.substring(1, whereCondition.length() - 1).trim();
        }
    
        //(?i):ignore case, use OR seperate 
        String[] orConditions = whereCondition.split("(?i)\\s+OR\\s+");
        boolean orResult = false;
        
        for (String orCondition : orConditions) {
            //use AND seperate
            String[] andConditions = orCondition.split("(?i)\\s+AND\\s+");
            boolean andResult = true;
            //check each AND
            for (String andCondition : andConditions) {
                boolean compareResult = evaluateLogic(andCondition, columnNames, values);
                //andResult = andResult && andResult
                andResult &= compareResult;
                //System.out.println("check AND: " + andCondition + " -> " + compareResult);
            }
            
            //OR orResult = orResult || andResult
            orResult |= andResult;
            //System.out.println("check OR: " + orCondition + " -> " + andResult);
        }
    
        //System.out.println("\nFinal WhereCondition result: " + orResult);
        return orResult;
    }

    //seperate 3 part to compare
    private boolean evaluateLogic(String condition, String[] columnNames, String[] values) {
        
        //System.out.println("\nevaluateLogic start: " + condition);

        //seperate column operator value
        String[] conditionParts = condition.split("\\s+");
        if (conditionParts.length < 3) {
            return false;
        }
    
        String columnName = conditionParts[0];
        String operator = conditionParts[1];
        String value = conditionParts[2].replaceAll("[\"';]", "").trim();  // 去掉引号和分号
    
        //find choose column
        int columnIndex = -1;
        try {
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equalsIgnoreCase(columnName)) {
                    columnIndex = i;
                    break;
                }
            }
        } catch (Exception ex) {
            //System.out.println("[Error] search column index");
            return false;
            
        }
    
        if (columnIndex == -1 || columnIndex >= values.length) {
            //System.out.println("Column not found.");
            return false;
        }
    
        String rowValue = values[columnIndex];
        //System.out.println("Value is [" + columnName + "] is " + rowValue);
    
        //compare
        boolean result = false;
        try {
            switch (operator) {
                case "==":
                case "=": 
                    result = rowValue.equalsIgnoreCase(value);
                    break;
                case "!=":
                    result = !rowValue.equalsIgnoreCase(value);
                    break;
                case ">":
                    result = Integer.parseInt(rowValue) > Integer.parseInt(value);
                    break;
                case "<":
                    result = Integer.parseInt(rowValue) < Integer.parseInt(value);
                    break;
                case ">=":
                    result = Integer.parseInt(rowValue) >= Integer.parseInt(value);
                    break;
                case "<=":
                    result = Integer.parseInt(rowValue) <= Integer.parseInt(value);
                    break;
                case "LIKE":
                    result = rowValue.contains(value);
                    break;
                default:
                    //System.out.println("Unsupported operator [" + operator + "], returning false.");
                    result = false;
                    break;
            }
        } catch (NumberFormatException ex) {
            result = false;
        }
    
        //System.out.println("\nevaluate Logic and sent to WhereCondition: " + result);
        return result;
    }

    //update
    public String handleUpdateCommand(SQLPrasing.UpdateCommand updateCommand) {
        //System.out.println("\n///Try to update something ");

        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }

        if (updateCommand == null) {
            return "[ERROR] updateCommand is null.";
        }

        Path tablePath = Paths.get(storageFolderPath, currentDatabase, updateCommand.TableName + ".tab");
        File tableFile = tablePath.toFile();
        if (!tableFile.exists()) {
            return "[ERROR] Table doesn't exist.";
        }
    
        List<String> updatedLines = new ArrayList<>();
        boolean beUpdated = false;
    
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return "[ERROR] Table is empty.";
            }
            //keep title
            updatedLines.add(headerLine);   
            String[] columnNames = headerLine.split("\\s+");
            Map<String, String> setMap = new HashMap<>();
            
            //analyze set
            for (String setClause : updateCommand.ColumnsData) {
                String[] parts = setClause.split("=");
                if (parts.length == 2) {
                    setMap.put(parts[0].trim(), parts[1].trim());
                }
            }

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                String[] values = line.isEmpty() ? new String[0] : line.split("\\s+");
                if (values.length < columnNames.length) {
                    return "[ERROR] Data is incomplete.";
                }
                //System.out.println("handleUpdateCommand.Read row: " + line);
    
            // 直接在 `if` 裡面處理 `updateRow`，沒有封裝
            if (WhereCondition(line, columnNames, updateCommand.WhereCondition)) {
                //System.out.println("\n///Updating row: " + Arrays.toString(values));

                for (int i = 0; i < columnNames.length; i++) {
                    if (setMap.containsKey(columnNames[i])) {
                        values[i] = setMap.get(columnNames[i]);
                    }
                }

                line = String.join("\t", values);
                //System.out.println("\nupdateRow.Updated row: " + line);

                beUpdated = true;
            }
                updatedLines.add(line);
            }

        } catch (IOException ex) {
            return "[ERROR] Can't read table file.";
        }
    
        if (!beUpdated) {
            return "[OK] No matching records found.";
        }
    
        //write
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
            for (String updatedLine : updatedLines) {
                writer.write(updatedLine);
                writer.newLine();
            }
        } catch (IOException ex) {
            return "\n[ERROR] Failed to write back to table file.";
        }
    
        //System.out.println("[OK] Table updated successfully.");
        return "\n[OK] Table updated successfully.";
    }
    
    //delete
    public String handleDeleteCommand(SQLPrasing.DeleteCommand deleteCommand) {
        //System.out.println("\n try to handle Delete table: " + deleteCommand.TableName);
        
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }
    
        String tableFileName = deleteCommand.TableName + ".tab";
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableFileName);
        File tableFile = tablePath.toFile();
        
        if (!tableFile.exists()) {
            return "[ERROR] Table doesn't exist.";
        }
    
        List<String> lines;
        try {
            lines = Files.readAllLines(tablePath, StandardCharsets.UTF_8);
            //System.out.println("handleDeleteCommand.Table read successfully:\n" + lines.size());
        } catch (IOException ex) {
            return "[ERROR] Failed to read table file.";
        }
    
        if (lines.isEmpty()) {
            return "[ERROR] Table is empty.";
        }
    
        String titleline = lines.get(0);
        String[] columnNames = titleline.split("\\s+");
    
        //store line
        List<String> newLines = new ArrayList<>();
        newLines.add(titleline);
    
        int deletedCount = 0;
        for (int i = 1; i < lines.size(); i++) {
            String row = lines.get(i).trim();
            //System.out.println("handleDeleteCommand.Checking row: " + row);
    
            //WHERE check
            if (!WhereCondition(row, columnNames, deleteCommand.WhereCondition)) {
                newLines.add(row);
            } else {
                //System.out.println("handleDeleteCommand.Row delete: " + row);
                deletedCount++;
            }
        }
    
        try {
            Files.write(tablePath, newLines, StandardCharsets.UTF_8);
            //System.out.println("handleDeleteCommand.Table updated successfully. Deleted rows: " + deletedCount);
        } catch (IOException ex) {
            return "[ERROR] Failed to update table file.";
        }
    
        return "\n[OK] Deleted " + deletedCount + " rows successfully.";
    }

    //drop
    public String handleDropDatabaseCommand(SQLPrasing.DropDatabaseCommand dropDatabaseCommand) {
        //System.out.println("\n///handleDropDatabaseCommand executing for database: " + dropDatabaseCommand.DatabaseName);
    
        if (dropDatabaseCommand.DatabaseName == null || dropDatabaseCommand.DatabaseName.trim().isEmpty()) {
            return "[ERROR] Invalid database name.";
        }

        Path databasePath = Paths.get(storageFolderPath, dropDatabaseCommand.DatabaseName);
        File databaseFolder = databasePath.toFile();

        if (!databaseFolder.exists()) {
            return "[ERROR] Database doesn't exist.";
        }

        //lists all files and subfolders
        File[] files = databaseFolder.listFiles();
        if (files != null) {
            for (File file : files) {
                //whether it is a folder
                if (file.isDirectory()) {
                    //System.out.println("DropDatabase.deleting directory: " + file.getAbsolutePath());
                    File[] subFiles = file.listFiles();
                    //check whether it has another folder
                    if (subFiles != null) {
                        for (File subFile : subFiles) {
                            //System.out.println("DropDatabase.deleting file: " + subFile.getAbsolutePath());
                            subFile.delete();
                        }
                    }
                    file.delete();
                } else {
                    file.delete();
                }
            }
        }
    
        //delete empty folder
        if (databaseFolder.delete()) {
            return "[OK] Database deleted successfully.";
        } else {
            return "[ERROR] Failed to delete database folder.";
        }
    }

    public String handleDropTableCommand(SQLPrasing.DropTableCommand dropTableCommand) {
        //System.out.println("\\n///Executing DROP TABLE for: " + dropTableCommand.TableName);
    
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }
    
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, dropTableCommand.TableName + ".tab");
        File tableFile = tablePath.toFile();
    
        if (!tableFile.exists()) {
            return "[ERROR] Table " + dropTableCommand.TableName + " doesn't exist in database.";
        }
    
        if (tableFile.delete()) {
            return "[OK] Table " + dropTableCommand.TableName + " deleted successfully.";
        } else {
            return "[ERROR] Failed to delete table " + dropTableCommand.TableName + ".";
        }
    }

    //alert
    public String handleAlterTableAddCommand(SQLPrasing.AlterTableAddCommand alterTableAddCommand) {
        //System.out.println("\n///Executing Alter Table Add for table");

        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }

        Path tablePath = Paths.get(storageFolderPath, currentDatabase, alterTableAddCommand.TableName + ".tab");
        File tableFile = tablePath.toFile();

        if (!tableFile.exists()) {
            return "[ERROR] Table " + alterTableAddCommand.TableName + " doesn't exist in database.";
        }

        List<String> lines;
        try {
            lines = Files.readAllLines(tablePath, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            return "[ERROR] Failed to read table file.";
        }

        if (lines.isEmpty()) {
            return "[ERROR] Table file is empty.";
        }

        //check title
        String header = lines.get(0);
        String[] columnNames = header.split("\\s+");

        for (String column : columnNames) {
            if (column.equalsIgnoreCase(alterTableAddCommand.TitleColumnsData)) {
                return "[ERROR] Column " + alterTableAddCommand.TitleColumnsData + " already exists.";
            }
        }

        //add title column
        String newHeader = header + " " + alterTableAddCommand.TitleColumnsData;
        List<String> newLines = new ArrayList<>();
        newLines.add(newHeader);

        //give a value for this row
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            newLines.add(line + " NULL");
        }

        //update table
        try {
            Files.write(tablePath, newLines, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            return "[ERROR] Failed to update table file.";
        }

        return "[OK] Column " + alterTableAddCommand.TitleColumnsData + " added successfully.";
    }
    
    public String handleAlterTableDropCommand(SQLPrasing.AlterTableDropCommand alterTableDropCommand) {
        //System.out.println("\nExecuting Alter Table Drop");
    
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }
    
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, alterTableDropCommand.TableName + ".tab");
        File tableFile = tablePath.toFile();

        if (!tableFile.exists()) {
            return "[ERROR] Table " + alterTableDropCommand.TableName + " doesn't exist in database.";
        }
    
        List<String> lines;
        try {
            lines = Files.readAllLines(tablePath);
        } catch (IOException ex) {
            return "[ERROR] Failed to read table file.";
        }
    
        if (lines.isEmpty()) {
            return "[ERROR] Table file is empty.";
        }
    
        String titleheader = lines.get(0);
        String[] columnNames = titleheader.split("\\s+");

        int columnIndex = -1;
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equalsIgnoreCase(alterTableDropCommand.TitleColumnsData)) {
                columnIndex = i;
                break;
            }
        }
    
        if (columnIndex == -1) {
            return "[ERROR] Column " + alterTableDropCommand.TitleColumnsData + " doesn't exist.";
        }

        // can't delete id
        if (alterTableDropCommand.TitleColumnsData.equalsIgnoreCase("id")) {
            return "[ERROR] The 'id' column cannot be dropped.";
        }
    
        List<String> newLines = new ArrayList<>();
        for (String line : lines) {
            String[] rowValues = line.split("\\s+");
            StringBuilder updatedRow = new StringBuilder();

            // update each row and delete the specified column
            for (int i = 0; i < rowValues.length; i++) {
                if (i != columnIndex) {
                    if (updatedRow.length() > 0) {
                        updatedRow.append(" ");
                    }
                    updatedRow.append(rowValues[i]);
                }
            }
            newLines.add(updatedRow.toString());
        }

        try {
            Files.write(tablePath, newLines, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            return "[ERROR] Failed to update table file.";
        }
    
        return "[OK] Column " + alterTableDropCommand.TitleColumnsData + " dropped successfully.";
    }

    //join
    public String handleJoinCommand(SQLPrasing.JoinCommand joinCommand) {
        if (currentDatabase == null) {
            return "[ERROR] No database selected.";
        }

        //check first table existence
        Path OnetablePath = Paths.get(storageFolderPath, currentDatabase, joinCommand.OneTableName + ".tab");
        File OnetableFile = OnetablePath.toFile();
        if (!OnetableFile.exists()) {
            return "[ERROR] Table1 " + joinCommand.OneTableName + " doesn't exist.";
        }

        //check second table existence
        Path TwotablePath = Paths.get(storageFolderPath, currentDatabase, joinCommand.TwoTableName + ".tab");
        File TwotableFile = TwotablePath.toFile();
        if (!TwotableFile.exists()) {
            return "[ERROR] Table2 " + joinCommand.TwoTableName + " doesn't exist.";
        }

        //read both table data
        List<String> linesOne = new ArrayList<>();
        try (BufferedReader reader  = new BufferedReader(new FileReader(OnetablePath.toFile()))) {
            String line;
            while ((line = reader .readLine()) != null) {
                linesOne.add(line);
            }
        } catch (IOException ex) {
            return "[ERROR] Failed to read the first table file.";
        }

        List<String> linesTwo = new ArrayList<>();
        try (BufferedReader reader  = new BufferedReader(new FileReader(TwotablePath.toFile()))) {
            String line;
            while ((line = reader .readLine()) != null) {
                linesTwo.add(line);
            }
        } catch (IOException ex) {
            return "[ERROR] Failed to read the second table file.";
        }

        if (linesOne.isEmpty() || linesTwo.isEmpty()) {
            return "[ERROR] One of the tables is empty.";
        }

        //extract headers
        String headerOne = linesOne.get(0);
        String headerTwo = linesTwo.get(0);
        String[] columnNamesOne = headerOne.split("\\s+");
        String[] columnNamesTwo = headerTwo.split("\\s+");

        //look for the correct row
        int firstTableJoinColumn = -1;
        for (int i = 0; i < columnNamesOne.length; i++) {
            if (columnNamesOne[i].equals(joinCommand.OneTitleColumnsData)) {
                firstTableJoinColumn = i;
            }
        }
        
        int secondTableJoinColumn = -1;
        for (int i = 0; i < columnNamesTwo.length; i++) {
            if (columnNamesTwo[i].equals(joinCommand.TwoTitleColumnsData)) {
                secondTableJoinColumn = i;
            }
        }

        //if didn't find it
        if (firstTableJoinColumn == -1 || secondTableJoinColumn == -1) {
            return "[ERROR] One of the specified columns doesn't exist.";
        }

        List<String> result = new ArrayList<>();
        //merge title
        String resultHeader = headerOne + " " + String.join(" ", Arrays.copyOfRange(columnNamesTwo, 2, columnNamesTwo.length));
        result.add(resultHeader);

        //linking
        int newId = 1;
        for (int i = 1; i < linesOne.size(); i++) {
            String firstTableRow = linesOne.get(i);
            String[] firstTableRowData = firstTableRow.split("\\s+");

            for (int j = 1; j < linesTwo.size(); j++) {
                String secondTableRow = linesTwo.get(j);
                String[] secondTableRowData = secondTableRow.split("\\s+");

                //compare connected columns
                if (firstTableRowData[firstTableJoinColumn].equals(secondTableRowData[secondTableJoinColumn])) {
                    String mergedRow = newId++ + " " + String.join(" ", Arrays.copyOfRange(firstTableRowData, 1, firstTableRowData.length)) + " " + String.join(" ", Arrays.copyOfRange(secondTableRowData, 2, secondTableRowData.length));
                    result.add(mergedRow);
                }
            }
        }

        return "\n[OK] \n" + String.join("\n", result);
    }
}

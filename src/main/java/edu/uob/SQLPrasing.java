package edu.uob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//pase
public class SQLPrasing {
    
    // set SQL KEYWORDS
    private static final Set<String> SQL_KEYWORDS = new HashSet<>(Set.of(
        "add","delete","select", "insert", "into", "values", "create", "table", "database", "use", "from", "where", "and", "or", "join", "on", "drop", "set", "update", "alter"
    ));

    //keywords converted to lowercase
    private static String processKeyword(String word) {
        return SQL_KEYWORDS.contains(word.toLowerCase()) ? word.toLowerCase() : word;
    }
    
    
    public ParsedCommand parse(String command) {
        
        // Split command by space
        String[] CommandParts = command.trim().replaceAll("[()';,]", "").split("\\s+");
        
        //System.out.println("Command parts: " + Arrays.toString(CommandParts));

        //keywords converted to lowercase
        for (int i = 0; i < CommandParts.length; i++) {
            CommandParts[i] = processKeyword(CommandParts[i]);
        }
        
        if (CommandParts[0].equals("use")) {
            return parseUse(CommandParts);
        } else if (CommandParts[0].equals("create") && CommandParts[1].equals("database")) {
            return parseCreateDatabase(CommandParts);
        } else if (CommandParts[0].equals("create") && CommandParts[1].equals("table")) {
            return parseCreateTable(CommandParts);
        } else if (CommandParts[0].equals("insert") && CommandParts[1].equals("into")) {
            return parseInsert(CommandParts);
        } else if (CommandParts[0].equals("select")) {
            if (CommandParts.length > 2 && CommandParts[1].trim().equals("*") && CommandParts[2].equalsIgnoreCase("from")) {
                return parseSelectTable(CommandParts);
            } else {
                return parseSelectData(CommandParts);
            }
        } else if (CommandParts[0].equals("update") && CommandParts[2].equals("set")) {
            return parseUpdate(CommandParts);
        } else if (CommandParts[0].equals("delete") && CommandParts[1].equals("from")) {
            return parseDelete(CommandParts);
        } else if (CommandParts[0].equals("drop") && CommandParts[1].equals("database")) {
            return parseDropDatebase(CommandParts);
        } else if (CommandParts[0].equals("drop") && CommandParts[1].equals("table")) {
            return parseDropTable(CommandParts);
        } else if (CommandParts[0].equals("alter") && CommandParts[1].equals("table")&& CommandParts[3].equals("add")) {
            return parseAlterTableAdd(CommandParts);
        } else if (CommandParts[0].equals("alter") && CommandParts[1].equals("table")&& CommandParts[3].equals("drop")) {
            return parseAlterTableDrop(CommandParts);
        } else if (CommandParts[0].equals("join") && CommandParts[2].equals("and")&& CommandParts[4].equals("on")&& CommandParts[6].equals("and")) {
            //System.out.println("SUCCEED");
            return parseJoin(CommandParts);
        } 

        throw new IllegalArgumentException("[ERROR]Invalid SQL commands: " + command);
    }

    private UseCommand parseUse(String[] parts) {
        if (parts.length != 2) {
            throw new IllegalArgumentException("USE command requires a database name.");
        }

        UseCommand useCommand = new UseCommand();
        useCommand.DatabaseName = parts[1];
        return useCommand;
    }

    private CreateDatabaseCommand parseCreateDatabase(String[] parts) {
        if (parts.length != 3) {
            throw new IllegalArgumentException("CREATE DATABASE command requires a database name.");
        }

        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand();
        createDatabaseCommand.DatabaseName = parts[2];
        return createDatabaseCommand;
    }

    private CreateTableCommand parseCreateTable(String[] parts) {
        if (parts.length < 4) {
            throw new IllegalArgumentException("CREATE TABLE command requires a table name and columns.");
        }
    
        CreateTableCommand createTableCommand = new CreateTableCommand();

        createTableCommand.TableName = parts[2];
        //combined columns
        String columnPart = String.join(" ", Arrays.copyOfRange(parts, 3, parts.length));

        columnPart = columnPart.replaceAll("[();]", "").trim();
        String[] columnsTitle = columnPart.split("\\s*,\\s*");

        createTableCommand.ColumnsData = new String[columnsTitle.length + 1];
        createTableCommand.ColumnsData[0] = "id";

        System.arraycopy(columnsTitle, 0, createTableCommand.ColumnsData, 1, columnsTitle.length);

        return createTableCommand;
    }

    private InsertCommand parseInsert (String[] parts) {
        if (parts.length < 4 || !parts[3].equalsIgnoreCase("values")) {
            throw new IllegalArgumentException("Invalid INSERT.");
        }

        InsertCommand insertintoCommand = new InsertCommand();
        insertintoCommand.TableName=parts[2];

        String columnPart = String.join(" ", Arrays.copyOfRange(parts, 4, parts.length));
        columnPart = columnPart.replaceAll("[();]", "").trim();
        insertintoCommand.ColumnsData = columnPart.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); 

        for (int i = 0; i < insertintoCommand.ColumnsData.length; i++) {
            insertintoCommand.ColumnsData[i] = insertintoCommand.ColumnsData[i].replaceAll("^[\"']|[\"']$", "").trim();
        }

        return insertintoCommand;
    }
    
    private SelectTableCommand parseSelectTable (String[] parts) {
        
        if (parts.length < 4 ) {
            throw new IllegalArgumentException("Invalid Select Table.");
        }

        SelectTableCommand selectTableCommand = new SelectTableCommand();
        selectTableCommand.TableName = parts[3].replace(";", "").trim();
        //if WHERE
        if (parts.length > 4 && "WHERE".equalsIgnoreCase(parts[4])) {
            selectTableCommand.WhereCondition = String.join(" ", Arrays.copyOfRange(parts, 5, parts.length)).trim();
        } else {
            selectTableCommand.WhereCondition = null;
        }

        return selectTableCommand;
    }
    
    private SelectDataCommand parseSelectData (String[] parts) {
        if (parts.length < 4 ) {
            throw new IllegalArgumentException("Invalid Select Table and Data.");
        }

        SelectDataCommand selectDataCommand = new SelectDataCommand();

        // look for from
        int fromIndex = -1;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase("from")) {
                fromIndex = i;
                break;
            }
        }
        if (fromIndex == -1 || fromIndex + 1 >= parts.length) {
            throw new IllegalArgumentException("Missing FROM clause.");
        }
        
        selectDataCommand.TableName=parts[fromIndex +1];
        String columnsPart = String.join(" ", Arrays.copyOfRange(parts, 1, fromIndex));
        String[] columns = columnsPart.replace(";", "").split(" ");
        selectDataCommand.ColumnsData = columns;
    
        // look for where
        int whereIndex = -1;
        for (int i = fromIndex + 2; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase("where")) {
                whereIndex = i;
                break;
            }
        }

        if (whereIndex != -1 && whereIndex + 1 < parts.length) {
            String condition = String.join(" ", Arrays.copyOfRange(parts, whereIndex + 1, parts.length));
            selectDataCommand.WhereCondition = condition;
        } else {
            selectDataCommand.WhereCondition = null;
        }

        return selectDataCommand;
    }

    private UpdateCommand parseUpdate(String[] parts) {
        if (parts.length < 5) {
            throw new IllegalArgumentException("Invalid UPDATE command.");
        }

        //System.out.println("parseUpdate.Command parts: " + Arrays.toString(parts));

        UpdateCommand updateCommand = new UpdateCommand();
        updateCommand.TableName = parts[1];
        //System.out.println("parseUpdate.Parsed Table Name: " + updateCommand.TableName);

        //look for set
        int setIndex = 2;
        int whereIndex = -1;

        for (int i = 3; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase("WHERE")) {
                whereIndex = i;
                break;
            }
        }

        if (whereIndex == -1) {
            throw new IllegalArgumentException("Invalid UPDATE command. Missing after WHERE.");
        }

        // analyze set has 3 parts
        List<String> columnsDataList = new ArrayList<>();
        for (int i = setIndex + 1; i < whereIndex; i += 3) {
            if (i + 2 < whereIndex && parts[i + 1].equals("=")) {
                String setClause = parts[i] + " = " + parts[i + 2];
                columnsDataList.add(setClause);
                //System.out.println("parseUpdate.Parsed SET clause: " + setClause);
            } else {
                throw new IllegalArgumentException("Invalid SET format. Expected 'column = value'.");
            }
        }

        updateCommand.ColumnsData = columnsDataList.toArray(new String[0]);

        // analyze where
        StringBuilder wherePartBuilder = new StringBuilder();
        for (int i = whereIndex + 1; i < parts.length; i++) {
            wherePartBuilder.append(parts[i]).append(" ");
        }
        updateCommand.WhereCondition = wherePartBuilder.toString().trim();
        //System.out.println("parseUpdate.Parsed WHERE condition: " + updateCommand.WhereCondition);

        //System.out.println("parseUpdate returning UpdateCommand: Table = " + updateCommand.TableName + ", SET = " + Arrays.toString(updateCommand.ColumnsData) + ", WHERE = " + updateCommand.WhereCondition);
        return updateCommand;
    }

    private DeleteCommand parseDelete(String[] parts) {

        if (parts.length < 5) {
            throw new IllegalArgumentException("Invalid DeleteCommand");
        }
    
        //System.out.println("parseDelete.Command parts: " + Arrays.toString(parts));
        DeleteCommand deleteCommand = new DeleteCommand();
    
        deleteCommand.TableName = parts[2];
        //System.out.println("parseDelete.Parsed Table Name: " + deleteCommand.TableName);
    
        int whereIndex = -1;
    
        //look for where
        for (int i = 3; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase("WHERE")) {
                whereIndex = i;
                break;
            }
        }
    
        //if no where is wrong
        if (whereIndex == -1 || whereIndex + 1 >= parts.length) {
            System.out.println("[ERROR] No valid WHERE condition found.");
            throw new IllegalArgumentException("Invalid DeleteCommand: Missing WHERE clause.");
        }
    
        //analyze where
        StringBuilder wherePartBuilder = new StringBuilder();
        for (int i = whereIndex + 1; i < parts.length; i++) {
            wherePartBuilder.append(parts[i]).append(" ");
        }
        deleteCommand.WhereCondition = wherePartBuilder.toString().trim();
    
        //System.out.println("parseDelete.Parsed WHERE condition: " + deleteCommand.WhereCondition);
        return deleteCommand;
    }

    private DropDatabaseCommand parseDropDatebase(String[] parts) {
        if (parts.length != 3) {
            throw new IllegalArgumentException("Drop Database command requires a database name.");
        }
        DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand();
        dropDatabaseCommand.DatabaseName = parts[2];
        //System.out.println("parseDropDatabase.Parsed Database Name: " + dropDatabaseCommand.DatabaseName);
        
        return dropDatabaseCommand;
    }
    
    private DropTableCommand parseDropTable(String[] parts) {
        if (parts.length != 3) {
            throw new IllegalArgumentException("Drop table command requires a table name.");
        }
        DropTableCommand dropTableCommand = new DropTableCommand();
        dropTableCommand.TableName = parts[2];
        //System.out.println("parseDropDatabase.Parsed Database Name: " + dropTableCommand.TableName);
        return dropTableCommand;
    }

    private AlterTableAddCommand parseAlterTableAdd(String[] parts){
        if (parts.length != 5) {
            throw new IllegalArgumentException("Drop table command requires a table name.");
        }
        AlterTableAddCommand alterTableAddCommand = new AlterTableAddCommand();
        alterTableAddCommand.TableName = parts[2];
        alterTableAddCommand.TitleColumnsData = parts[4];
        //System.out.println("parseDropDatabase.Parsed Database Name: " + alterTableAddCommand.TableName);
        
        return alterTableAddCommand;     
    }

    private AlterTableDropCommand parseAlterTableDrop(String[] parts){
        if (parts.length != 5) {
            throw new IllegalArgumentException("Alter table command requires a table name.");
        }
        AlterTableDropCommand alterTableDropCommand = new AlterTableDropCommand();
        alterTableDropCommand.TableName = parts[2];
        alterTableDropCommand.TitleColumnsData = parts[4];
        //System.out.println("parseDropDatabase.Parsed Database Name: " + alterTableDropCommand.TableName);
        
        return alterTableDropCommand;     
    }

    private JoinCommand parseJoin(String[] parts){
        //System.out.println("SUCCEED");
        if (parts.length != 8) {
            throw new IllegalArgumentException("Join command requires a table name.");
        }
        JoinCommand joinCommand = new JoinCommand();
        joinCommand.OneTableName = parts[1];
        //System.out.println("parseJoin: " + joinCommand.OneTableName);
        joinCommand.TwoTableName = parts[3];
        //System.out.println("parseJoin: " + joinCommand.TwoTableName);
        joinCommand.OneTitleColumnsData = parts[5];
        //System.out.println("parseJoin: " + joinCommand.OneTitleColumnsData);
        joinCommand.TwoTitleColumnsData = parts[7];
        //System.out.println("parseJoin: " + joinCommand.TwoTitleColumnsData);

        return joinCommand;
    }

    //it is the result of storage and parsing
    public class ParsedCommand {
        //<Use>|<Create>|<Drop>|<Alter>|<Insert>|<Select>|<Update>|<Delete>|<Join>
        public String use; 
    }

    public class UseCommand extends ParsedCommand {
        public String DatabaseName;
    }

    public class CreateDatabaseCommand extends ParsedCommand {
        public String DatabaseName;
    }

    public class CreateTableCommand extends ParsedCommand {
        public String TableName;
        //["id", "name", "age"]
        public String[] ColumnsData;
    }

    public class InsertCommand extends ParsedCommand {
        public String TableName;
        //["id", "name", "age"]
        public String[] ColumnsData;
    }

    public class SelectTableCommand extends ParsedCommand {
        public String TableName;
        //["id", "name", "age"]
        public String WhereCondition;
    }

    public class SelectDataCommand extends ParsedCommand {
        public String TableName;
        //["id", "name", "age"]
        public String[] ColumnsData;
        public String WhereCondition;
    }

    public class DropDatabaseCommand extends ParsedCommand {
        public String DatabaseName;
    }
    
    public class DropTableCommand extends ParsedCommand {
        public String TableName;
    }

    public class AlterTableAddCommand extends ParsedCommand {
        public String TableName;
        public String TitleColumnsData;
    }
    
    public class AlterTableDropCommand extends ParsedCommand {
        public String TableName;
        public String TitleColumnsData;
    }

    public class UpdateCommand extends ParsedCommand {
        public String TableName;
        //["id", "name", "age"]
        public String[] ColumnsData;
        public String WhereCondition;
    }

    public class DeleteCommand extends ParsedCommand {
        public String TableName;
        //["id", "name", "age"]
        public String WhereCondition;
    }

    public class JoinCommand extends ParsedCommand {
        public String OneTableName;
        public String TwoTableName;
        public String OneTitleColumnsData;
        public String TwoTitleColumnsData;
    }
}


package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;
    //"databases/"
    private String storageFolderPath;
    private Handling handling;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
    * KEEP this signature otherwise we won't be able to mark your submission correctly.
    */
    public DBServer() {
        this.storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        this.handling = new Handling(storageFolderPath);
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
    }

    /**
    * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
    * able to mark your submission correctly.
    *
    * <p>This method handles all incoming DB commands and carries out the required actions.
    */

    public String handleCommand(String command) {
        //TODO implement your server logic here
        try{
            //System.out.println("Command received: " + command);
            SQLPrasing parser = new SQLPrasing();
            SQLPrasing.ParsedCommand parsedCommand = parser.parse(command);

            if (parsedCommand instanceof SQLPrasing.UseCommand) {
                return handling.handleUseCommand((SQLPrasing.UseCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.CreateDatabaseCommand) {
                return handling.handleCreateDatabaseCommand((SQLPrasing.CreateDatabaseCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.CreateTableCommand) {
                return handling.handleCreateTableCommand((SQLPrasing.CreateTableCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.InsertCommand) {
                return handling.handleInsertCommand((SQLPrasing.InsertCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.SelectTableCommand) {
                return handling.handleSelectTableCommand((SQLPrasing.SelectTableCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.SelectDataCommand) {
                return handling.handleSelectDataCommand((SQLPrasing.SelectDataCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.UpdateCommand) {
                return handling.handleUpdateCommand((SQLPrasing.UpdateCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.DeleteCommand) {
                return handling.handleDeleteCommand((SQLPrasing.DeleteCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.DropDatabaseCommand) {
                return handling.handleDropDatabaseCommand((SQLPrasing.DropDatabaseCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.DropTableCommand) {
                return handling.handleDropTableCommand((SQLPrasing.DropTableCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.AlterTableAddCommand) {
                return handling.handleAlterTableAddCommand((SQLPrasing.AlterTableAddCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.AlterTableDropCommand) {
                return handling.handleAlterTableDropCommand((SQLPrasing.AlterTableDropCommand) parsedCommand);
            } 
            else if (parsedCommand instanceof SQLPrasing.JoinCommand) {
                return handling.handleJoinCommand((SQLPrasing.JoinCommand) parsedCommand);
            }
            return "[ERROR] Can't recognize the command: " + command;
        } catch (IllegalArgumentException ex) {
            return "[ERROR] " + ex.getMessage();
        } catch (Exception ex) {
            return "[ERROR] An unexpected error occurred while processing your request.";
        }
    }


    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===
    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }
}

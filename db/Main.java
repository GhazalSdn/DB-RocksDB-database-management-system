package aut.db;

import org.rocksdb.RocksDBException;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws RocksDBException {
        String[] commands;
        DB db = new DB();
        Scanner userInputScanner = new Scanner(System.in);

        while (true) {
            commands = userInputScanner.nextLine().split(" ");
            String command = commands[0];
            switch (command) {
                case "create": {

                    db.create(commands[1], commands[2]);
                    break;

                }
                case "fetch": {
                    db.fetch(commands[1]);
                    break;

                }
                case "update": {
                    db.update(commands[1], commands[2]);
                    break;
                }
                case "delete": {
                    db.delete(commands[1]);
                    break;

                }
                default: {
                    System.out.println();
                }


            }
        }


    }
}

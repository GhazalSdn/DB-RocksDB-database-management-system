package aut.db;

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

public class DB {
    RocksDB db;

    public DB() {
        this.loadCSV();
    }

    public void loadCSV() {
        File f = new File("American Stock Exchange 20200206_Names_ClosedVal.csv");
        BufferedReader br = null;
        String line = "";
        byte[] key;
        byte[] value;


        RocksDB.loadLibrary();

        try (final Options options = new Options().setCreateIfMissing(true)) {

            try {
                db = RocksDB.open(options, "DBpath");


                br = new BufferedReader(new FileReader(f));
                while ((line = br.readLine()) != null) {


                    String[] data = line.split(",");
                    key = data[0].getBytes(StandardCharsets.UTF_8);
                    value = data[1].getBytes(StandardCharsets.UTF_8);
                    db.put(key, value);


//                    System.out.println("key= " + data[0] + " , value= " + data[1]);


                }
            } catch (RocksDBException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

    }


    public void create(String key, String value) {

        try {
            byte[] existedvalue = db.get(key.getBytes(StandardCharsets.UTF_8));
            if (existedvalue != null) {
                System.out.println("false");
                System.out.println();
            } else {
                db.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
                System.out.println("true");
                System.out.println();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }


    }

    public void fetch(String key) throws RocksDBException {
        byte[] existedvalue = db.get(key.getBytes(StandardCharsets.UTF_8));
        if (existedvalue != null) {
            System.out.println("true");
            String result = new String(existedvalue);
            System.out.println(result);
            System.out.println();
        } else {

            System.out.println("false");
            System.out.println();
        }

    }

    public void update(String key, String value) throws RocksDBException {
        byte[] existedvalue = db.get(key.getBytes(StandardCharsets.UTF_8));
        if (existedvalue != null) {
            db.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            System.out.println("true");
            System.out.println();
        } else {
            System.out.println("false");
            System.out.println();
        }

    }

    public void delete(String key) throws RocksDBException {
        byte[] existedvalue = db.get(key.getBytes(StandardCharsets.UTF_8));
        if (existedvalue != null) {
            db.delete(key.getBytes(StandardCharsets.UTF_8));
            System.out.println("true");
            System.out.println();
        } else {
            System.out.println("false");
            System.out.println();
        }

    }


}

package edu.buffalo.cse.cse486586.simpledynamo.storage;

/**
 * Created by pavanjoshi on 4/28/17.
 */

public class DynamoStorageSchema {
    public static final String DATABASE_NAME = "Dynamo.db";
    public static int DATABASE_VERSION = 1;


    public static class HashTable{
        public static final String TABLE_NAME = "local_hash_table";

        public static final String COLUMN_ONE = "key";
        public static final String COLUMN_ONE_TYPE = "TEXT";

        public static final String COLUMN_TWO = "value";
        public static final String COLUMN_TWO_TYPE = "TEXT NOT NULL";

        public static final String COLUMN_THREE = "version";
        public static final String COLUMN_THREE_TYPE = "INTEGER NOT NULL";

        public static final String COLUMN_FOUR = "key_hash";
        public static final String COLUMN_FOUR_TYPE = "TEXT NOT NULL";

        public static final String PRIMARY_KEY = "PRIMARY KEY( " + COLUMN_ONE + " )";

        public static final String CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                + "( " + COLUMN_ONE + " " + COLUMN_ONE_TYPE + ","
                + COLUMN_TWO + " " + COLUMN_TWO_TYPE + ","
                + COLUMN_THREE + " " + COLUMN_THREE_TYPE + ","
                + COLUMN_FOUR + " " + COLUMN_FOUR_TYPE + ","
                + PRIMARY_KEY + ");";

        public static final String DROP = "DROP TABLE IF EXISTS " + TABLE_NAME;

    }

}

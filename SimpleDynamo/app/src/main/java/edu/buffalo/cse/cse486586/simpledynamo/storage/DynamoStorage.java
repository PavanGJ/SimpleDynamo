package edu.buffalo.cse.cse486586.simpledynamo.storage;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by pavanjoshi on 4/28/17.
 */

public class DynamoStorage extends SQLiteOpenHelper {

    public DynamoStorage(Context context){
        super(context,DynamoStorageSchema.DATABASE_NAME,null,DynamoStorageSchema.DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(DynamoStorageSchema.HashTable.CREATE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(DynamoStorageSchema.HashTable.DROP);
        this.onCreate(db);
    }

}

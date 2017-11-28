package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import edu.buffalo.cse.cse486586.simpledynamo.storage.DynamoStorage;
import edu.buffalo.cse.cse486586.simpledynamo.storage.DynamoStorageSchema;

public class SimpleDynamoProvider extends ContentProvider {

    private final String TAG = this.getClass().getName();

    private final String[] EMULATORID = {"5562","5556","5554","5558","5560"};
    protected final int MODVAL = EMULATORID.length;
    protected final int PREFLISTSIZE = 3;

    protected final int SERVERPORT = 10000;

    protected final Uri uri = buildURI();

    protected static final String JSON_DEST = "dest";
    protected static final String JSON_PAYLOAD = "payload";
    protected static final String JSON_SRC = "src";
    protected static final String JSON_TYPE = "type";
    protected static final String JSON_CONTENT = "content";
    protected static final String JSON_ARGS = "args";

    protected static final String CONTENTVALUES_KEY = DynamoStorageSchema.HashTable.COLUMN_ONE;
    protected static final String CONTENTVALUES_VALUE = DynamoStorageSchema.HashTable.COLUMN_TWO;
    protected static final String CONTENTVALUES_VERSION
            = DynamoStorageSchema.HashTable.COLUMN_THREE;
    protected static final String CONTENTVALUES_HASHVAL
            = DynamoStorageSchema.HashTable.COLUMN_FOUR;

    protected static final int JSONTYPE_ALIVE = 0;
    protected static final int JSONTYPE_DEL = 1;
    protected static final int JSONTYPE_INSERT = 2;
    protected static final int JSONTYPE_QUERY = 3;
    protected static final int JSONTYPE_ALIVERESPONSE = 5;
    protected static final int JSONTYPE_RESETRESPONSE = 6;

	protected List<String> remotePorts = Collections.synchronizedList(new ArrayList<String>(MODVAL));
    protected Boolean[] rportIsalive = new Boolean[MODVAL];
    protected String[] rportHashval = new String[MODVAL];
	protected String myPort = null;
    protected int selfIdx;
    protected int failures = 0;
    protected boolean boolQuery = Boolean.FALSE;
    protected boolean reset = Boolean.FALSE;
    protected PriorityBlockingQueue<JSONObject> qReply = new PriorityBlockingQueue<JSONObject>(10,
            new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject lhs, JSONObject rhs) {
                    return 0;
                }
            });


	private SQLiteDatabase localStorage;

    public Uri buildURI(){
        Uri.Builder uri = new Uri.Builder();
        uri.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
        uri.scheme("content");
        return uri.build();
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        JSONObject msg,payload;
        JSONArray dest;
        switch (selection.charAt(0)){
            case '*':
                dest = new JSONArray();
                for(int idx = 0; idx < remotePorts.size(); idx++){
                    if(remotePorts.get(idx).compareTo(myPort)==0) continue;
                    dest.put(remotePorts.get(idx));
                }
                try {
                    payload = new JSONObject();
                    payload.put(JSON_SRC,myPort);
                    payload.put(JSON_TYPE,JSONTYPE_DEL);
                    payload.put(JSON_CONTENT,"@");

                    msg = new JSONObject();
                    msg.put(JSON_DEST,dest);
                    msg.put(JSON_PAYLOAD,payload.toString());

                    new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);

                } catch (JSONException e) {
                    Log.e(TAG,"JSON Exception");
                    e.printStackTrace();
                }

            case '@':
                localStorage.execSQL(DynamoStorageSchema.HashTable.DROP);
                localStorage.execSQL(DynamoStorageSchema.HashTable.CREATE);
                break;
            default:
                if(selectionArgs==null || selectionArgs[0].compareTo("False")!=0) {
                    dest = new JSONArray();
                    try {
                        String selHash = genHash(selection);
                        for (int idx = 0; idx < rportHashval.length; idx++) {
                            if (idx == 0) {
                                if (selHash.compareTo(rportHashval[0]) < 0 ||
                                        selHash.compareTo(rportHashval[MODVAL-1]) > 0) {
                                    for (int i = 0; i < PREFLISTSIZE; i++) {
                                        dest.put(i, remotePorts.get(idx + i));
                                    }
                                    break;
                                }
                            } else {
                                if (selHash.compareTo(rportHashval[idx]) < 0 &&
                                        selHash.compareTo(rportHashval[idx - 1]) > 0) {
                                    for (int i = 0; i < PREFLISTSIZE; i++) {
                                        dest.put(i, remotePorts.get((idx + i)%MODVAL));
                                    }
                                    break;
                                }
                            }
                        }
                        payload = new JSONObject();
                        payload.put(JSON_CONTENT,selection);
                        payload.put(JSON_SRC,myPort);
                        payload.put(JSON_TYPE,JSONTYPE_DEL);

                        msg = new JSONObject();
                        msg.put(JSON_DEST,dest);
                        msg.put(JSON_PAYLOAD,payload.toString());

                        new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);

                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "No Such Algorithm");
                        e.printStackTrace();
                    } catch (JSONException e) {
                        Log.e(TAG, "JSONException");
                        e.printStackTrace();
                    }
                }
                else {
                    String whereClause = DynamoStorageSchema.HashTable.COLUMN_ONE + " = ?";
                    String[] whereArgs = {selection};
                    localStorage.delete(
                            DynamoStorageSchema.HashTable.TABLE_NAME,
                            whereClause,
                            whereArgs
                    );
                }

                break;

        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString(CONTENTVALUES_KEY);
        String value = values.getAsString(CONTENTVALUES_VALUE);
        long timestamp = System.currentTimeMillis();
        JSONObject pair = new JSONObject();
        try {
            pair.put(CONTENTVALUES_VALUE, value);
            pair.put(CONTENTVALUES_KEY, key);
            pair.put(CONTENTVALUES_VERSION, timestamp);
        } catch (JSONException e){
            e.printStackTrace();
        }
        JSONArray dest;
        JSONObject payload,msg;
        dest = new JSONArray();
        try {
            String selHash = genHash(key);
            for (int idx = 0; idx < rportHashval.length; idx++) {
                if (idx == 0) {
                    if (selHash.compareTo(rportHashval[0]) < 0 ||
                            selHash.compareTo(rportHashval[MODVAL-1]) > 0) {
                        for (int i = 0; i < PREFLISTSIZE; i++) {
                            dest.put(i, remotePorts.get(idx + i));
                        }
                        break;
                    }
                } else {
                    if (selHash.compareTo(rportHashval[idx]) < 0 &&
                            selHash.compareTo(rportHashval[idx - 1]) > 0) {
                        for (int i = 0; i < PREFLISTSIZE; i++) {
                            dest.put(i, remotePorts.get((idx + i)%MODVAL));
                        }
                        break;
                    }
                }
            }
            payload = new JSONObject();
            payload.put(JSON_CONTENT,pair.toString());
            payload.put(JSON_SRC,myPort);
            payload.put(JSON_TYPE,JSONTYPE_INSERT);

            msg = new JSONObject();
            msg.put(JSON_DEST,dest);
            msg.put(JSON_PAYLOAD,payload.toString());

            new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No Such Algorithm");
            e.printStackTrace();
        } catch (JSONException e) {
            Log.e(TAG, "JSONException");
            e.printStackTrace();
        }
		return uri;
	}

	public void localInsert(Uri uri, ContentValues values) {
        try {
            String selection = values.getAsString(CONTENTVALUES_KEY);
            if (update(uri, values, selection, null) == 0) {
                localStorage.beginTransaction();
                values.put(CONTENTVALUES_HASHVAL, genHash(values.getAsString(CONTENTVALUES_KEY)));
                localStorage.insert(
                        DynamoStorageSchema.HashTable.TABLE_NAME,
                        null,
                        values
                );
                localStorage.setTransactionSuccessful();
                localStorage.endTransaction();
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
	@Override
	public boolean onCreate() {


        for(int idx = 0; idx < MODVAL; idx++){
            String REMOTE_PORT = Integer.toString(Integer.parseInt(EMULATORID[idx])*2);
            this.remotePorts.add(idx, REMOTE_PORT);
            this.rportIsalive[idx] = Boolean.FALSE;
            try {
                this.rportHashval[idx] = this.genHash(EMULATORID[idx]);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG,"No Such Algorithm : Raised by genHash at onCreate()");
            }

        }
		TelephonyManager telephonyManager =
				(TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String lineNumber = telephonyManager.getLine1Number();
		this.myPort = Integer.toString(
				Integer.parseInt(lineNumber.substring(lineNumber.length()-4))*2);
		DynamoStorage dbStorage = new DynamoStorage(this.getContext());
        this.selfIdx = this.remotePorts.indexOf(this.myPort);
        this.rportIsalive[this.selfIdx] = Boolean.TRUE;
		this.localStorage = dbStorage.getWritableDatabase();
        this.localStorage.beginTransaction();
        this.localStorage.execSQL(DynamoStorageSchema.HashTable.DROP);
        this.localStorage.execSQL(DynamoStorageSchema.HashTable.CREATE);
        this.localStorage.setTransactionSuccessful();
        this.localStorage.endTransaction();
        //Log.e("Test - "+myPort,"I am alive");
        this.reset = Boolean.TRUE;
        new ServerThread().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);

        JSONObject payload = new JSONObject();

        try {
            payload.put(JSON_SRC,myPort);
            payload.put(JSON_TYPE,JSONTYPE_ALIVE);
            JSONObject reply = new JSONObject();
            JSONArray dest = new JSONArray(remotePorts.toArray());
            reply.put(JSON_DEST,dest);
            reply.put(JSON_PAYLOAD,payload.toString());
            new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,reply);
        } catch (JSONException e) {
            Log.e(TAG,"JSON Exception");
            e.printStackTrace();
        }

        return false;
	}

	public void reset(JSONObject data){
        Iterator<String> iterator = data.keys();
        while(iterator.hasNext()){
            try {
                ContentValues values = new ContentValues();
                String key = iterator.next();
                String[] value = data.getString(key).split(":");
                values.put(CONTENTVALUES_KEY,key);
                values.put(CONTENTVALUES_HASHVAL,genHash(key));
                values.put(CONTENTVALUES_VALUE,value[0]);
                values.put(CONTENTVALUES_VERSION,Long.parseLong(value[1]));
                localInsert(uri,values);
                //Log.e("RInsert",key+": "+value[0]);

            } catch (JSONException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        JSONObject msg,payload;
        JSONArray dest;
        int responses;
        boolQuery = Boolean.TRUE;
        if(selection.contains("*")){
            responses = MODVAL - failures;
            dest = new JSONArray();
            for(int idx = 0; idx < remotePorts.size(); idx++){
                dest.put(remotePorts.get(idx));
            }
            try {
                payload = new JSONObject();
                payload.put(JSON_SRC,myPort);
                payload.put(JSON_TYPE,JSONTYPE_QUERY);
                payload.put(JSON_CONTENT,selection);
                payload.put(JSON_ARGS,"null");

                msg = new JSONObject();
                msg.put(JSON_DEST,dest);
                msg.put(JSON_PAYLOAD,payload.toString());
                msg.put(JSON_TYPE,JSONTYPE_QUERY);

                new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
            } catch (JSONException e) {
                Log.e(TAG,"JSON Exception" + e.getMessage());
                e.printStackTrace();
            }
        }
        else{
            responses = PREFLISTSIZE - failures;
            dest = new JSONArray();
            try {
                payload = new JSONObject();
                payload.put(JSON_CONTENT,selection);
                payload.put(JSON_SRC,myPort);
                payload.put(JSON_TYPE,JSONTYPE_QUERY);
                String selHash = genHash(selection);
                String args = "null";
                if(selection.contains("@")){
                    responses = MODVAL - failures;
                    if((this.selfIdx-3+MODVAL)%MODVAL>this.selfIdx){
                        args = DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                " <= '" + rportHashval[this.selfIdx] + "' OR " +
                                DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                " > '" + rportHashval[(this.selfIdx-3+MODVAL)%MODVAL] + "'";
                    }
                    else{
                        args = DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                " <= '" + rportHashval[this.selfIdx] + "' AND " +
                                DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                " > '" + rportHashval[(this.selfIdx-3+MODVAL)%MODVAL] + "'";
                    }
                    for (int idx = 0; idx < MODVAL; idx++) {
                        dest.put(idx, remotePorts.get(idx % MODVAL));
                    }
                }
                else {
                    for (int idx = 0; idx < rportHashval.length; idx++) {
                        if (idx == 0) {
                            if (selHash.compareTo(rportHashval[0]) < 0 ||
                                    selHash.compareTo(rportHashval[MODVAL - 1]) > 0) {
                                for (int i = 0; i < PREFLISTSIZE; i++) {
                                    dest.put(i, remotePorts.get(idx + i));
                                }
                                break;
                            }
                        } else {
                            if (selHash.compareTo(rportHashval[idx]) < 0 &&
                                    selHash.compareTo(rportHashval[idx - 1]) > 0) {
                                for (int i = 0; i < PREFLISTSIZE; i++) {
                                    dest.put(i, remotePorts.get((idx + i) % MODVAL));
                                }
                                break;
                            }
                        }
                    }
                }
                payload.put(JSON_ARGS,args);

                msg = new JSONObject();
                msg.put(JSON_DEST,dest);
                msg.put(JSON_PAYLOAD,payload.toString());
                msg.put(JSON_TYPE,JSONTYPE_QUERY);

                new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);

            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "No Such Algorithm");
                e.printStackTrace();
            } catch (JSONException e) {
                Log.e(TAG, "JSONException");
                e.printStackTrace();
            }
        }
        JSONObject[] responseArray;
        if(selection.contains("*") || selection.contains("@")) {
            responseArray = new JSONObject[responses];
            for (int idx = 0; idx < responses; idx++) {
                //Log.e("Waiting For: "+idx+ " of "+responses,"PREFLIST: "+PREFLISTSIZE+" FAILED: "+failures);
                try {
                    responseArray[idx] = qReply.take();
                } catch (InterruptedException e) {
                    Log.e(TAG, "InterruptedException");
                    e.printStackTrace();
                    return null;
                }
            }
        }
        else{
            responseArray = new JSONObject[responses];
            int idx = 0;
            while(idx < responses){
                //Log.e("Waiting For: "+idx+ " of "+responses,"PREFLIST: "+PREFLISTSIZE+" FAILED: "+failures+" Q Size: "+qReply.size());
                //TODO: Handle infinite loops because of qReply.take()
                try {
                    JSONObject resp = qReply.take();
                    if(resp.has(selection)){
                        responseArray[idx] = resp;
                        idx++;
                    }
                    else{
                        if(!resp.keys().hasNext())
                            resp.put(selection,"Failed:-1");
                        qReply.add(resp);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
        boolQuery = Boolean.FALSE;
        String[] columnNames = {
                DynamoStorageSchema.HashTable.COLUMN_ONE,
                DynamoStorageSchema.HashTable.COLUMN_TWO
        };
        JSONObject result = responseArray[0];
        //Log.e("0",result.toString());
        for(int i = 1; i < responses; i++){
            //Log.e(i+"",responseArray[i].toString());
            Iterator<String> iterator = responseArray[i].keys();
            while(iterator.hasNext()){
                try {
                    String key = iterator.next();
                    String value = responseArray[i].getString(key);
                    if(result.has(key)){
                        String[] newVal = value.split(":");
                        String[] oldVal = result.getString(key).split(":");
                        long oldVersion = Long.parseLong(oldVal[1]);
                        long newVersion = Long.parseLong(newVal[1]);
                        if(newVersion>oldVersion){
                            result.put(key,value);
                        }
                    }
                    else{
                        result.put(key,value);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        Iterator<String> iterator = result.keys();
        while(iterator.hasNext()){
            try {
                String key = iterator.next();
                String value = result.getString(key).split(":")[0];
                matrixCursor.addRow(new Object[]{key,value});
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

		return matrixCursor;
	}

	public JSONObject localQuery(String selection,String selectionArgs) {
        String[] projection = new String[]{
                DynamoStorageSchema.HashTable.COLUMN_ONE,
                DynamoStorageSchema.HashTable.COLUMN_TWO,
                DynamoStorageSchema.HashTable.COLUMN_THREE
        };
        String whereClause = null;
        String[] whereArgs = null;
        if(selection.contains("@")){
            whereClause = selectionArgs;
        }
        else if(!selection.contains("*")) {
            whereClause = DynamoStorageSchema.HashTable.COLUMN_ONE + " = ?";
            whereArgs = new String[]{selection};
        }


        String sortOrder = DynamoStorageSchema.HashTable.COLUMN_ONE + " DESC";
        try {
            localStorage.beginTransaction();
            Cursor cursor = localStorage.query(
                    DynamoStorageSchema.HashTable.TABLE_NAME,
                    projection,
                    whereClause,
                    whereArgs,
                    null,
                    null,
                    sortOrder
            );
            localStorage.setTransactionSuccessful();
            localStorage.endTransaction();
            JSONObject reply = new JSONObject();
            if (cursor != null) {
                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    String key = cursor.getString(
                            cursor.getColumnIndex(
                                    DynamoStorageSchema.HashTable.COLUMN_ONE
                            )
                    );
                    String value = cursor.getString(
                            cursor.getColumnIndex(
                                    DynamoStorageSchema.HashTable.COLUMN_TWO
                            )
                    );
                    String version = cursor.getString(
                            cursor.getColumnIndex(
                                    DynamoStorageSchema.HashTable.COLUMN_THREE
                            )
                    );
                    reply.put(key, value + ":" + version);
                    cursor.moveToNext();
                }
                cursor.close();
            }
            return reply;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

        String[] projection = {
                DynamoStorageSchema.HashTable.COLUMN_ONE,
                DynamoStorageSchema.HashTable.COLUMN_TWO,
                DynamoStorageSchema.HashTable.COLUMN_THREE
        };
        String whereClause = DynamoStorageSchema.HashTable.COLUMN_ONE + " = ?";
        String[] whereArgs = {selection};
        String sortOrder = DynamoStorageSchema.HashTable.COLUMN_ONE + " DESC";
        localStorage.beginTransaction();
        Cursor cursor = localStorage.query(
                DynamoStorageSchema.HashTable.TABLE_NAME,
                projection,
                whereClause,
                whereArgs,
                null,
                null,
                sortOrder
        );

        if(cursor.getCount()==0) {
            cursor.close();
            localStorage.setTransactionSuccessful();
            localStorage.endTransaction();
            return 0;
        }
        else{
            cursor.moveToFirst();
            int idx = cursor.getColumnIndex(DynamoStorageSchema.HashTable.COLUMN_THREE);
            long old_timestamp = cursor.getLong(idx);
            long new_timestamp = values.getAsLong(CONTENTVALUES_VERSION);
            if(new_timestamp>old_timestamp) {
                localStorage.update(
                        DynamoStorageSchema.HashTable.TABLE_NAME,
                        values,
                        whereClause,
                        whereArgs
                );
            }
            cursor.close();
            localStorage.setTransactionSuccessful();
            localStorage.endTransaction();
            return 1;
        }

	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientThread extends AsyncTask<JSONObject,String,Void>{

        private final String TAG = getClass().getName();


        @Override
        protected Void doInBackground(JSONObject... params) {

            JSONObject msg = params[0];
            try {
                JSONArray ports = msg.getJSONArray(JSON_DEST);
                String message = msg.getString(JSON_PAYLOAD);
                for (int idx = 0; idx < ports.length(); idx++) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(
                                new byte[]{10, 0, 2, 2}), Integer.parseInt(ports.getString(idx)));
                        socket.setSoTimeout(500);

                        DataOutputStream outputStream =
                                new DataOutputStream(socket.getOutputStream());
                        outputStream.writeUTF(message);
                        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                        if(msg.has(JSON_TYPE)){
                            if(msg.getInt(JSON_TYPE)==JSONTYPE_QUERY) {
                                String reply = inputStream.readUTF();
                                JSONObject response = new JSONObject(reply);
                                qReply.add(response);
                            }
                            else if(msg.getInt(JSON_TYPE)==JSONTYPE_RESETRESPONSE){
                                JSONObject resetPacket = new JSONObject(msg.getString(JSON_CONTENT));
                                reset(resetPacket);
                            }
                        }
                        int ack = inputStream.readInt();
                        if(ack==-1)
                            throw new EOFException();
                        outputStream.flush();
                        socket.close();


                    } catch (EOFException e){
                        int rIdx = remotePorts.indexOf(ports.getString(idx));
                        Log.e(TAG, "End of File - Raised by ClientThread - " + rIdx);
                        rportIsalive[rIdx] = Boolean.FALSE;
                        if (rIdx >= selfIdx && rIdx < (selfIdx + PREFLISTSIZE) % MODVAL)
                            failures = 0;
                        if(boolQuery) {
                            JSONObject fail = new JSONObject();
                            fail.put(new JSONObject(message).getString(JSON_CONTENT),"Failed:-1");
                            qReply.add(fail);
                        }
                        e.printStackTrace();
                    } catch (SocketTimeoutException e){
                        int rIdx = remotePorts.indexOf(ports.getString(idx));
                        Log.e(TAG, "End of File - Raised by ClientThread - " + rIdx);
                        rportIsalive[rIdx] = Boolean.FALSE;
                        if (rIdx >= selfIdx && rIdx < (selfIdx + PREFLISTSIZE) % MODVAL)
                            failures = 0;
                        if(boolQuery) {
                            JSONObject fail = new JSONObject();
                            fail.put(new JSONObject(message).getString(JSON_CONTENT),"Failed:-1");
                            qReply.add(fail);
                        }
                        e.printStackTrace();
                    } catch (StreamCorruptedException e){
                        int rIdx = remotePorts.indexOf(ports.getString(idx));
                        Log.e(TAG, "End of File - Raised by ClientThread - " + rIdx);
                        rportIsalive[rIdx] = Boolean.FALSE;
                        if (rIdx >= selfIdx && rIdx < (selfIdx + PREFLISTSIZE) % MODVAL)
                            failures = 0;
                        if(boolQuery) {
                            JSONObject fail = new JSONObject();
                            fail.put(new JSONObject(message).getString(JSON_CONTENT),"Failed:-1");
                            qReply.add(fail);
                        }
                        e.printStackTrace();
                    } catch (IOException e){
                        int rIdx = remotePorts.indexOf(ports.getString(idx));
                        Log.e(TAG, "End of File - Raised by ClientThread - " + rIdx);
                        rportIsalive[rIdx] = Boolean.FALSE;
                        if (rIdx >= selfIdx && rIdx < (selfIdx + PREFLISTSIZE) % MODVAL)
                            failures = 0;
                        if(boolQuery) {
                            JSONObject fail = new JSONObject();
                            fail.put(new JSONObject(message).getString(JSON_CONTENT),"Failed:-1");
                            qReply.add(fail);
                        }
                        e.printStackTrace();
                    }
                }
            } catch (JSONException e) {
                Log.e(TAG,"JSON Exception - Raised by " +
                        "doInBackground at `JSONObject`.get() methods");
                e.printStackTrace();
            }

            return null;
        }

    }

    private class ServerThread extends AsyncTask<Void,JSONObject,Void>{

        private final String TAG = this.getClass().getName();
        private ServerSocket server;


        @Override
        protected void onPreExecute() {
            super.onPreExecute();
            try {
                this.server = new ServerSocket(SERVERPORT);
            } catch (IOException e) {
                Log.e(TAG,"I/O Exception - Raised by ServerSocket");
                e.printStackTrace();
            }
        }

        @Override
        protected Void doInBackground(Void... params) {

            while(Boolean.TRUE) {
                try {
                    Socket socket = this.server.accept();
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    JSONObject msg = new JSONObject(inputStream.readUTF());
                    if(msg.getInt(JSON_TYPE)==JSONTYPE_QUERY){
                        String query = msg.getString(JSON_CONTENT);
                        String args = msg.getString(JSON_ARGS);
                        String src = msg.getString(JSON_SRC);
                        if(src.compareTo(myPort)==0 || args.compareTo("null")==0)
                            args=null;
                        JSONObject qreply = localQuery(query,args);
                        outputStream.writeUTF(qreply.toString());
                    }
                    else{
                        publishProgress(msg);
                    }
                    outputStream.writeInt(1);
                    outputStream.flush();
                    socket.close();

                } catch (EOFException e) {
                    Log.e(TAG, "End of File - Raised by Server");
                    qReply.add(new JSONObject());
                    e.printStackTrace();
                } catch (SocketTimeoutException e) {
                    Log.e(TAG, "Socket Timed Out - Raised by Server");
                    qReply.add(new JSONObject());
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    Log.e(TAG, "Stream Corrupted - Raised by Server");
                    qReply.add(new JSONObject());
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "I/O Exception - Raised by Server");
                    qReply.add(new JSONObject());
                    e.printStackTrace();
                } catch (JSONException e) {
                    qReply.add(new JSONObject());
                    e.printStackTrace();
                }
            }
            return null;
        }
        @Override
        protected void onProgressUpdate(JSONObject... params){
            try {
                JSONObject msg = params[0];
                int type = msg.getInt(JSON_TYPE);
                switch (type){
                    case JSONTYPE_ALIVE:
                        String port = msg.getString(JSON_SRC);
                        int idx = remotePorts.indexOf(port);
                        rportIsalive[idx] = Boolean.TRUE;
                        //Log.e("Test - " + myPort, "Node alive - " + port);
                        JSONObject payload = new JSONObject();
                        payload.put(JSON_SRC, myPort);
                        payload.put(JSON_TYPE, JSONTYPE_ALIVERESPONSE);
                        JSONObject reply = new JSONObject();
                        JSONArray dest = new JSONArray(new String[]{port});
                        reply.put(JSON_DEST, dest);
                        reply.put(JSON_PAYLOAD, payload.toString());
                        new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply);
                        if((idx==(selfIdx-1+MODVAL)%MODVAL ||
                                idx==(selfIdx+1+MODVAL)%MODVAL)){
                            String selection = "@";
                            String args;
                            if((idx-3+MODVAL)%MODVAL>idx){
                                args = DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                        " <= '" + rportHashval[idx] + "' OR " +
                                        DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                        " > '" + rportHashval[(idx-3+MODVAL)%MODVAL] + "'";
                            }
                            else{
                                args = DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                        " <= '" + rportHashval[idx] + "' AND " +
                                        DynamoStorageSchema.HashTable.COLUMN_FOUR +
                                        " > '" + rportHashval[(idx-3+MODVAL)%MODVAL] + "'";
                            }
                            JSONObject reset = localQuery(selection,args);
                            payload.put(JSON_CONTENT,reset.toString());
                            payload.put(JSON_TYPE,JSONTYPE_RESETRESPONSE);
                            reply.put(JSON_PAYLOAD,payload.toString());
                            new ClientThread().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,reply);
                            //Log.e("Reset Sent","From: "+myPort+" To: "+port);
                            if(failures==1 && idx>=selfIdx && idx<(selfIdx+PREFLISTSIZE)%MODVAL)
                                failures = 0;
                        }
                        break;
                    case JSONTYPE_DEL:
                        String selection = msg.getString(JSON_CONTENT);
                        delete(uri,selection,new String[]{"False"});
                        break;
                    case JSONTYPE_INSERT:
                        JSONObject keyval = new JSONObject(msg.getString(JSON_CONTENT));
                        ContentValues values = new ContentValues();
                        values.put(CONTENTVALUES_KEY,keyval.getString(CONTENTVALUES_KEY));
                        values.put(CONTENTVALUES_VALUE,keyval.getString(CONTENTVALUES_VALUE));
                        values.put(CONTENTVALUES_VERSION,keyval.getLong(CONTENTVALUES_VERSION));
                        localInsert(uri,values);
                        break;
                    case JSONTYPE_ALIVERESPONSE:
                        String rport = msg.getString(JSON_SRC);
                        int index = remotePorts.indexOf(rport);
                        rportIsalive[index] = Boolean.TRUE;
                        //Log.e("Test - " + myPort, "Node alive - " + rport);
                        break;
                    case JSONTYPE_RESETRESPONSE:
                        JSONObject resetPacket = new JSONObject(msg.getString(JSON_CONTENT));
                        //Log.e("Reset Received","From: "+msg.getString(JSON_SRC));
                        reset(resetPacket);
                        break;

                }
            } catch (JSONException e) {
                Log.e(TAG,"JSON Exception - Raised by " +
                        "onProgressUpdate at `JSONObject`");
                e.printStackTrace();
            }
        }
    }


}

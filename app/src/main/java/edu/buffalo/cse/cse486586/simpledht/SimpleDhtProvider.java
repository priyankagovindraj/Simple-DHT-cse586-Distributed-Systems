package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    String mynode;
    String portStr = null;
    String myPort = null;
    public Uri mUri=null;
    boolean flag=true;
    TreeMap<String,String> mapquery = new TreeMap<String,String>();
    Map<String,String> map = new HashMap<String, String>();
    List<Map.Entry<String,String>> entries = new ArrayList<Map.Entry<String, String>>(map.entrySet());
    Map<String,String> finalquery = new HashMap<String, String>();
    String successor="";
    String predecessor="";

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String SelQ = selection.replaceAll("\"","");
        if ((!SelQ.equals("@") && !SelQ.equals("*")))
        {
            try {
                getContext().deleteFile(SelQ);
                return 1;
            } catch (Exception e) {
                Log.e(TAG,"Delete File Exception");
                return 0;
            }
        }

        else if (SelQ.equals("@")){
            try {
                for(String s:getContext().fileList()) {
                    getContext().deleteFile(s);
                }
                return 1;
            } catch (Exception e) {
                Log.e(TAG,"delete exception");
                return 0;
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean partitionCheck(String query)
    {
        try {
            String pred = genHash(predecessor);
            String porst = genHash(portStr);
            String q = genHash(query);
            if(successor.equals(portStr) && portStr.equals(predecessor))
                return true;
            else if (q.compareTo(pred) > 0 && q.compareTo(porst) <= 0)
                 return true;
            else if(pred.compareTo(porst) > 0 && (((q.compareTo(porst) < 0) || (q.compareTo(porst) > 0))))
                return true;
            else
                return false;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,"No such algorithm");
        }
        return false;
    }

    /*public boolean foo() {

    }*/

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = (String)values.get("key");
        String value = (String)values.get("value");
        try {
            if(!partitionCheck(key)) {
                String Successorport="";
                Successorport=String.valueOf((Integer.parseInt(successor) * 2));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "M", Successorport, key, value);
            }
            else {
                if (insertioncheck(key, value))
                    Log.d("Inserted key successfully",key);
            }
        } catch (Exception e) {
            Log.e(TAG, "Insert exception");
        }
        return uri;
    }
    @Override
    public boolean onCreate() {
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            successor=portStr;
            predecessor=portStr;
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Executor e = Executors.newFixedThreadPool(10);
            new ServerTask().executeOnExecutor(e, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "J", myPort);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        FileInputStream in = null;
        MatrixCursor matrixCursor;
        String squery= selection.replaceAll("\"","");
        if (squery.equals("@")) {
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            for (String s : getContext().fileList()) {
                try {
                    in = getContext().openFileInput(s);
                InputStreamReader read = new InputStreamReader(in);
                BufferedReader buf1 = new BufferedReader(read);
                String key11 = s;
                String value11 = buf1.readLine();
                    matrixCursor.newRow().add(key11).add(value11);
                } catch (IOException e) {
                    Log.e(TAG,"File not found");
                }

            }
            return matrixCursor;
        }
        else if (squery.equals("*")) {
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            String queryStar = qstring();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "star",portStr,queryStar);
            flag=true;
            mynode = mynode.substring(0,mynode.length());
            String[] string =mynode.split(";");
            for(int i=0;i<string.length;i++)
            {
                String[] s = string[i].split("=");
                String key=s[0];
                String value=s[1];
                matrixCursor.newRow().add(key).add(value);
            }
            return matrixCursor;
        }
        else if((!squery.equals("@") && !squery.equals("*")))
        {

                if(partitionCheck(squery)) {
                    String fetchedQuery=checkproviderquery(squery);
                    String[] string=fetchedQuery.split(";");
                    String key=string[0];
                    String value=string[1];
                    matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                    matrixCursor.newRow().add(key).add(value);
                    return matrixCursor;
                }
                else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Q", squery, myPort);
                    flag=true;
                    String[] string=mynode.split(";");
                    String key=string[0];
                    String value=string[1];
                    matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                    matrixCursor.newRow().add(key).add(value);
                    return matrixCursor;
                }

        }

        return null;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            while (true) {
                try {
                    socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String read = in.readLine();
                    if (read.contains("join")) {
                        join(read);
                    }

                    if (read.contains("M")) {
                     message(read);
                    }
                    if (read.contains("finalQ")) {
                        mynode = read.split(":")[1];
                        flag = false;
                    }
                    if (read.contains("star")) {
                     star(read);
                    }
                    if (read.contains("Q")) {
                       fquery(read);
                    }
                    if (read.contains("Pred")) {
                       predecessor(read);
                    }
                    in.close();
                } catch (IOException e) {
                    Log.e(TAG, "Exception occurred in reading Inputstream");
                }
            }
        }
        public String Predecessor(String key)
        {
            String p="";
            for(String str:mapquery.keySet())
            {
                if(key==str)
                {
                    break;
                }
                else
                {
                    if(key.equals(mapquery.firstKey())){
                        Map.Entry<String,String> entry=mapquery.lastEntry();
                        String[] res=entry.getValue().split(":");
                        p=res[0];
                        break;
                    }
                    String[] result=mapquery.get(str).split(":");
                    p=result[0];
                }
            }
            return p;
        }
        public String Succeedor(String key)
        {
            String s="";
            Set keys = mapquery.keySet();
            for (Iterator i = keys.iterator(); i.hasNext();) {
                String str=(String)i.next();
                try {
                    if(str==key)
                    {
                        String hashKeyNext = (String) i.next();
                        String[] resultNext = (String[]) mapquery.get(hashKeyNext).split(":");
                        s=resultNext[0];
                        break;

                    }
                    else
                    {
                        if(str.equals(mapquery.lastKey())){
                            Map.Entry<String,String> entrySucc=mapquery.firstEntry();
                            String[] result=entrySucc.getValue().split(":");
                            s=result[0];
                            break;
                        }
                    }
                }catch (Exception e)
                {
                    Log.e(TAG,"Exception");
                }
            }
            return s;
        }
        public void join(String string) {
            Log.d(TAG, "portStr-->" + portStr);
            String[] res = string.split(":");
            String port = res[1];
            try {
                mapquery.put(genHash(port), port + ":" + successor + ":" + predecessor);
                 for (String str : mapquery.keySet()) {
                String[] value = mapquery.get(str).split(":");
                String portno = value[0];
                String Sccessor = Succeedor(str);
                String Predcessor = Predecessor(str);
                    finalquery.put(genHash(portno), portno + ":" + Sccessor + ":" + Predcessor);
                }
            }catch (NoSuchAlgorithmException e) {
                Log.e(TAG,"No Algo");
            }
            for (String str : finalquery.keySet()) {
                String[] res1 = finalquery.get(str).split(":");
                String portNo = String.valueOf((Integer.parseInt(res1[0]) * 2));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Pred", portNo, res1[1], res1[2]);
            }
        }
        public void message(String string) {
            String[] str = string.split(":");
            String key = str[1];
            String value = str[2];
            ContentValues values = new ContentValues();
            values.put("key", key);
            values.put("value", value);
            insert(mUri, values);
        }
        public void star(String string) {
                String[] str = string.split(":");
                int len = str.length;
                String originalSenderPort = str[1];
                String queryString = "";
                if (len <= 2) {
                    queryString = "";
                } else
                    queryString = str[2];
                if (!originalSenderPort.equals(portStr)) {
                    String queryForward = queryString + qstring();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "star", originalSenderPort, queryForward);
                } else {
                    mynode = queryString;
                    flag = false;
                }
        }
        public void fquery(String string) {
            String[] str = string.split(":");
            String query = str[1];
            String port = str[2];
            if (partitionCheck(query)) {
                String query1 = checkproviderquery(query);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "finalQ", port, query1);
            } else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Q", query, port);
            }
        }
        public void predecessor(String string) {
            String[] str = string.split(":");
            successor = str[1];
            predecessor = str[2];
        }


    }
    private class ClientTask extends AsyncTask<String, Void, Void> {
        // I used the PrintStream code from :http://www.tutorialspoint.com/javaexamples/net_singleuser.htm
        @Override
        protected Void doInBackground(String... msgs) {
                String msgToSend = msgs[0];
                switch (msgToSend.charAt(0)) {
                    case 'M':
                        clmsg(msgs);
                        break;
                    case 'Q':
                        clquery(msgs);
                        break;
                    case 's':
                        clstar(msgs);
                        break;
                    case 'f':
                        clfq(msgs);
                        break;
                    case 'J':
                        cljoin(msgToSend);
                        break;
                    case 'P':
                        clpred(msgToSend, msgs);
                        break;
                }

            return null;
        }
        public void clmsg (String ... msgs) {
            try {
                String port = msgs[1];
                String key = msgs[2];
                String value = msgs[3];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                PrintStream print = new PrintStream(socket.getOutputStream());
                String str = "M" + ":" + key + ":" + value;
                print.println(str);
                print.flush();
                print.close();
                socket.close();
            }catch (IOException e) {
                Log.e(TAG,"IOException");
            }
        }
        public void clquery (String ... msgs) {
            try {
                String query=msgs[1];
                String senderport=msgs[2];
                String successor=String.valueOf((Integer.parseInt(successor) * 2));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                    PrintStream print = new PrintStream(socket.getOutputStream());
                String str = "Q" + ":" + query + ":" + senderport;
                print.println(str);
                    print.flush();
                    print.close();
                    socket.close();
            }catch (IOException e) {
                Log.e(TAG,"IOException");
            }
        }
        public void clstar (String ... msgs) {
            try {
                String senderport= msgs[1];
                String queryString = msgs[2];
                String successor = String.valueOf((Integer.parseInt(successor) * 2));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                PrintStream print = new PrintStream(socket.getOutputStream());
                String str = "star" + ":" + senderport + ":" + queryString;
                print.println(str);
                print.flush();
                print.close();
                socket.close();
            } catch (IOException e) {
                Log.e(TAG, "IOException");
            }
        }
        public void clfq (String ... msgs) {
            try {
                String senderport=msgs[1];
                String queryResult=msgs[2];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(senderport));
                    PrintStream print = new PrintStream(socket.getOutputStream());
                String str = "fquery" + ":" + queryResult;
                print.println(str);
                    print.flush();
                    print.close();
                    socket.close();
            }catch (IOException e) {
                Log.e(TAG, "IOException");
            }
        }
        public void cljoin (String msg) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                    PrintStream print = new PrintStream(socket.getOutputStream());
                String str = msg+":"+portStr;
                print.println(str);
                    print.flush();
                    print.close();
                    socket.close();
            }catch (IOException e) {
                Log.e(TAG,"IOException");
            }
        }
        public void clpred (String msg, String ... msgs) {
            try {
                Log.d("InClientProcess","UpdateSuccPre");
                String myport=msgs[1];
                Log.d("portNoClient",myport);
                String successor=msgs[2];
                String predecessor=msgs[3];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myport));
                    PrintStream print = new PrintStream(socket.getOutputStream());
                String str = msg+":"+successor+":"+predecessor;
                print.println(str);
                    print.flush();
                    print.close();
                    socket.close();
            }catch (IOException e) {
                Log.e(TAG,"IOException");
            }
        }

    }
    public String checkproviderquery(String key)
    {
        FileInputStream inp = null;
        try {
            inp = getContext().openFileInput(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader buf1 = new BufferedReader(new InputStreamReader(inp));
        String value = null;
        try {
            value = buf1.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return key+";"+value;
    }
    public boolean insertioncheck(String key,String value)
    {
        FileOutputStream out;
        try {
            out = getContext().openFileOutput(key, getContext().MODE_PRIVATE);
            out.write(value.getBytes());
            out.close();
            return true;
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        return false;
    }
    public String qstring() {
        FileInputStream in = null;
        String query = "";
        for (String str : getContext().fileList()) {
            try {
                BufferedReader buf1 = new BufferedReader(new InputStreamReader(in));
                String keyq = str;
                String value = buf1.readLine();
                query = query + keyq + "=" + value + ";";

            } catch (IOException e) {
                Log.e(TAG, "IOException");
            }
        }
                return query;


    }
}

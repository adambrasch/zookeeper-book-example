/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.book;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private HashMap<String, Integer> keyMap;
    private HashMap<String, Integer> locationKeyMap;
    
    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;
    
    /**
     * Creates a new Worker instance.
     * 
     * @param hostPort 
     */
    public Worker(String hostPort) { 
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1, 1, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
    }
    
    /**
     * Creates a ZooKeeper session.
     * 
     * @throws IOException
     */
    public void startZK() throws IOException {
        //connects to zookeeper instance
        zk = new ZooKeeper(hostPort, 15000, this);
    }
    
    /**
     * Deals with session events like connecting
     * and disconnecting.
     * 
     * @param e new event generated
     */
    public void process(WatchedEvent e) {
        //Default watcher for this worker
        LOG.info(e.toString() + ", " + hostPort);
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                /*
                 * Registered with ZooKeeper
                 */
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                LOG.error("Session expired");
            default:
                break;
            }
        }
    }
    
    /**
     * Checks if this client is connected.
     * 
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }
    
    /**
     * Checks if ZooKeeper session is expired.
     * 
     * @return
     */
    public boolean isExpired() {
        return expired;
    }
    
    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap(){
        createAssignNode();
    }
    
    void createAssignNode(){
        //Creates node for this worker to be assigned tasks
        zk.create("/assign/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }
    
    StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                createAssignNode();
                break;
            case OK:
                LOG.info("Assign node created");
                break;
            case NODEEXISTS:
                LOG.warn("Assign node already registered");
                break;
            default:
                LOG.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    String name;

    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register(){
        name = "worker-" + serverId;
        //Adds this worker to list of active workers
        zk.create("/workers/" + name,
                "Idle".getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }
    
    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                register();
                
                break;
            case OK:
                LOG.info("Registered successfully: " + serverId);
                
                break;
            case NODEEXISTS:
                LOG.warn("Already registered: " + serverId);
                
                break;
            default:
                LOG.error("Something went wrong: ", 
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                updateStatus((String)ctx);
                return;
            }
        }
    };

    String status;
    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            //Updates the status of this worker (i.e. Idle or Working)
            zk.setData("/workers/" + name, status.getBytes(), -1,
                statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }
    
    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }
    /*
     *************************************** 
     ***************************************
     * Methods to wait for new assignments.*
     *************************************** 
     ***************************************
     */
    
    Watcher newTaskWatcher = new Watcher(){
        //Watcher which is called when a new task is added to this worker's assignment node
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-"+ serverId ).equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
        //Gets the children of this worker's assignment node, i.e. its new tasks
        zk.getChildren("/assign/worker-" + serverId, 
                newTaskWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
   
    protected ChildrenCache assignedTasksCache = new ChildrenCache();
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null){
                    executor.execute(new Runnable() {
                        List<String> children;
                        DataCallback cb;
                        
                        /*
                         * Initializes input of anonymous class
                         */
                        public Runnable init (List<String> children, DataCallback cb) {
                            this.children = children;
                            this.cb = cb;
                            
                            return this;
                        }
                        
                        public void run() {
                            if(children == null) {
                                return;
                            }
    
                            LOG.info("Looping into tasks");
                            setStatus("Working");
                            for(String task : children){
                                LOG.trace("New task: {}", task);
                                //Gets the tasks data for the given task, cb will process it
                                zk.getData("/assign/worker-" + serverId  + "/" + task,
                                        false,
                                        cb,
                                        task);   
                            }
                        }
                    }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                } 
                break;
            default:
                System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                //Gets the tasks data for the given task, cb will process it
                zk.getData(path, false, taskDataCallback, null);
                break;
            case OK:
                /*
                 *  Executing a task in this example is simply printing out
                 *  some string representing the task.
                 */
                executor.execute( new Runnable() {
                    byte[] data;
                    Object ctx;
                    
                    /*
                     * Initializes the variables this anonymous class needs
                     */
                    public Runnable init(byte[] data, Object ctx) {
                        this.data = data;
                        this.ctx = ctx;
                        
                        return this;
                    }
                    
                    public void run() {
                        String taskData = new String(data);
                        if(taskData.startsWith("Insert")) {
                            try {
                                insertKey(taskData, ctx);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        else if(taskData.startsWith("Retrieve")) {
                            retrieveKey(taskData,ctx);
                        }
                        else if(taskData.startsWith("Delete")) {
                            try {
                                deleteKey(taskData, ctx);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        else if(taskData.startsWith("Share")) {
                            try {
                                shareKey(taskData,ctx);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        else { //calculate
                            try {
                                calculate(taskData, ctx);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }.init(data, ctx));
                
                break;
            default:
                LOG.error("Failed to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    StringCallback taskCompletedCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                //Create the completed node for the given task
                zk.create(path,((String)ctx).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        taskCompletedCreateCallback, ctx);

                break;
            case OK:
                LOG.info("Created completed znode correctly: " + name);
                break;
            case NODEEXISTS:
                LOG.warn("Node exists: " + path);
                break;
            default:
                LOG.error("Failed to create task data: ", KeeperException.create(Code.get(rc), path));
            }
            
        }
    };
    
    VoidCallback taskVoidCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    /**
     * Closes the ZooKeeper session.
     */
    @Override
    public void close() 
            throws IOException
    {
        LOG.info( "Closing" );
        try{
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }
    
    /**
     * Main method showing the steps to execute a worker.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception { 
        Worker w = new Worker(args[0]);
        w.startZK();
        
        while(!w.isConnected()){
            Thread.sleep(100);
        }   
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();
        
        /*
         * Getting assigned tasks.
         */
        w.getTasks();
        
        while(!w.isExpired()){
            Thread.sleep(1000);
        }
        
    }

    public void insertKey(String data, Object ctx) throws KeeperException, InterruptedException {
        LOG.info("Executing your task: " + data);
        String[] newKeyValue = data.split(" ");
        int value = Integer.parseInt(newKeyValue[2]);
        keyMap.put(newKeyValue[1], value);
        String lockPath = writeLock();
        HashMap<String, String> hm = getHashMap();
        hm.put(newKeyValue[1], "worker-" + serverId);
        rewriteHashMap(hm);
        writeUnlock(lockPath);
        String statusMessage = "Key \"" + newKeyValue[1] + "\" added with value " + value + ".";
        //Create the completed node for this task for master to see and notify client
        zk.create("/completed/" + (String) ctx, statusMessage.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, taskCompletedCreateCallback, statusMessage);
        //Delete the assignment node for this task
        zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                -1, taskVoidCallback, null);
    }

    public void retrieveKey(String data, Object ctx) {
        LOG.info("Executing your task: " + data);
        String key = data.split(" ")[1];
        int value = keyMap.get(key);
        String statusMessage = "Key \"" + key + "\" has value " + value + ".";
        //Create the completed node for this task for master to see and notify client
        zk.create("/completed/" + (String) ctx, statusMessage.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, taskCompletedCreateCallback, statusMessage);
        //Delete the assignment node for this task
        zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                -1, taskVoidCallback, null);
    }

    public void deleteKey(String data, Object ctx) throws KeeperException, InterruptedException {
        LOG.info("Executing your task: " + data);
        String key = data.split(" ")[1];
        keyMap.remove(key);
        String statusMessage = "Key \"" + key + "\" removed.";
        String lockPath = writeLock();
        HashMap<String, String> hm = getHashMap();
        hm.remove(key);
        rewriteHashMap(hm);
        writeUnlock(lockPath);
        //Create the completed node for this task for master to see and notify client
        zk.create("/completed/" + (String) ctx, statusMessage.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, taskCompletedCreateCallback, statusMessage);
        //Delete the assignment node for this task
        zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                -1, taskVoidCallback, null);
    }

    public void shareKey(String data, Object ctx) throws KeeperException, InterruptedException {
        String key = data.split(" ")[1];
        String value = "" + keyMap.get(key);
        byte[] valueBytes = value.getBytes();
        //Create node under the /keys dir containing value of the particular key that was requested
        zk.create("/keys/" + key, valueBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void calculate(String data, Object ctx) throws KeeperException, InterruptedException {
        int operand1 = 0;
        int operand2 = 0;
        String[] operatorAndOperands = data.split(" ");
        String operator = operatorAndOperands[1];
        String key1 = operatorAndOperands[2];
        boolean isInt1 = isInteger(key1);
        if(isInt1) {
            operand1 = Integer.parseInt(key1);
        }
        String key2 = operatorAndOperands[3];
        boolean isInt2 = isInteger(key2);
        if(isInt2) {
            operand2 = Integer.parseInt(key2);
        }
        int result = 0;
        //check local hashmap for keys
        boolean contains1 = keyMap.containsKey(key1);
        boolean contains2 = keyMap.containsKey(key2);
        HashMap<String, String> workerMap = null;
        String readLockPath = "";
        if((!contains1 && !isInt1) || (!contains2 && !isInt2)) {
            readLockPath = readLock();
            workerMap = getHashMap();
        }
        if(!contains1 && !isInt1) {
            String worker1 = workerMap.get(key1);
            operand1 = getForeignKeyValue(worker1, key1);
        }
        else if(!isInt1) {
            operand1 = keyMap.get(key1);
        }
        if(!contains2 && !isInt2) {
            String worker2 = workerMap.get(key2);
            operand2 = getForeignKeyValue(worker2, key2);
        }
        else if(!isInt2) {
            operand2 = keyMap.get(key2);
        }
        if(!contains1 || !contains2) {
            readUnlock(readLockPath);
        }
        switch(operator) {
            case "+":
                result = operand1 + operand2;
                break;
            case "-":
                result = operand1 - operand2;
                break;
            case "*":
                result = operand1 * operand2;
                break;
            case "/":
                result = operand1 / operand2;
                break;
        }
        if(operatorAndOperands.length > 4) {
            String[] newCalc = makeNewCalcArray(operatorAndOperands, result);
            String calcString = String.join(" ", newCalc);
            //Recreate task node for master to reassign
            zk.create("/tasks/" + (String)ctx, calcString.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, calcString);
        }
        else {
            String statusMessage = "Calculation completed with value of " + result;
            //create completed node as calculation is completed.
            zk.create("/completed/" + (String)ctx, statusMessage.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, taskCompletedCreateCallback, statusMessage);
        }
        //Delete assignment node from under this worker
        zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                -1, taskVoidCallback, null);

    }

    StringCallback recreateTaskCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path, ((String)ctx).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, ctx);
                    break;
                case OK:
                    LOG.info("Task node recreated after subcalculation");
                    break;
                default:
                    LOG.error("Error occured while trying to create task node after subcalculation");
            }
        }
    };

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch(NumberFormatException e) {
            return false;
        } catch(NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    public int getForeignKeyValue(String worker, String key) throws KeeperException, InterruptedException {
        String dataString = "Share " + key;
        //Create a task for the worker with the given key assigning it to share the key
        zk.create("/assign/" + worker + "/share-", dataString.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        //Check if the shared key node exists
        Stat stat = zk.exists("/keys/" + key, false);
        while(stat == null) {
            Thread.sleep(1000);
            stat = zk.exists("/keys/" + key, false);
        }
        //Get the value of the key from the shared key node
        int operand = Integer.parseInt(new String(zk.getData("/keys/" + key, false, null)));
        return operand;
    }

    public String[] makeNewCalcArray(String[] current, int result) {
        String[] newCalc = new String[current.length - 1];
        newCalc[0] = current[0];
        newCalc[1] = current[1];
        newCalc[2] = "" + result;
        for(int i = 3; i < newCalc.length; i++) {
            newCalc[i] = current[i + 1];
        }
        return newCalc;
    }

    public String readLock() throws KeeperException, InterruptedException {
        //Create a new read node under the lock
        String newLock = zk.create("_locknode_/read-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        int lockNum = Integer.parseInt(newLock.split("-")[1]);
        //Get all the children of the lock in order to see if I can read lock the resource
        List<String> locks = zk.getChildren("_locknode", false);
        boolean writeLocked = false;
        int writeNum = Integer.MIN_VALUE;
        for(String lock: locks) {
            int currentNum;
            if(lock.startsWith("write") && (currentNum = Integer.parseInt(lock.split("-")[1])) < lockNum) {
                writeLocked = true;
                if(currentNum > writeNum) {
                    writeNum = currentNum;
                }
            }
        }
        if(writeLocked) {
            //Observe the write lock node and see if it gets deleted
            Stat stat = zk.exists("_locknode_/write-" + writeNum, false);
            while(stat != null) {
                Thread.sleep(1000);
                stat = zk.exists("_locknode_/write-" + writeNum, false);
            }
            return newLock;
        }
        else {
            return newLock;
        }
    }

    public void readUnlock(String nodePath) throws KeeperException, InterruptedException {
        //Delete my read lock node, thereby releasing my read lock
        zk.delete(nodePath, -1);
    }

    public String writeLock() throws KeeperException, InterruptedException {
        //Create a new write lock node
        String newLock = zk.create("_locknode_/write-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        int lockNum = Integer.parseInt(newLock.split("-")[1]);
        //Get all the other lock nodes to determine if I can lock the resource
        List<String> locks = zk.getChildren("_locknode", false);
        boolean readLocked = false;
        int readNum = Integer.MIN_VALUE;
        for(String lock: locks) {
            int currentNum;
            if(lock.startsWith("read") && (currentNum = Integer.parseInt(lock.split("-")[1])) < lockNum) {
                readLocked = true;
                if(currentNum > readNum) {
                    readNum = currentNum;
                }
            }
        }
        if(readLocked) {
            //Check if the lower numbered lock node exists. If not, acquire the lock and access the resource
            Stat stat = zk.exists("_locknode_/read-" + readNum, false);
            while(stat != null) {
                Thread.sleep(1000);
                stat = zk.exists("_locknode_/read-" + readNum, false);
            }
            return newLock;
        }
        else {
            return newLock;
        }
    }

    public void writeUnlock(String nodePath) throws KeeperException, InterruptedException {
        //Delete my write lock node, thereby releasing my write lock
        zk.delete(nodePath, -1);
    }

    public HashMap<String, String> getHashMap() throws KeeperException, InterruptedException {
        SerializationUtils su = new SerializationUtils();
        //Get the data in the /keys node, which contains  a hashmap of keys and the workers which store them
        byte[] bytes = zk.getData("/keys", false, null);
        HashMap<String, String> hm = su.deserialize(bytes);
        return hm;
    }

    public void rewriteHashMap(HashMap<String, String> hm) throws KeeperException, InterruptedException {
        SerializationUtils su = new SerializationUtils();
        byte[] bytes = su.serialize(hm);
        //Set the data on the /keys node to the updated hashMap.
        zk.setData("/keys", bytes, -1);
    }
    
}

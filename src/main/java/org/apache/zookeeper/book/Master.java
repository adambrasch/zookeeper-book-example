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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
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
import org.apache.zookeeper.KeeperException.Code;

import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the master of the master-worker example we use
 * throughout the book. The master is responsible for tracking the list of
 * available workers, determining when there are new tasks and assigning
 * them to available workers. 
 * 
 * The flow without crashes is like this. The master reads the list of
 * available workers and watch for changes to the list of workers. It also
 * reads the list of tasks and watches for changes to the list of tasks.
 * For each new task, it assigns the task to a worker chosen at random.
 * 
 * Before exercising the role of master, this ZooKeeper client first needs
 * to elect a primary master. It does it by creating a /master znode. If
 * it succeeds, then it exercises the role of master. Otherwise, it watches
 * the /master znode, and if it goes away, it tries to elect a new primary
 * master.
 * 
 * The states of this client are three: RUNNING, ELECTED, NOTELECTED. 
 * RUNNING means that according to its view of the ZooKeeper state, there
 * is no primary master (no master has been able to acquire the /master lock).
 * If some master succeeds in creating the /master znode and this master learns
 * it, then it transitions to ELECTED if it is the primary and NOTELECTED
 * otherwise.
 *   
 * Because workers may crash, this master also needs to be able to reassign
 * tasks. When it watches for changes in the list of workers, it also 
 * receives a notification when a znode representing a worker is gone, so 
 * it is able to reassign its tasks.
 * 
 * A primary may crash too. In the case a primary crashes, the next primary
 * that takes over the role needs to make sure that it assigns and reassigns
 * tasks that the previous primary hasn't had time to process.
 *
 */
public class Master implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    
    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.  
     */
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private volatile MasterStates state = MasterStates.RUNNING;
    
    MasterStates getState() {
        return state;
    }
    
    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString( random.nextInt() );
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    
    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;
    protected ChildrenCache completedCache;
    
    /**
     * Creates a new master instance.
     * 
     * @param hostPort
     */
    Master(String hostPort) { 
        this.hostPort = hostPort;
    }
    
    
    /**
     * Creates a new ZooKeeper session.
     * 
     * @throws IOException
     */
    void startZK() throws IOException {
        //Connects to the zookeeper server
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        //Closes the connection to the zookeeper server
        zk.close();
    }
    
    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session. 
     * 
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {  
        LOG.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                LOG.error("Session expiration");
            default:
                break;
            }
        }
    }
    
    
    /**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
     */
    public void bootstrap(){
        SerializationUtils su = new SerializationUtils();
        HashMap<String, String> workerMap = new HashMap<>();
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
        createParent("/completed", new byte[0]);
        createParent("/keys", su.serialize(workerMap));
        createParent("_locknode_", new byte[0]);
    }
    
    void createParent(String path, byte[] data){
        //Creates a parent directory node
        zk.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                createParentCallback, 
                data);
    }
    
    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                createParent(path, (byte[]) ctx);
                
                break;
            case OK:
                LOG.info("Parent created");
                
                break;
            case NODEEXISTS:
                LOG.warn("Parent already registered: " + path);
                
                break;
            default:
                LOG.error("Something went wrong: ", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
        
    /**
     * Check if this client is connected.
     * 
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }
    
    /**
     * Check if the ZooKeeper session has expired.
     * 
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }

    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */
    
    
    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of. 
     * 
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again. 
     * 
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
    StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                checkMaster();
                
                break;
            case OK:
                state = MasterStates.ELECTED;
                takeLeadership();

                break;
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();
                
                break;
            default:
                state = MasterStates.NOTELECTED;
                LOG.error("Something went wrong when running for master.", 
                        KeeperException.create(Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    void masterExists() {
        //Sets a watch on the master node
        zk.exists("/master", 
                masterExistsWatcher, 
                masterExistsCallback, 
                null);
    }
    
    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                masterExists();
                
                break;
            case OK:
                break;
            case NONODE:
                state = MasterStates.RUNNING;
                runForMaster();
                LOG.info("It sounds like the previous master is gone, " +
                    		"so let's run for master again."); 
                
                break;
            default:     
                checkMaster();
                break;
            }
        }
    };
    
    Watcher masterExistsWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeDeleted) {
                assert "/master".equals( e.getPath() );
                
                runForMaster();
            }
        }
    };
    
    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();
        
        (new RecoveredAssignments(zk)).recover( new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == RecoveryCallback.FAILED) {
                    LOG.error("Recovery of assigned tasks failed.");
                } else {
                    LOG.info( "Assigning recovered tasks" );
                    getTasks();
                }
            }
        });
    }
    
    /*
     * Run for master. To run for master, we try to create the /master znode,
     * with masteCreateCallback being the callback implementation. 
     * In the case the create call succeeds, the client becomes the master.
     * If it receives a CONNECTIONLOSS event, then it needs to check if the 
     * znode has been created. In the case the znode exists, it needs to check
     * which server is the master.
     */
    
    /**
     * Tries to create a /master lock znode to acquire leadership.
     */
    public void runForMaster() {
        LOG.info("Running for master");
        //Creates master node. If succesful, take over as master. If already exists, not master
        zk.create("/master", 
                serverId.getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);
    }
        
    DataCallback masterCheckCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                
                break;
            case NONODE:
                runForMaster();
                
                break; 
            case OK:
                if( serverId.equals( new String(data) ) ) {
                    state = MasterStates.ELECTED;
                    takeLeadership();
                } else {
                    state = MasterStates.NOTELECTED;
                    masterExists();
                }
                
                break;
            default:
                LOG.error("Error when reading data.", 
                        KeeperException.create(Code.get(rc), path));               
            }
        } 
    };
        
    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    } //Retrieves node data of master node to determine which server is the master
    
    /*
     ****************************************************
     **************************************************** 
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */
    
    
    /**
     * This method is here for testing purposes.
     * 
     * @return size Size of the worker list
     */
    public int getWorkersSize(){
        if(workersCache == null) {
            return 0;
        } else {
            return workersCache.getList().size();
        }
    }
    
    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/workers".equals( e.getPath() );
                
                getWorkers();
            }
        }
    };
    
    void getWorkers(){
        //Gets children of /workers node, all active worker nodes
        zk.getChildren("/workers", 
                workersChangeWatcher, 
                workersGetChildrenCallback, 
                null);
    }
    
    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("Succesfully got a list of workers: " 
                        + children.size() 
                        + " workers");
                reassignAndSet(children);
                break;
            default:
                LOG.error("getChildren failed",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */
    
    void reassignAndSet(List<String> children){
        List<String> toProcess;
        
        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info( "Removing and setting" );
            toProcess = workersCache.removedAndSet( children );
        }
        
        if(toProcess != null) {
            for(String worker : toProcess){
                getAbsentWorkerTasks(worker);
            }
        }
    }
    
    void getAbsentWorkerTasks(String worker){
        //Gets assigned tasks of worker that has been determined to have died.
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }
    
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                
                break;
            case OK:
                LOG.info("Succesfully got a list of assignments: " 
                        + children.size() 
                        + " tasks");
                
                /*
                 * Reassign the tasks of the absent worker.  
                 */
                
                for(String task: children) {
                    getDataReassign(path + "/" + task, task);                    
                }
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. * 
     ************************************************
     */
    
    /**
     * Get reassigned task data.
     * 
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        //Get data of task from dead worker
        zk.getData(path, 
                false, 
                getDataReassignCallback, 
                task);
    }
    
    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx); 
                
                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
                
                break;
            default:
                LOG.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        //Recreates the task which was recovered from dead worker, process of task assignment begins again.
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }
    
    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);
                
                break;
            case NODEEXISTS:
                LOG.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);
                
                break;
            default:
                LOG.error("Something wwnt wrong when recreating task", 
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
        zk.delete(path, -1, taskDeletionCallback, null);
    } //Deletes old task assigned to dead worker

    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */
      
    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
        //Gets all tasks and sets watcher to watch for more
        zk.getChildren("/tasks", 
                tasksChangeWatcher, 
                tasksGetChildrenCallback, 
                null);
    }

    //TODO add completed watcher method
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
                List<String> toProcess;
                if(tasksCache == null) {
                    tasksCache = new ChildrenCache(children);
                    
                    toProcess = children;
                } else {
                    toProcess = tasksCache.addedAndSet( children );
                }
                
                if(toProcess != null){
                    assignTasks(toProcess);
                } 
                
                break;
            default:
                LOG.error("getChildren failed.",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void assignTasks(List<String> tasks) {
        for(String task : tasks){
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        //Get data for a particular task
        zk.getData("/tasks/" + task, 
                false, 
                taskDataCallback, 
                task);
    }
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                
                break;
            case OK:
                /*
                 * Choose worker depending on the task
                 */
                String designatedWorker = "";
                String taskData = new String(data);
                if(taskData.startsWith("Insert") || taskData.startsWith("Calculate")) {
                    List<String> list = workersCache.getList();
                    designatedWorker = list.get(random.nextInt(list.size()));
                }
                else {
                    String key = taskData.split(" ")[1];
                    try {
                        String nodePath = readLock();
                        HashMap<String,String> workersMap = getHashMap();
                        designatedWorker = workersMap.get(key);
                        readUnlock(nodePath);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                
                /*
                 * Assign task to randomly chosen or determined worker.
                 */
                String assignmentPath = "/assign/" + 
                        designatedWorker + 
                        "/" + 
                        (String) ctx;
                LOG.info( "Assignment path: " + assignmentPath );
                createAssignment(assignmentPath, data);
                
                break;
            default:
                LOG.error("Error when trying to get task data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void createAssignment(String path, byte[] data){
        //Created a task node under the /assign/worker node of the assigned worker
        zk.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                assignTaskCallback, 
                data);
    }
    
    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                
                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                //Task assigned, can be deleted from /tasks node
                deleteTask(name.substring( name.lastIndexOf("/") + 1));
                
                break;
            case NODEEXISTS: 
                LOG.warn("Task already assigned");
                
                break;
            default:
                LOG.error("Error when trying to assign task.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name){
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    } //Deletes the task node under /tasks
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);
                
                break;
            case OK:
                LOG.info("Successfully deleted " + path);
                
                break;
            case NONODE:
                LOG.info("Task has been deleted already");
                
                break;
            default:
                LOG.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    Watcher completedChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/completed".equals( e.getPath() );

                getCompleted();
            }
        }
    };

    void getCompleted(){
        //Gets all tasks and sets watcher to watch for more
        zk.getChildren("/completed",
                completedChangeWatcher,
                completedGetChildrenCallback,
                null);
    }

    ChildrenCallback completedGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getCompleted();

                    break;
                case OK:
                    List<String> toProcess;
                    if(completedCache == null) {
                        completedCache = new ChildrenCache(children);

                        toProcess = children;
                    } else {
                        toProcess = completedCache.addedAndSet( children );
                    }

                    if(toProcess != null){
                        makeStatus(toProcess);
                    }

                    break;
                default:
                    LOG.error("getChildren failed.",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void makeStatus(List<String> completedTasks) {
        for(String completed : completedTasks){
            statusNode(completed);
        }
    }

    void statusNode(String completedTask) {
        //Get data for a particular completed task
        zk.getData("/completed/" + completedTask,
                false,
                completedDataCallback,
                completedTask);
    }

    DataCallback completedDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    statusNode((String) ctx);

                    break;
                case OK:
                    path.replace("/completed/", "/status/");
                    createStatus(path, data, ctx);
                default:
                    LOG.error("Error when trying to get completed data.",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void createStatus(String path, byte[] data, Object ctx) {
        //Create the status node for a given task
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, statusCreatedCallback, ctx);
    }

    StringCallback statusCreatedCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    createStatus(path, new byte[0], ctx);
                    break;
                case OK:
                    path.replace("/status/", "/completed/");
                    deleteCompleted(path);
                    LOG.info("Status node created");
                    break;
                default:
                    LOG.error("Error while trying to create status node.");
            }
        }
    };

    void deleteCompleted(String path) {
        //Delete the completed node for a given task
        zk.delete(path, -1, deleteCompletedCallback, null);
    }

    VoidCallback deleteCompletedCallback = new VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteCompleted(path);
                    break;
                case OK:
                    LOG.info("Completed Node Deleted Succesfully");
                    break;
                default:
                    LOG.error("Error while trying to delete completed node");
            }
        }
    };
    
    /**
     * Closes the ZooKeeper session. 
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if(zk != null) {
            try{
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn( "Interrupted while closing ZooKeeper session.", e );
            }
        }
    }
    
    /**
     * Main method providing an example of how to run the master.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception { 
        Master m = new Master(args[0]);
        m.startZK();
        
        while(!m.isConnected()){
            Thread.sleep(100);
        }
        /*
         * bootstrap() creates some necessary znodes.
         */
        m.bootstrap();
        
        /*
         * now runs for master.
         */
        m.runForMaster();
        
        while(!m.isExpired()){
            Thread.sleep(1000);
        }   

        m.stopZK();
    }

    public String readLock() throws KeeperException, InterruptedException {
        //Creates read lock node
        String newLock = zk.create("_locknode_/read-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        int lockNum = Integer.parseInt(newLock.split("-")[1]);
        //Gets all children of lock node to determine if the resource can be acquired
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
            //Waits for current lock node to be deleted, check for existence
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
        //Deletes my lock node, effectively freeing resource
        zk.delete(nodePath, -1);
    }

    public String writeLock() throws KeeperException, InterruptedException {
        //Creates a write lock node under the /locknode node in an attempt to acquire resource
        String newLock = zk.create("_locknode_/write-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        int lockNum = Integer.parseInt(newLock.split("-")[1]);
        //Gets a list of all children of the lock node in order to determine if it is able to acquire resource
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
            //Checks for existence of current lock node to know when it can acquire resource
            Stat stat = zk.exists("_locknode_/read-" + readNum, true);
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
        //Deletes my lock node effectively freeing resource
        zk.delete(nodePath, -1);
    }

    public HashMap<String, String> getHashMap() throws KeeperException, InterruptedException {
        SerializationUtils su = new SerializationUtils();
        //gets key-worker hashmap from /keys node (currently serialized
        byte[] bytes = zk.getData("/keys", false, null);
        HashMap<String, String> hm = su.deserialize(bytes);
        return hm;
    }

    public void rewriteHashMap(HashMap<String, String> hm) throws KeeperException, InterruptedException {
        SerializationUtils su = new SerializationUtils();
        byte[] bytes = su.serialize(hm);
        //sets serialized version of updated key-worker hashmap as the data of the /keys node
        zk.setData("/keys", bytes, -1);
    }
}

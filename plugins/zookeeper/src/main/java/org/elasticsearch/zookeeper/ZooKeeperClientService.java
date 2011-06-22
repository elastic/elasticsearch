/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author imotov
 */
public class ZooKeeperClientService extends AbstractLifecycleComponent<ZooKeeperClient> implements ZooKeeperClient {

    private ZooKeeper zooKeeper;

    private final ZooKeeperEnvironment environment;

    private final ZooKeeperFactory zooKeeperFactory;

    private final Lock sessionRestartLock = new ReentrantLock();

    private final CopyOnWriteArrayList<SessionResetListener> sessionResetListeners = new CopyOnWriteArrayList<SessionResetListener>();

    @Inject public ZooKeeperClientService(Settings settings, ZooKeeperEnvironment environment, ZooKeeperFactory zooKeeperFactory) {
        super(settings);
        this.environment = environment;
        this.zooKeeperFactory = zooKeeperFactory;
    }

    @Override protected void doStart() throws ElasticSearchException {
        try {
            zooKeeper = zooKeeperFactory.newZooKeeper();
            createPersistentNode(environment.rootNodePath());
        } catch (InterruptedException e) {
            throw new ZooKeeperClientException("Cannot start ZooKeeper client", e);
        }
    }

    @Override protected void doStop() throws ElasticSearchException {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                // Ignore
            }
            zooKeeper = null;
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void createPersistentNode(final String path) throws InterruptedException {
        if (!path.startsWith("/")) {
            throw new ZooKeeperClientException("Path " + path + " doesn't start with \"/\"");
        }
        try {
            zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    String[] nodes = path.split("/");
                    String currentPath = "";
                    for (int i = 1; i < nodes.length; i++) {
                        currentPath = currentPath + "/" + nodes[i];
                        if (zooKeeper.exists(currentPath, null) == null) {
                            try {
                                zooKeeper.create(currentPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            } catch (KeeperException.NodeExistsException e) {
                                // Ignore - node was created between our check and attempt to create it
                            }
                        }
                    }
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot create node at " + path, e);
        }

    }

    @Override public void setOrCreatePersistentNode(final String path, final byte[] data) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("setOrCreatePersistentNode is called after service was stopped");
        }
        try {
            zooKeeperCall("Cannot publish state", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    if (zooKeeper.exists(path, null) != null) {
                        zooKeeper.setData(path, data, -1);
                    } else {
                        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    return null;
                }
            });

        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot publish state", e);
        }
    }

    @Override public void setOrCreateTransientNode(final String path, final byte[] data) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("setOrCreateTransientNode is called after service was stopped");
        }
        try {
            try {
                zooKeeperCall("Creating node " + path, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        return null;
                    }
                });
            } catch (KeeperException.NodeExistsException e1) {
                // Ignore
            }
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot create node " + path, e);
        }
    }


    @Override public byte[] getOrCreateTransientNode(final String path, final byte[] data, final NodeListener nodeListener) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("getOrCreateTransientNode is called after service was stopped");
        }
        while (true) {
            try {
                // First, we try to obtain existing node
                return zooKeeperCall("Getting master data", new Callable<byte[]>() {
                    @Override public byte[] call() throws Exception {
                        return zooKeeper.getData(path, wrapNodeListener(nodeListener), null);
                    }
                });
            } catch (KeeperException.NoNodeException e) {
                try {
                    // If node doesn't exist - we try to create the node and return data without setting the
                    // watcher
                    zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            return null;
                        }
                    });
                    return data;
                } catch (KeeperException.NodeExistsException e1) {
                    // If node is already created - we will try to read created node on the next iteration of the loop
                } catch (KeeperException e1) {
                    throw new ZooKeeperClientException("Cannot create node " + path, e1);
                }
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot obtain node" + path, e);
            }
        }
    }

    @Override public byte[] getNode(final String path, final NodeListener nodeListener) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("getNode is called after service was stopped");
        }

        // If the node doesn't exist, we will use createdWatcher to wait for its appearance
        // If the node exists, we will use deletedWatcher to monitor its disappearance
        final Watcher watcher = wrapNodeListener(nodeListener);
        while (true) {
            try {
                // First we check if the node exists and set createdWatcher
                Stat stat = zooKeeperCall("Checking if node exists", new Callable<Stat>() {
                    @Override public Stat call() throws Exception {
                        return zooKeeper.exists(path, watcher);
                    }
                });

                // If the node exists, returning the current node data
                if (stat != null) {
                    return zooKeeperCall("Getting node data", new Callable<byte[]>() {
                        @Override public byte[] call() throws Exception {
                            return zooKeeper.getData(path, watcher, null);
                        }
                    });
                } else {
                    return null;
                }
            } catch (KeeperException.NoNodeException e) {
                // Node disappeared between exists() and getData() calls
                // We will try again
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot obtain node " + path, e);
            }
        }
    }

    @Override public Set<String> listNodes(final String path, final NodeListChangedListener nodeListChangedListener) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("listNodes is called after service was stopped");
        }
        Set<String> res = new HashSet<String>();
        final Watcher watcher = (nodeListChangedListener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                            nodeListChangedListener.onNodeListChanged();
                        }
                    }
                } : null;
        try {

            List<String> children = zooKeeperCall("Cannot list nodes", new Callable<List<String>>() {
                @Override public List<String> call() throws Exception {
                    return zooKeeper.getChildren(path, watcher);
                }
            });

            if (children == null) {
                return null;
            }
            for (String childPath : children) {
                res.add(extractLastPart(childPath));
            }
            return res;
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot list nodes", e);
        }
    }

    @Override public void deleteNode(final String path) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("deleteNode is called after service was stopped");
        }
        try {
            zooKeeperCall("Cannot delete node", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    zooKeeper.delete(path, -1);
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot delete node" + path, e);
        }
    }

    @Override public void addSessionResetListener(SessionResetListener sessionResetListener) {
        sessionResetListeners.add(sessionResetListener);
    }

    @Override public void removeSessionResetListener(SessionResetListener sessionResetListener) {
        sessionResetListeners.remove(sessionResetListener);
    }

    @Override public boolean connected() {
        return zooKeeper != null && zooKeeper.getState().isAlive();
    }

    @Override public long sessionId() {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("sessionId is called after service was stopped");
        }
        return zooKeeper.getSessionId();
    }

    private void resetSession() {
        sessionRestartLock.lock();
        try {
            if (lifecycle.started()) {
                if (!connected()) {
                    logger.info("Restarting ZooKeeper discovery");
                    try {
                        logger.trace("Stopping ZooKeeper");
                        doStop();
                    } catch (Exception ex) {
                        logger.error("Error stopping ZooKeeper", ex);
                    }
                    while (lifecycle.started()) {
                        try {
                            logger.trace("Starting ZooKeeper");
                            doStart();
                            logger.trace("Started ZooKeeper");
                            notifySessionReset();
                            return;
                        } catch (ZooKeeperClientException ex) {
                            if (ex.getCause() != null && ex.getCause() instanceof InterruptedException) {
                                logger.info("ZooKeeper startup was interrupted", ex);
                                return;
                            }
                            logger.warn("Error starting ZooKeeper ", ex);
                        }
                    }
                } else {
                    logger.trace("ZooKeeper is already restarted. Ignoring");
                }

            }
        } finally {
            sessionRestartLock.unlock();
        }

    }

    private void notifySessionReset() {
        for (SessionResetListener listener : sessionResetListeners) {
            listener.sessionReset();
        }
    }

    private <T> T zooKeeperCall(String reason, Callable<T> callable) throws InterruptedException, KeeperException {
        while (true) {
            try {
                return callable.call();
            } catch (KeeperException.ConnectionLossException ex) {
                // Retry
            } catch (KeeperException.SessionExpiredException e) {
                resetSession();
                throw new ZooKeeperClientSessionExpiredException(reason, e);
            } catch (KeeperException e) {
                throw e;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                throw new ZooKeeperClientException(reason, e);
            }
        }
    }


    private Watcher wrapNodeListener(final NodeListener nodeListener) {
        if (nodeListener != null) {
            return new Watcher() {
                @Override public void process(WatchedEvent event) {
                    if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                        nodeListener.onNodeCreated(event.getPath());
                    } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                        nodeListener.onNodeDeleted(event.getPath());
                    } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                        nodeListener.onNodeDataChanged(event.getPath());
                    }
                }
            };
        } else {
            return null;
        }
    }

    private String extractLastPart(String path) {
        int index = path.lastIndexOf('/');
        if (index >= 0) {
            return path.substring(index + 1);
        } else {
            return path;
        }
    }


}

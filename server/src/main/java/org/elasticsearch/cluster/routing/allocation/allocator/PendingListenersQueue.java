/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import static org.elasticsearch.cluster.service.ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME;
import static org.elasticsearch.cluster.service.MasterService.MASTER_UPDATE_THREAD_NAME;

public class PendingListenersQueue {

    private static final Logger logger = LogManager.getLogger(PendingListenersQueue.class);

    private record PendingListener(long index, ActionListener<Void> listener) {}

    private final ThreadPool threadPool;
    private final Queue<PendingListener> pendingListeners = new LinkedList<>();
    private volatile long completedIndex = -1;

    public PendingListenersQueue(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void add(long index, ActionListener<Void> listener) {
        synchronized (pendingListeners) {
            pendingListeners.add(new PendingListener(index, listener));
        }
    }

    public void complete(long convergedIndex) {
        assert MasterService.assertMasterUpdateOrTestThread();
        synchronized (pendingListeners) {
            if (convergedIndex > completedIndex) {
                completedIndex = convergedIndex;
            }
        }
        executeListeners(completedIndex, true);
    }

    public void completeAllAsNotMaster() {
        assert ThreadPool.assertCurrentThreadPool(MASTER_UPDATE_THREAD_NAME, CLUSTER_UPDATE_THREAD_NAME);
        completedIndex = -1;
        executeListeners(Long.MAX_VALUE, false);
    }

    public long getCompletedIndex() {
        return completedIndex;
    }

    private void executeListeners(long convergedIndex, boolean isMaster) {
        var listeners = pollListeners(convergedIndex);
        if (listeners.isEmpty() == false) {
            threadPool.generic().execute(() -> {
                if (isMaster) {
                    ActionListener.onResponse(listeners, null);
                } else {
                    ActionListener.onFailure(listeners, new NotMasterException("no longer master"));
                }
            });
        }
    }

    private Collection<ActionListener<Void>> pollListeners(long maxIndex) {
        var listeners = new ArrayList<ActionListener<Void>>();
        PendingListener listener;
        synchronized (pendingListeners) {
            while ((listener = pendingListeners.peek()) != null && listener.index <= maxIndex) {
                listeners.add(pendingListeners.poll().listener);
            }
            logger.trace("Polled listeners up to [{}]. Poll {}, remaining {}", maxIndex, listeners, pendingListeners);
        }
        return listeners;
    }
}

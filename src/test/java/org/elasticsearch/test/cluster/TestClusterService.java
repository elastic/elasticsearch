/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.test.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;

/** a class that simulate simple cluster service features, like state storage and listeners */
public class TestClusterService implements ClusterService {

    volatile ClusterState state;
    private final Collection<ClusterStateListener> listeners = new CopyOnWriteArrayList<>();
    private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue();
    private final ThreadPool threadPool;

    public TestClusterService() {
        this(ClusterState.builder(new ClusterName("test")).build());
    }

    public TestClusterService(ThreadPool threadPool) {
        this(ClusterState.builder(new ClusterName("test")).build(), threadPool);
    }

    public TestClusterService(ClusterState state) {
        this(state, null);
    }

    public TestClusterService(ClusterState state, @Nullable ThreadPool threadPool) {
        if (state.getNodes().size() == 0) {
            state = ClusterState.builder(state).nodes(
                    DiscoveryNodes.builder()
                            .put(new DiscoveryNode("test_id", DummyTransportAddress.INSTANCE, Version.CURRENT))
                            .localNodeId("test_id")).build();
        }

        assert state.getNodes().localNode() != null;
        this.state = state;
        this.threadPool = threadPool;

    }


    /** set the current state and trigger any registered listeners about the change */
    public void setState(ClusterState state) {
        assert state.getNodes().localNode() != null;
        // make sure we have a version increment
        state = ClusterState.builder(state).version(this.state.version() + 1).build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", state, this.state);
        this.state = state;
        for (ClusterStateListener listener : listeners) {
            listener.clusterChanged(event);
        }
    }

    /** set the current state and trigger any registered listeners about the change */
    public void setState(ClusterState.Builder state) {
        setState(state.build());
    }

    @Override
    public DiscoveryNode localNode() {
        return state.getNodes().localNode();
    }

    @Override
    public ClusterState state() {
        return state;
    }

    @Override
    public void addInitialStateBlock(ClusterBlock block) throws ElasticsearchIllegalStateException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void removeInitialStateBlock(ClusterBlock block) throws ElasticsearchIllegalStateException {
        throw new UnsupportedOperationException();

    }

    @Override
    public OperationRouting operationRouting() {
        return null;
    }

    @Override
    public void addFirst(ClusterStateListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLast(ClusterStateListener listener) {
        listeners.add(listener);
    }

    @Override
    public void add(ClusterStateListener listener) {
        listeners.add(listener);
    }

    @Override
    public void remove(ClusterStateListener listener) {
        listeners.remove(listener);
        for (Iterator<NotifyTimeout> it = onGoingTimeouts.iterator(); it.hasNext(); ) {
            NotifyTimeout timeout = it.next();
            if (timeout.listener.equals(listener)) {
                timeout.cancel();
                it.remove();
            }
        }
    }

    @Override
    public void add(LocalNodeMasterListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(LocalNodeMasterListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(final TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (threadPool == null) {
            throw new UnsupportedOperationException("TestClusterService wasn't initialized with a thread pool");
        }
        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
        notifyTimeout.future = threadPool.schedule(timeout, ThreadPool.Names.GENERIC, notifyTimeout);
        onGoingTimeouts.add(notifyTimeout);
        listeners.add(listener);
        listener.postAdded();
    }

    @Override
    public void submitStateUpdateTask(String source, Priority priority, ClusterStateUpdateTask updateTask) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void submitStateUpdateTask(String source, ClusterStateUpdateTask updateTask) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PendingClusterTask> pendingTasks() {
        throw new UnsupportedOperationException();

    }

    @Override
    public int numberOfPendingTasks() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lifecycle.State lifecycleState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterService start() throws ElasticsearchException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterService stop() throws ElasticsearchException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws ElasticsearchException {
        throw new UnsupportedOperationException();
    }

    class NotifyTimeout implements Runnable {
        final TimeoutClusterStateListener listener;
        final TimeValue timeout;
        volatile ScheduledFuture future;

        NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            FutureUtils.cancel(future);
        }

        @Override
        public void run() {
            if (future != null && future.isCancelled()) {
                return;
            }
            listener.onTimeout(this.timeout);
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }
}

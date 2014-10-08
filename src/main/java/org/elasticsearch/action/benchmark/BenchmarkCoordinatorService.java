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
package org.elasticsearch.action.benchmark;

import com.google.common.base.Joiner;
import com.google.common.collect.UnmodifiableIterator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.benchmark.abort.*;
import org.elasticsearch.action.benchmark.pause.*;
import org.elasticsearch.action.benchmark.resume.*;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.action.benchmark.exception.*;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates execution of benchmarks.
 *
 * This class is responsible for coordinating the cluster metadata associated with each benchmark. It
 * listens for cluster change events via
 * {@link org.elasticsearch.cluster.ClusterStateListener#clusterChanged(org.elasticsearch.cluster.ClusterChangedEvent)}
 * and responds by initiating the appropriate sequence of actions necessary to service each event. Since this class
 * is primarily focused on the management of cluster state, it checks to see if it is executing on the master and, if not,
 * simply does nothing.
 *
 * There is a related class, {@link org.elasticsearch.action.benchmark.BenchmarkExecutorService} which communicates
 * with, and is coordinated by, this class. It's role is to manage the actual execution of benchmarks on the assigned
 * nodes.
 *
 * The typical lifecycle of a benchmark is as follows:
 *
 * 1. Client submits a benchmark, thereby creating a metadata entry with state INITIALIZING.
 * 2. Executor nodes notice the metadata change via
 *    {@link org.elasticsearch.action.benchmark.BenchmarkExecutorService#clusterChanged(org.elasticsearch.cluster.ClusterChangedEvent)}
 *     and initialize themselves.
 * 3. Executor nodes call back to the coordinator (using the transport service) to get description of the benchmark. Once received,
 *    executors update their node state to READY.
 * 4. Once the coordinator receives READY from all executors, it updates the metadata state to RUNNING.
 * 5. Executor nodes notice the metadata change to state RUNNING and start actually executing the benchmark.
 * 6. As each executor finishes, it updates its state to COMPLETE.
 * 7. Once the coordinator receives COMPLETE from all executors, it requests the results (using the transport service) from
 *    each executor.
 * 8. After all results have been received, the coordinator updates the benchmark state to COMPLETE.
 * 9. On receipt of the COMPLETE state, the coordinator removes the benchmark from the cluster metadata and
 *    returns the results back to the client.
 */
public class BenchmarkCoordinatorService extends AbstractBenchmarkService {

    protected final BenchmarkStateManager manager;
    protected final Map<String, State> benchmarks = new ConcurrentHashMap<>();

    /**
     * Constructs a service component for running benchmarks
     */
    @Inject
    public BenchmarkCoordinatorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                       TransportService transportService, BenchmarkStateManager manager) {

        super(settings, clusterService, transportService, threadPool);
        this.manager = manager;
        transportService.registerHandler(NodeStateUpdateRequestHandler.ACTION, new NodeStateUpdateRequestHandler());
        transportService.registerHandler(BenchmarkDefinitionRequestHandler.ACTION, new BenchmarkDefinitionRequestHandler());
    }

    /**
     * Listens for and responds to state transitions.
     *
     * @param event Cluster change event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (isMasterNode() && event.nodesDelta().removed()) {
            updateNodeLiveness(event.nodesDelta());
            processOrphanedBenchmarks();
        }

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);
        final BenchmarkMetaData prev = event.previousState().metaData().custom(BenchmarkMetaData.TYPE);

        if (!isMasterNode() || !event.metaDataChanged() || meta == null || meta.entries().size() == 0) {
            return;
        }

        for (final BenchmarkMetaData.Entry entry : BenchmarkMetaData.addedOrChanged(prev, meta)) {

            logger.info(entry.toString());
            final State state = benchmarks.get(entry.benchmarkId());
            if (state == null) {
                // Remove any unknown benchmark state from the cluster metadata
                logger.warn("benchmark [{}]: unknown benchmark in cluster metadata", entry.benchmarkId());
                manager.clear(entry.benchmarkId(), new ActionListener() {
                    @Override
                    public void onResponse(Object o) { /* no-op */ }
                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("benchmark [{}]: failed to remove unknown benchmark from metadata", e, entry.benchmarkId());
                    }
                });
                continue;
            }

            if (allNodesFailed(entry)) {
                logger.error("benchmark [{}]: all nodes failed", entry.benchmarkId());
                state.failed(new ElasticsearchException("All nodes failed"));
                continue;
            }

            switch (entry.state()) {
                case INITIALIZING:
                    if (allNodesReady(entry) && state.canStartRunning()) {
                        // Once all executors have initialized and reported 'ready', we can update the benchmark's
                        // top-level state, thereby signalling to the executors that it's okay to begin execution.
                        state.ready();
                    }
                    break;
                case RUNNING:
                    if (allNodesFinished(entry) && state.canStopRunning()) {
                        // Once all executors have completed, successfully or otherwise, we can fetch the benchmark's
                        // results from each executor node, merge them into a single top-level result, and update
                        // the benchmark's top-level state.
                        state.finished(entry);
                    }
                    break;
                case RESUMING:
                    if (allNodesRunning(entry) && state.canResumeRunning()) {
                        assert state.batchedResponder != null;
                        state.batchedResponder.resumed(entry);
                    }
                    break;
                case PAUSED:
                    if (allNodesPaused(entry) && state.canPauseRunning()) {
                        assert state.batchedResponder != null;
                        state.batchedResponder.paused(entry);
                    }
                    break;
                case COMPLETED:
                    if (state.canComplete()) {
                        state.completed();
                    }
                    break;
                case FAILED:
                    state.failed(new ElasticsearchException("benchmark [" + entry.benchmarkId() + "]: failed"));
                    break;
                case ABORTED:
                    if (allNodesAborted(entry)) {
                        assert state.batchedResponder != null;
                        state.batchedResponder.aborted(entry);
                    }
                    break;
                default:
                    throw new ElasticsearchIllegalStateException("benchmark [" + entry.benchmarkId() + "]: illegal state [" + entry.state() + "]");
            }
        }
    }

    /* ** Public API Methods ** */

    /**
     * Starts a benchmark. Sets top-level and per-node state to INITIALIZING.
     * @param request   Benchmark request
     * @param listener  Response listener
     */
    public void startBenchmark(final BenchmarkStartRequest request,
                               final ActionListener<BenchmarkStartResponse> listener) {

        preconditions(request.numExecutorNodes());

        manager.start(request, new ActionListener<BenchmarkStateManager.BenchmarkCreationStatus>() {

            @Override
            public void onResponse(BenchmarkStateManager.BenchmarkCreationStatus creationStatus) {
                if (creationStatus.created()) {
                    assert null == benchmarks.get(request.benchmarkId());
                    benchmarks.put(request.benchmarkId(), new State(request, creationStatus.nodeIds(), listener));
                } else {
                    onFailure(new ElasticsearchIllegalStateException("benchmark [" + request.benchmarkId() + "]: aborted due to master failure"));
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Reports on the status of running benchmarks
     * @param request   Status request
     * @param listener  Response listener
     */
    public void listBenchmarks(final BenchmarkStatusRequest request, 
                               final ActionListener<BenchmarkStatusResponses> listener) {

        final BenchmarkMetaData meta = clusterService.state().metaData().custom(BenchmarkMetaData.TYPE);

        if (BenchmarkUtility.executors(clusterService.state().nodes(), 1).size() == 0 || meta == null || meta.entries().size() == 0) {
            listener.onResponse(new BenchmarkStatusResponses());
            return;
        }

        final BenchmarkStatusResponses responses = new BenchmarkStatusResponses();

        for (final BenchmarkMetaData.Entry entry : meta.entries()) {

            if (request.benchmarkIdPatterns() == null ||
                request.benchmarkIdPatterns().length == 0 ||
                Regex.simpleMatch(request.benchmarkIdPatterns(), entry.benchmarkId())) {

                try {
                    responses.add(manager.status(entry));
                } catch (Throwable t) {
                    logger.error("benchmark [{}]: failed to read status", t, entry.benchmarkId());
                    listener.onFailure(t);
                    return;
                }
            }
        }

        listener.onResponse(responses);
    }

    /**
     * Pauses a benchmark(s)
     * @param request   Pause request
     * @param listener  Response listener
     */
    public void pauseBenchmark(final BenchmarkPauseRequest request,
                               final ActionListener<BenchmarkPauseResponse> listener) {

        preconditions(1);

        manager.pause(request, new ActionListener<String[]>() {

            @Override
            public void onResponse(final String[] benchmarkIds) {
                if (benchmarkIds == null || benchmarkIds.length == 0) {
                    listener.onFailure(new BenchmarkMissingException(
                            "No benchmarks found matching: [" + Joiner.on(",").join(request.benchmarkIdPatterns()) + "]"));
                } else {
                    final BatchedResponder<BenchmarkPauseResponse> responder =
                            new BatchedResponder<>(new BenchmarkPauseResponse(), listener, new CountDown(benchmarkIds.length));

                    for (final String benchmarkId : benchmarkIds) {
                        final State state = benchmarks.get(benchmarkId);
                        if (state == null) {
                            throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        state.batchedResponder = responder;
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    /**
     * Resumes a previously paused benchmark(s)
     * @param request   Resume request
     * @param listener  Response listener
     */
    public void resumeBenchmark(final BenchmarkResumeRequest request,
                                final ActionListener<BenchmarkResumeResponse> listener) {

        preconditions(1);

        manager.resume(request, new ActionListener<String[]>() {

            @Override
            public void onResponse(final String[] benchmarkIds) {
                if (benchmarkIds == null || benchmarkIds.length == 0) {
                    listener.onFailure(new BenchmarkMissingException(
                            "No benchmarks found matching: [" + Joiner.on(",").join(request.benchmarkIdPatterns()) + "]"));
                } else {
                    final BatchedResponder<BenchmarkResumeResponse> responder =
                            new BatchedResponder<>(new BenchmarkResumeResponse(), listener, new CountDown(benchmarkIds.length));

                    for (final String benchmarkId : benchmarkIds) {
                        final State state = benchmarks.get(benchmarkId);
                        if (state == null) {
                            throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        state.batchedResponder = responder;
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    /**
     * Aborts a running benchmark(s)
     * @param request   Abort request
     * @param listener  Response listener
     */
    public void abortBenchmark(final BenchmarkAbortRequest request,
                               final ActionListener<BenchmarkAbortResponse> listener) {

        preconditions(1);

        manager.abort(request, new ActionListener<String[]>() {

            @Override
            public void onResponse(final String[] benchmarkIds) {
                if (benchmarkIds == null || benchmarkIds.length == 0) {
                    listener.onFailure(new BenchmarkMissingException(
                            "No benchmarks found matching: [" + Joiner.on(",").join(request.benchmarkIdPatterns()) + "]"));
                } else {
                    final BatchedResponder<BenchmarkAbortResponse> responder =
                            new BatchedResponder<>(new BenchmarkAbortResponse(), listener, new CountDown(benchmarkIds.length));

                    for (final String benchmarkId : benchmarkIds) {
                        final State state = benchmarks.get(benchmarkId);
                        if (state == null) {
                            throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        state.batchedResponder = responder;
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    /* ** State Change Listeners ** */

    /**
     * Certain client requests may operate on multiple benchmarks in a single request by
     * passing wildcard patterns. In such cases this class is used to wait on events from
     * all matching benchmarks and respond to the caller only after all have completed the
     * requested action.
     *
     * @param <T>   The type of the response payload
     */
    private final class BatchedResponder<T extends BatchedResponse> {

        final T response;
        final CountDown countDown;
        final ActionListener<T> listener;

        BatchedResponder(final T response, final ActionListener<T> listener, final CountDown countDown) {
            this.response = response;
            this.listener = listener;
            this.countDown = countDown;
        }

        /**
         * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#ABORTED}
         * and all executor nodes have reported {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#ABORTED}.
         */
        void aborted(final BenchmarkMetaData.Entry entry) {
            try {
                addResponse(entry);
                // Initiate completion sequence; send partial results back to original caller
                final State state = benchmarks.get(entry.benchmarkId());
                state.finished(entry);
            } finally {
                if (countDown.countDown()) {
                    listener.onResponse(response);
                }
            }
        }

        /**
         * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#RESUMING}.
         */
        void resumed(final BenchmarkMetaData.Entry entry) {
            try {
                addResponse(entry);
                final State state = benchmarks.get(entry.benchmarkId());
                manager.update(entry.benchmarkId(), BenchmarkMetaData.State.RUNNING, BenchmarkMetaData.Entry.NodeState.RUNNING,
                        state.liveness,
                        new ActionListener() {
                            @Override
                            public void onResponse(Object o) { /* no-op */ }
                            @Override
                            public void onFailure(Throwable e) {
                                state.failed(e);
                            }
                        });
            } finally {
                if (countDown.countDown()) {
                    listener.onResponse(response);
                }
            }
        }

        /**
         * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#PAUSED}
         * and all executor nodes have reported {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#PAUSED}.
         */
        void paused(final BenchmarkMetaData.Entry entry) {
            try {
                addResponse(entry);
            } finally {
                if (countDown.countDown()) {
                    listener.onResponse(response);
                }
            }
        }

        void addResponse(final BenchmarkMetaData.Entry entry) {
            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                response.addNodeResponse(entry.benchmarkId(), e.getKey(), e.getValue());
            }
        }
    }

    /* ** Utilities ** */

    protected static final class Liveness {

        private final AtomicBoolean liveness;

        Liveness() {
            liveness = new AtomicBoolean(true);
        }

        public boolean alive() {
            return liveness.get();
        }

        public boolean set(boolean expected, boolean updated) {
            return liveness.compareAndSet(expected, updated);
        }
    }

    private final class State {

        final String benchmarkId;
        final BenchmarkStartRequest request;
        final ImmutableOpenMap<String, Liveness> liveness;
        final List<String> errorMessages = new ArrayList<>();
        BenchmarkStartResponse response;

        AtomicBoolean running  = new AtomicBoolean(false);
        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean paused   = new AtomicBoolean(false);

        ActionListener<BenchmarkStartResponse> listener;
        BatchedResponder batchedResponder;

        State(final BenchmarkStartRequest request, final List<String> nodeIds, final ActionListener<BenchmarkStartResponse> listener) {

            this.benchmarkId = request.benchmarkId();
            this.request = request;
            this.listener = listener;

            ImmutableOpenMap.Builder<String, Liveness> builder = ImmutableOpenMap.builder();
            for (final String nodeId : nodeIds) {
                builder.put(nodeId, new Liveness());
            }
            liveness = builder.build();
        }

        /**
         * Called when all executor nodes have reported {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#READY}.
         * Sets top-level state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#RUNNING} and
         * per-node state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#RUNNING}.
         */
        void ready() {

            manager.update(benchmarkId, BenchmarkMetaData.State.RUNNING, BenchmarkMetaData.Entry.NodeState.RUNNING,
                    liveness,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            failed(e);
                        }
                    });
        }

        /**
         * Called when all executor nodes have completed, successfully or otherwise.
         * Fetches benchmark response and sets state.
         * Sets top-level state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#COMPLETED} and
         * per-node state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#COMPLETED}.
         */
        void finished(final BenchmarkMetaData.Entry entry) {

            try {
                response = manager.status(entry);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("benchmark [{}]: failed to read status", e, entry.benchmarkId());
                failed(e);
                return;
            }

            manager.update(entry.benchmarkId(), BenchmarkMetaData.State.COMPLETED, BenchmarkMetaData.Entry.NodeState.COMPLETED,
                    liveness,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            failed(e);
                        }
                    });
        }

        /**
         * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#COMPLETED}.
         */
        void completed() {

            manager.clear(benchmarkId, new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    benchmarks.remove(benchmarkId);
                    sendResponse();
                }

                @Override
                public void onFailure(Throwable e) {
                    benchmarks.remove(benchmarkId);
                    listener.onFailure(e);
                }
            });
        }

        void failed(final Throwable cause) {

            manager.clear(benchmarkId, new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    internalFailed(cause);
                }

                @Override
                public void onFailure(Throwable e) {
                    internalFailed(cause);
                }
            });
        }

        private void internalFailed(final Throwable cause) {

            benchmarks.remove(benchmarkId);
            if (response == null) {
                response = new BenchmarkStartResponse(benchmarkId);
            }

            response.state(BenchmarkStartResponse.State.FAILED);
            if (cause != null) {
                response.errors(cause.getMessage());
            }

            sendResponse();
        }

        private void sendResponse() {
            if (response == null) {
                listener.onFailure(new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing response"));
            } else {
                response.errors(errorMessages);
                listener.onResponse(response);
            }
        }

        boolean isNodeAlive(final String nodeId) {
            return liveness.containsKey(nodeId) && liveness.get(nodeId).alive();
        }

        boolean canStartRunning() {
            return running.compareAndSet(false, true);
        }

        boolean canStopRunning() {
            return running.compareAndSet(true, false);
        }

        boolean canPauseRunning() {
            return paused.compareAndSet(false, true);
        }

        boolean canResumeRunning() {
            return paused.compareAndSet(true, false);
        }

        boolean canComplete() {
            return complete.compareAndSet(false, true);
        }
    }

    /**
     * Record nodes that dropped out of the cluster
     * @param delta     Delta of discovery nodes from last cluster state update
     */
    private void updateNodeLiveness(final DiscoveryNodes.Delta delta) {

        for (final DiscoveryNode node : delta.removedNodes()) {
            for (Map.Entry<String, State> entry : benchmarks.entrySet()) {
                if (entry.getValue().isNodeAlive(node.id())) {
                    final Liveness liveness = entry.getValue().liveness.get(node.id());
                    if (liveness != null) {
                        if (liveness.set(true, false)) {
                            logger.warn("benchmark [{}]: marked node [{}] as not live", entry.getKey(), node.id());
                            entry.getValue().errorMessages.add("node: [" + node.id() + "] dropped out of cluster");
                        }
                    }
                }
            }
        }
    }

    /**
     * Find benchmarks for which all executor nodes have dropped from cluster state
     * and forcefully fail them.
     */
    private void processOrphanedBenchmarks() {

        for (Map.Entry<String, State> entry : benchmarks.entrySet()) {
            boolean hasLiveNodes = false;
            UnmodifiableIterator<String> iter = entry.getValue().liveness.keysIt();
            while (iter.hasNext()) {
                if (entry.getValue().isNodeAlive(iter.next())) {
                    hasLiveNodes = true;
                }
            }

            if (!hasLiveNodes) {
                logger.warn("benchmark [{}]: has no live nodes; manually killing it", entry.getKey());
                entry.getValue().failed(new ElasticsearchException(
                        "benchmark [" + entry.getKey() + "]: all executor nodes dropped out of cluster"));
            }
        }
    }

    protected void preconditions(int num) {
        final int n = BenchmarkUtility.executors(clusterService.state().nodes(), num).size();
        if (n < num) {
            throw new BenchmarkNodeMissingException(
                    "Insufficient executor nodes in cluster: require at least [" + num + "] found [" + n + "]");
        }
    }

    private boolean allNodesFailed(final BenchmarkMetaData.Entry entry) {
        return checkAllNodeStates(entry, BenchmarkMetaData.Entry.NodeState.FAILED);
    }

    private boolean allNodesReady(final BenchmarkMetaData.Entry entry) {
        return checkAllNodeStates(entry, BenchmarkMetaData.Entry.NodeState.READY);
    }

    private boolean allNodesRunning(final BenchmarkMetaData.Entry entry) {
        return checkAllNodeStates(entry, BenchmarkMetaData.Entry.NodeState.RUNNING);
    }

    private boolean allNodesAborted(final BenchmarkMetaData.Entry entry) {
        return checkAllNodeStates(entry, BenchmarkMetaData.Entry.NodeState.ABORTED);
    }

    private boolean allNodesPaused(final BenchmarkMetaData.Entry entry) {
        return checkAllNodeStates(entry, BenchmarkMetaData.Entry.NodeState.PAUSED);
    }

    private boolean checkAllNodeStates(final BenchmarkMetaData.Entry entry, final BenchmarkMetaData.Entry.NodeState nodeState) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() == BenchmarkMetaData.Entry.NodeState.FAILED) {
                continue;   // Failed nodes don't factor in
            }
            final State state = benchmarks.get(entry.benchmarkId());
            if (state != null && !state.isNodeAlive(e.getKey())) {
                continue;   // Dead nodes don't factor in
            }
            if (e.getValue() != nodeState) {
                return false;
            }
        }
        return true;
    }

    private static final EnumSet<BenchmarkMetaData.Entry.NodeState> NOT_FINISHED = EnumSet.of(
            BenchmarkMetaData.Entry.NodeState.INITIALIZING, BenchmarkMetaData.Entry.NodeState.READY,
            BenchmarkMetaData.Entry.NodeState.RUNNING, BenchmarkMetaData.Entry.NodeState.PAUSED);

    private boolean allNodesFinished(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {

            final State state = benchmarks.get(entry.benchmarkId());
            if (state != null && !state.isNodeAlive(e.getKey())) {
                continue;   // Dead nodes don't factor in
            }

            if (NOT_FINISHED.contains(e.getValue())) {
                return false;
            }
        }
        return true;
    }

    /* ** Request Handlers ** */

    /**
     * Responds to requests from the executor nodes to transmit the definition for the given benchmark.
     */
    public class BenchmarkDefinitionRequestHandler extends BaseTransportRequestHandler<BenchmarkDefinitionTransportRequest> {

        public static final String ACTION = "indices:data/benchmark/node/definition";

        @Override
        public BenchmarkDefinitionTransportRequest newInstance() {
            return new BenchmarkDefinitionTransportRequest();
        }

        @Override
        public void messageReceived(BenchmarkDefinitionTransportRequest request, TransportChannel channel) throws Exception {
            if (benchmarks.get(request.benchmarkId) != null) {
                final BenchmarkDefinitionActionResponse response =
                        new BenchmarkDefinitionActionResponse(benchmarks.get(request.benchmarkId).request, request.nodeId);
                channel.sendResponse(response);
            } else {
                channel.sendResponse(new ElasticsearchIllegalStateException("benchmark [" + request.benchmarkId + "]: missing internal state"));
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

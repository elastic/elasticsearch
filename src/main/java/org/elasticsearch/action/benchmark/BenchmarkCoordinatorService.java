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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
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
public class BenchmarkCoordinatorService extends AbstractBenchmarkService<BenchmarkCoordinatorService> {

    protected final BenchmarkStateManager manager;
    protected final Map<String, InternalCoordinatorState> benchmarks = new ConcurrentHashMap<>();



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

    @Override
    protected void doStart() throws ElasticsearchException { }
    @Override
    protected void doStop() throws ElasticsearchException { }
    @Override
    protected void doClose() throws ElasticsearchException { }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        // NOCOMMIT - Need to detect dropped node here and remove from internal state where appropriate

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);
        final BenchmarkMetaData prev = event.previousState().metaData().custom(BenchmarkMetaData.TYPE);

        if (!isMasterNode() || !event.metaDataChanged() || meta == null || meta.entries().size() == 0) {
            return;
        }

        for (final BenchmarkMetaData.Entry entry : BenchmarkMetaData.delta(prev, meta)) {

            log(entry);
            final InternalCoordinatorState ics = benchmarks.get(entry.benchmarkId());
            if (ics == null) {
                logger.warn("unknown benchmark in cluster metadata [{}]", entry.benchmarkId());
                continue;
            }

            switch (entry.state()) {
                case INITIALIZING:
                    if (allNodesReady(entry)) {
                        // Once all executors have initialized and reported 'ready', we can update the benchmark's
                        // top-level state, thereby signalling to the executors that it's okay to begin execution.
                        if (ics.canStartRunning()) {
                            ics.onReady.onStateChange(entry);
                        }
                    }
                    break;
                case RUNNING:
                    if (allNodesFinished(entry)) {
                        // Once all executors have completed, successfully or otherwise, we can fetch the benchmark's
                        // results from each executor node, merge them into a single top-level result, and update
                        // the benchmark's top-level state.
                        if (ics.canStopRunning()) {
                            ics.onFinished.onStateChange(entry);
                        }
                    }
                    break;
                case RESUMING:
                    if (allNodesRunning(entry)) {
                        if (ics.canResumeRunning()) {
                            assert ics.onResumed != null;
                            ics.onResumed.onStateChange(entry);
                        }
                    }
                    break;
                case PAUSED:
                    if (allNodesPaused(entry)) {
                        if (ics.canPauseRunning()) {
                            assert ics.onPaused != null;
                            ics.onPaused.onStateChange(entry);
                        }
                    }
                    break;
                case COMPLETED:
                    if (ics.canComplete()) {
                        ics.onComplete.onStateChange(entry);
                    }
                    break;
                case FAILED:

                    // NOCOMMIT - Finish
                    throw new UnsupportedOperationException("Unimplemented");

                case ABORTED:
                    if (allNodesAborted(entry)) {
                        assert ics.onAbort != null;
                        ics.onAbort.onStateChange(entry);
                    }
                    break;
                default:
                    throw new ElasticsearchIllegalStateException("benchmark [" + entry.benchmarkId() + "]: illegal state [" + entry.state() + "]");
            }
        }
    }

    /* ** Public API Methods ** */

    /**
     * Starts a benchmark
     * @param request   Benchmark request
     * @param listener  Response listener
     */
    public void startBenchmark(final BenchmarkStartRequest request,
                               final ActionListener<BenchmarkStartResponse> listener) {

        preconditions(request.numExecutorNodes());

        manager.start(request, new ActionListener() {

            @Override
            public void onResponse(Object o) {

                assert null == benchmarks.get(request.benchmarkId());

                final InternalCoordinatorState ics = new InternalCoordinatorState(request, listener);

                ics.onReady = new OnReadyStateChangeListener(ics);
                ics.onFinished = new OnFinishedStateChangeListener(ics);
                ics.onComplete = new OnCompleteStateChangeListener(ics);

                benchmarks.put(request.benchmarkId(), ics);
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
                    final OnPausedStateChangeListener on = new OnPausedStateChangeListener(listener,
                                                                                           new CountDown(benchmarkIds.length));

                    for (final String benchmarkId : benchmarkIds) {
                        final InternalCoordinatorState ics = benchmarks.get(benchmarkId);
                        if (ics == null) {
                            throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        ics.onPaused = on;
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
                    for (final String benchmarkId : benchmarkIds) {
                        final InternalCoordinatorState ics = benchmarks.get(benchmarkId);
                        if (ics == null) {
                            throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        ics.onResumed = new OnResumedStateChangeListener(ics, listener);
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
                    for (final String benchmarkId : benchmarkIds) {
                        final InternalCoordinatorState ics = benchmarks.get(benchmarkId);
                        if (ics == null) {
                            throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        ics.onAbort = new OnAbortStateChangeListener(ics, listener);
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

    private interface  StateChangeListener {
        void onStateChange(BenchmarkMetaData.Entry entry);
    }

    /**
     * Called when all executor nodes have reported {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#READY}.
     * Sets top-level state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#RUNNING} and
     * per-node state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#RUNNING}.
     */
    private final class OnReadyStateChangeListener implements StateChangeListener {

        final InternalCoordinatorState ics;

        OnReadyStateChangeListener(InternalCoordinatorState ics) {
            this.ics = ics;
        }

        @Override
        public synchronized void onStateChange(final BenchmarkMetaData.Entry entry) {

            manager.update(ics.benchmarkId, BenchmarkMetaData.State.RUNNING, BenchmarkMetaData.Entry.NodeState.RUNNING,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            ics.onFailure(e);
                        }
                    });
        }
    }

    /**
     * Called when all executor nodes have completed, successfully or otherwise.
     * Fetches benchmark response and sets state.
     * Sets top-level state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#COMPLETED} and
     * per-node state to {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#COMPLETED}.
     */
    private final class OnFinishedStateChangeListener implements StateChangeListener {

        final InternalCoordinatorState ics;

        OnFinishedStateChangeListener(InternalCoordinatorState ics) {
            this.ics = ics;
        }

        @Override
        public synchronized void onStateChange(final BenchmarkMetaData.Entry entry) {

            try {
                ics.response = manager.status(entry);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("benchmark [{}]: failed to read status", e, entry.benchmarkId());

                // NOCOMMIT - Should we respond to the listener here or set the state to FAILED and
                //            initiate failure procedure?

                ics.onFailure(e);
            }

            manager.update(ics.benchmarkId, BenchmarkMetaData.State.COMPLETED, BenchmarkMetaData.Entry.NodeState.COMPLETED,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            ics.onFailure(e);
                        }
                    });
        }
    }

    /**
     * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#COMPLETED}.
     */
    private final class OnCompleteStateChangeListener implements StateChangeListener {

        final InternalCoordinatorState ics;

        OnCompleteStateChangeListener(final InternalCoordinatorState ics) {
            this.ics = ics;
        }

        @Override
        public synchronized void onStateChange(final BenchmarkMetaData.Entry entry) {

             manager.clear(ics.benchmarkId, new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    benchmarks.remove(ics.benchmarkId);
                    ics.onResponse();
                }

                @Override
                public void onFailure(Throwable e) {
                    benchmarks.remove(ics.benchmarkId);
                    ics.onFailure(e);
                }
            });
        }
    }

    /**
     * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#ABORTED}
     * and all executor nodes have reported {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#ABORTED}.
     */
    private final class OnAbortStateChangeListener implements StateChangeListener {

        final InternalCoordinatorState               ics;
        final ActionListener<BenchmarkAbortResponse> listener;

        OnAbortStateChangeListener(final InternalCoordinatorState ics, final ActionListener<BenchmarkAbortResponse> listener) {
            this.ics      = ics;
            this.listener = listener;
        }

        @Override
        public synchronized void onStateChange(BenchmarkMetaData.Entry entry) {

            final BenchmarkAbortResponse response = new BenchmarkAbortResponse();

            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                assert e.getValue() == BenchmarkMetaData.Entry.NodeState.ABORTED;
                response.addNodeResponse(entry.benchmarkId(), e.getKey(), e.getValue());
            }

            listener.onResponse(response);

            // Initiate completion sequence; send partial results back to original caller
            ics.onFinished.onStateChange(entry);
        }
    }

    /**
     * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#PAUSED}
     * and all executor nodes have reported {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.Entry.NodeState#PAUSED}.
     */
    private final class OnPausedStateChangeListener implements StateChangeListener {

        final CountDown                              countdown;
        final ActionListener<BenchmarkPauseResponse> listener;
        final BenchmarkPauseResponse                 response;

        OnPausedStateChangeListener(final ActionListener<BenchmarkPauseResponse> listener,
                                    final CountDown countdown) {
            this.countdown = countdown;
            this.listener  = listener;
            this.response  = new BenchmarkPauseResponse();
        }

        @Override
        public synchronized void onStateChange(BenchmarkMetaData.Entry entry) {

            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                assert e.getValue() == BenchmarkMetaData.Entry.NodeState.PAUSED;
                response.addNodeResponse(entry.benchmarkId(), e.getKey(), e.getValue());
            }

            if (countdown.countDown()) {
                listener.onResponse(response);
            }
        }
    }

    /**
     * Called when top-level state has been reported as {@link org.elasticsearch.cluster.metadata.BenchmarkMetaData.State#RESUMING}.
     */
    private final class OnResumedStateChangeListener implements StateChangeListener {

        final InternalCoordinatorState                ics;
        final ActionListener<BenchmarkResumeResponse> listener;

        OnResumedStateChangeListener(final InternalCoordinatorState ics, final ActionListener<BenchmarkResumeResponse> listener) {
            this.ics      = ics;
            this.listener = listener;
        }

        @Override
        public synchronized void onStateChange(BenchmarkMetaData.Entry entry) {

            final BenchmarkResumeResponse response = new BenchmarkResumeResponse();

            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                response.addNodeResponse(entry.benchmarkId(), e.getKey(), e.getValue());
            }

            listener.onResponse(response);

            manager.update(ics.benchmarkId, BenchmarkMetaData.State.RUNNING, BenchmarkMetaData.Entry.NodeState.RUNNING,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            ics.onFailure(e);
                        }
                    });
        }
    }

    /* ** Utilities ** */

    protected static final class InternalCoordinatorState {

        private static final ESLogger logger = ESLoggerFactory.getLogger(InternalCoordinatorState.class.getName());

        final String                benchmarkId;
        final BenchmarkStartRequest request;
        BenchmarkStartResponse      response;

        AtomicBoolean running  = new AtomicBoolean(false);
        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean paused   = new AtomicBoolean(false);

        ActionListener<BenchmarkStartResponse> listener;
        final Object listenerLock = new Object();

        OnReadyStateChangeListener    onReady;
        OnFinishedStateChangeListener onFinished;
        OnCompleteStateChangeListener onComplete;
        OnPausedStateChangeListener   onPaused;
        OnResumedStateChangeListener  onResumed;
        OnAbortStateChangeListener    onAbort;

        InternalCoordinatorState(BenchmarkStartRequest request, final ActionListener<BenchmarkStartResponse> listener) {
            this.benchmarkId = request.benchmarkId();
            this.request     = request;
            this.listener    = listener;
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

        void onFailure(Throwable t) {
            synchronized (listenerLock) {
                if (listener == null) {
                    logger.warn("benchmark [{}]: attempted redundant response [{}]", benchmarkId, t.getMessage());
                } else {
                    try {
                        listener.onFailure(t);
                    } finally {
                        listener = null;
                    }
                }
            }
        }

        void onResponse() {
            synchronized (listenerLock) {
                if (listener == null) {
                    logger.warn("benchmark [{}]: attempted redundant response [{}]", benchmarkId);
                } else {
                    try {
                        if (response == null) {
                            listener.onFailure(new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: missing response"));
                        } else {
                            listener.onResponse(response);
                        }
                    } finally {
                        listener = null;
                    }
                }
            }
        }
    }

    private void preconditions(int num) {
        final int n = BenchmarkUtility.executors(clusterService.state().nodes(), num).size();
        if (n < num) {
            throw new BenchmarkNodeMissingException(
                    "Insufficient executor nodes in cluster: require at least [" + num + "] found [" + n + "]");
        }
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

    private boolean checkAllNodeStates(final BenchmarkMetaData.Entry entry, final BenchmarkMetaData.Entry.NodeState state) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() != state) {
                return false;
            }
        }
        return true;
    }

    private boolean allNodesFinished(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() == BenchmarkMetaData.Entry.NodeState.INITIALIZING ||
                e.getValue() == BenchmarkMetaData.Entry.NodeState.READY ||
                e.getValue() == BenchmarkMetaData.Entry.NodeState.RUNNING) {

                // XXX - does this cover all appropriate states? what about PAUSED?

                return false;
            }
        }
        return true;
    }

    private void log(BenchmarkMetaData.Entry entry) {
        StringBuilder sb = new StringBuilder();
        sb.append("benchmark state change: [").append(entry.benchmarkId()).append("] (").append(entry.state()).append(") [");
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            sb.append(" ").append(e.getKey()).append(":").append(e.getValue());
        }
        sb.append(" ]");
        logger.debug(sb.toString());
    }

    /* ** Request Handlers ** */

    /**
     * Responds to requests from the executor nodes to transmit the definition for the given benchmark.
     */
    public class BenchmarkDefinitionRequestHandler extends BaseTransportRequestHandler<BenchmarkDefinitionTransportRequest> {

        static final String ACTION = "indices:data/benchmark/node/definition";

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

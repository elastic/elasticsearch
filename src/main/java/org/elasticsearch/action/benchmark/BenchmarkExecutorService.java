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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.benchmark.competition.CompetitionResult;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.*;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * Manages the execution of benchmarks.
 *
 * See {@link org.elasticsearch.action.benchmark.BenchmarkCoordinatorService} for a description of how
 * benchmark communication works.
 */
public class BenchmarkExecutorService extends AbstractBenchmarkService {

    protected final BenchmarkExecutor executor;
    protected final Map<String, State> states = new ConcurrentHashMap<>();

    private static final long TIMEOUT = 60;
    private static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;

    @Inject
    public BenchmarkExecutorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                    Client client, TransportService transportService) {

        this(settings, clusterService, threadPool, transportService, new BenchmarkExecutor(client, clusterService));
    }

    protected BenchmarkExecutorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                       TransportService transportService, BenchmarkExecutor executor) {

        super(settings, clusterService, transportService, threadPool);
        this.executor = executor;
        transportService.registerHandler(BenchmarkStatusRequestHandler.ACTION, new BenchmarkStatusRequestHandler());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);
        final BenchmarkMetaData prev = event.previousState().metaData().custom(BenchmarkMetaData.TYPE);

        if (!isBenchmarkNode() || !event.metaDataChanged() || meta == null || meta.entries().size() == 0) {
            return;
        }

        for (final BenchmarkMetaData.Entry entry : BenchmarkMetaData.addedOrChanged(prev, meta)) {

            if (entry.nodeStateMap().get(nodeId()) == null) {   // Benchmark not assigned to this node. Skip it.
                continue;
            }

            if (states.get(entry.benchmarkId()) == null) {
                if (entry.state() == BenchmarkMetaData.State.INITIALIZING &&
                    entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.INITIALIZING) {
                    states.put(entry.benchmarkId(), new State(entry.benchmarkId()));
                } else {
                    logger.error("benchmark [{}]: missing internal state", entry.benchmarkId());
                    continue;
                }
            }

            final State state = states.get(entry.benchmarkId());

            switch (entry.state()) {
                case INITIALIZING:
                    if (entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.INITIALIZING) {
                        break;  // Benchmark has already been initialized on this node
                    }

                    if (state.initialize()) {
                        // Fetch benchmark definition from master
                        logger.debug("benchmark [{}]: fetching definition", entry.benchmarkId());
                        final BenchmarkDefinitionResponseHandler handler = new BenchmarkDefinitionResponseHandler(entry.benchmarkId());

                        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                            @Override
                            public void run() {
                                transportService.sendRequest(
                                        master(),
                                        BenchmarkCoordinatorService.BenchmarkDefinitionRequestHandler.ACTION,
                                        new BenchmarkDefinitionTransportRequest(entry.benchmarkId(), nodeId()),
                                        handler);
                            }
                        });
                    }
                    break;
                case RUNNING:
                    if (entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.RUNNING) {
                        break;
                    }

                    if (state.start()) {
                        final ActionListener<BenchmarkStartResponse> listener = new ActionListener<BenchmarkStartResponse>() {
                            @Override
                            public void onResponse(BenchmarkStartResponse response) {
                                logger.debug("benchmark [{}]: completed [{}]", response.benchmarkId(), response.state());
                                updateNodeState(response.benchmarkId(), nodeId(), convertToNodeState(response.state()));
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                logger.error(t.getMessage(), t);
                                updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                            }
                        };

                        logger.debug("benchmark [{}]: starting execution", entry.benchmarkId());

                        threadPool.executor(ThreadPool.Names.BENCH).execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    final BenchmarkStatusNodeActionResponse response =
                                            executor.start(state.request, state.response, state.benchmarkSemaphores);
                                    state.response = response.response();
                                    state.complete = true;
                                    listener.onResponse(state.response);
                                } catch (Throwable t) {
                                    listener.onFailure(t);
                                }
                            }
                        });
                    }
                    break;
                case RESUMING:
                    if (entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.PAUSED) {
                        break;
                    }

                    try {
                        if (state.resume()) {
                            logger.debug("benchmark [{}]: resuming execution", entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.RUNNING);
                        }
                    } catch (Exception e) {
                        logger.error("benchmark [{}]: failed to resume", e, entry.benchmarkId());
                        updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                    }
                    break;
                case PAUSED:
                    if (entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.RUNNING &&
                        entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.READY) {
                        break;
                    }

                    try {
                        if (state.pause()) {
                            logger.debug("benchmark [{}]: pausing execution", entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.PAUSED);
                        }
                    } catch (Exception e) {
                        logger.error("benchmark [{}]: failed to pause", e, entry.benchmarkId());
                        updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                    }
                    break;
                case ABORTED:
                    if (entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.COMPLETED &&
                        entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.ABORTED &&
                        entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.FAILED) {
                        break;
                    }

                    try {
                        if (state.abort()) {
                            logger.debug("benchmark [{}]: aborting execution", entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.ABORTED);
                        }
                    } catch (Exception e) {
                        logger.error("benchmark [{}]: failed to abort", e, entry.benchmarkId());
                        updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                    }
                    break;
                case COMPLETED:
                    states.remove(entry.benchmarkId());
                    logger.debug("benchmark [{}]: completed", entry.benchmarkId());
                    break;
                default:
                    throw new ElasticsearchIllegalStateException("benchmark [" + entry.benchmarkId() + "]: illegal state [" + entry.state() + "]");
            }
        }
    }

    /**
     * Response handler for benchmark definition requests. The payload is the benchmark definition which
     * is transmitted to us from the master. We use this to know how to execute the benchmark.
     */
    public class BenchmarkDefinitionResponseHandler implements TransportResponseHandler<BenchmarkDefinitionActionResponse> {

        final String benchmarkId;

        public BenchmarkDefinitionResponseHandler(String benchmarkId) {
            this.benchmarkId = benchmarkId;
        }

        @Override
        public BenchmarkDefinitionActionResponse newInstance() {
            return new BenchmarkDefinitionActionResponse();
        }

        @Override
        public void handleResponse(BenchmarkDefinitionActionResponse response) {

            // We have received the benchmark definition from the master.
            logger.debug("benchmark [{}]: received definition", response.benchmarkId);

            // Update our internal bookkeeping
            final State state = states.get(response.benchmarkId);
            if (state == null) {
                throw new ElasticsearchIllegalStateException("benchmark [" + response.benchmarkId + "]: missing internal state");
            }

            state.request = response.benchmarkStartRequest;
            BenchmarkMetaData.Entry.NodeState newNodeState = BenchmarkMetaData.Entry.NodeState.READY;
            state.benchmarkSemaphores = new BenchmarkSemaphores(state.request.competitors());

            try {
                // Initialize the benchmark response payload
                final BenchmarkStartResponse bsr = new BenchmarkStartResponse(state.benchmarkId, new HashMap<String, CompetitionResult>());
                bsr.state(BenchmarkStartResponse.State.RUNNING);
                bsr.verbose(state.request.verbose());
                state.response = bsr;
            } catch (Throwable t) {
                logger.error("benchmark [{}]: failed to create", t, response.benchmarkId);
                states.remove(benchmarkId);
                newNodeState = BenchmarkMetaData.Entry.NodeState.FAILED;
            }

            // Notify the master that either we are ready to start executing or that we have failed to initialize
            final NodeStateUpdateResponseHandler handler = new NodeStateUpdateResponseHandler(benchmarkId);
            final NodeStateUpdateTransportRequest update = new NodeStateUpdateTransportRequest(response.benchmarkId, response.nodeId, newNodeState);

            transportService.sendRequest(master(), BenchmarkCoordinatorService.NodeStateUpdateRequestHandler.ACTION, update, handler);
        }

        @Override
        public void handleException(TransportException e) {
            logger.error("benchmark [{}]: failed to receive definition - cannot execute", e, benchmarkId);
            states.remove(benchmarkId);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    /**
     * Response handler for node state update requests.
     */
    public class NodeStateUpdateResponseHandler implements TransportResponseHandler<NodeStateUpdateActionResponse> {

        final String benchmarkId;

        public NodeStateUpdateResponseHandler(String benchmarkId) {
            this.benchmarkId = benchmarkId;
        }

        @Override
        public NodeStateUpdateActionResponse newInstance() {
            return new NodeStateUpdateActionResponse();
        }

        @Override
        public void handleResponse(NodeStateUpdateActionResponse response) { /* no-op */ }

        @Override
        public void handleException(TransportException e) {
            logger.error("benchmark [{}]: failed to receive state change acknowledgement - cannot execute", e, benchmarkId);
            states.remove(benchmarkId);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    /**
     * Responds to requests from the master to transmit the results of the given benchmark from executors back to master.
     */
    public class BenchmarkStatusRequestHandler extends BaseTransportRequestHandler<BenchmarkStatusTransportRequest> {

        public static final String ACTION = "indices:data/benchmark/node/status";

        @Override
        public BenchmarkStatusTransportRequest newInstance() {
            return new BenchmarkStatusTransportRequest();
        }

        @Override
        public void messageReceived(final BenchmarkStatusTransportRequest request, final TransportChannel channel) throws Exception {

            final State state = states.get(request.benchmarkId);
            if (state == null) {
                channel.sendResponse(new ElasticsearchIllegalStateException("benchmark [" + request.benchmarkId + "]: missing internal state"));
                return;
            }

            try {
                if (state.complete()) {
                    channel.sendResponse(new BenchmarkStatusNodeActionResponse(state.benchmarkId, nodeId(), state.response));
                } else {
                    final BenchmarkStatusNodeActionResponse status = new BenchmarkStatusNodeActionResponse(state.benchmarkId, nodeId());
                    status.response(state.response);
                    if (!status.hasErrors()) {
                        channel.sendResponse(status);
                    } else {
                        channel.sendResponse(new ElasticsearchIllegalStateException("benchmark [" + request.benchmarkId + "]: " + status.error()));
                    }
                }
            } catch (Throwable t) {
                try {
                    logger.error("benchmark [{}]: failed to send results", t, request.benchmarkId);
                    channel.sendResponse(t);
                } catch (Throwable e) {
                    logger.error("unable to send failure response", e);
                }
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    /**
     * Update node state in cluster meta data by sending a request to the master.
     */
    protected void updateNodeState(String benchmarkId, String nodeId, BenchmarkMetaData.Entry.NodeState nodeState) {

        final NodeStateUpdateResponseHandler  handler = new NodeStateUpdateResponseHandler(benchmarkId);
        final NodeStateUpdateTransportRequest request = new NodeStateUpdateTransportRequest(benchmarkId, nodeId, nodeState);

        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
            @Override
            public void run() {
                transportService.sendRequest(
                        master(),
                        BenchmarkCoordinatorService.NodeStateUpdateRequestHandler.ACTION,
                        request, handler);
            }
        });
    }

    public static class BenchmarkSemaphores {

        private final ImmutableOpenMap<String, StoppableSemaphore> semaphores;

        BenchmarkSemaphores(List<BenchmarkCompetitor> competitors) {
            ImmutableOpenMap.Builder<String, StoppableSemaphore> builder = ImmutableOpenMap.builder();
            for (BenchmarkCompetitor competitor : competitors) {
                builder.put(competitor.name(), new StoppableSemaphore(competitor.settings().concurrency()));
            }
            semaphores = builder.build();
        }

        void stopAllCompetitors() {
            for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                entry.value.stop();
            }
        }

        void abortAllCompetitors() {
            for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                entry.value.stop();
            }
        }

        void pauseAllCompetitors() throws InterruptedException {
            for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                entry.value.tryAcquireAll(TIMEOUT, TIMEUNIT);
            }
        }

        void resumeAllCompetitors() {
            for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                entry.value.releaseAll();
            }
        }

        StoppableSemaphore competitorSemaphore(String name) {
            return semaphores.get(name);
        }
    }

    protected static final class State {

        private final String benchmarkId;
        private BenchmarkStartRequest request;
        private BenchmarkStartResponse response;
        private BenchmarkSemaphores benchmarkSemaphores;

        private volatile boolean initialized = false;
        private volatile boolean started = false;
        private volatile boolean paused = false;
        private volatile boolean complete = false;

        State(final String benchmarkId) {
            this.benchmarkId = benchmarkId;
        }

        public boolean initialize() {
            if (!initialized) {
                initialized = true;
                return true;
            }
            return false;
        }

        public boolean start() {
            if (initialized && !started) {
                started = true;
                return true;
            }
            return false;
        }

        public boolean pause() throws InterruptedException {
            if (initialized && started && !paused) {
                if (response == null || benchmarkSemaphores == null) {
                    throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: not properly initialized");
                }
                benchmarkSemaphores.pauseAllCompetitors();
                response.state(BenchmarkStartResponse.State.PAUSED);
                paused = true;
                return true;
            }
            return false;
        }

        public boolean resume() {
            if (initialized && started && paused) {
                if (response == null || benchmarkSemaphores == null) {
                    throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: not properly initialized");
                }
                benchmarkSemaphores.resumeAllCompetitors();
                response.state(BenchmarkStartResponse.State.RUNNING);
                paused = false;
                return true;
            }
            return false;
        }

        public boolean abort() {
            if (initialized) {
                if (response == null || benchmarkSemaphores == null) {
                    throw new ElasticsearchIllegalStateException("benchmark [" + benchmarkId + "]: not properly initialized");
                }
                benchmarkSemaphores.abortAllCompetitors();
                response.state(BenchmarkStartResponse.State.ABORTED);
                return true;
            }
            return false;
        }

        public boolean complete() {
            return complete;
        }
    }

    protected BenchmarkMetaData.Entry.NodeState convertToNodeState(BenchmarkStartResponse.State state) {
        switch (state) {
            case INITIALIZING:
                return BenchmarkMetaData.Entry.NodeState.INITIALIZING;
            case RUNNING:
                return BenchmarkMetaData.Entry.NodeState.RUNNING;
            case PAUSED:
                return BenchmarkMetaData.Entry.NodeState.PAUSED;
            case COMPLETED:
                return BenchmarkMetaData.Entry.NodeState.COMPLETED;
            case FAILED:
                return BenchmarkMetaData.Entry.NodeState.FAILED;
            case ABORTED:
                return BenchmarkMetaData.Entry.NodeState.ABORTED;
            default:
                throw new ElasticsearchIllegalStateException("unhandled benchmark response state: " + state);
        }
    }
}

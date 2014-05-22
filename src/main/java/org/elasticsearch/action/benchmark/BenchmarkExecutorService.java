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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.action.benchmark.exception.*;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.HashMap;
import java.util.Map;


/**
 * Service class for executing benchmarks on designated nodes.
 */
public class BenchmarkExecutorService extends AbstractBenchmarkService<BenchmarkExecutorService> {

    protected final BenchmarkExecutor executor;
    protected final Map<String, InternalExecutorState> benchmarks = new HashMap<>();

    @Inject
    public BenchmarkExecutorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                    Client client, TransportService transportService, BenchmarkUtility utility) {

        this(settings, clusterService, threadPool, transportService, new BenchmarkExecutor(client, clusterService), utility);
    }

    protected BenchmarkExecutorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                       TransportService transportService, BenchmarkExecutor executor, BenchmarkUtility utility) {

        super(settings, clusterService, transportService, threadPool, utility);

        this.executor = executor;

        transportService.registerHandler(BenchmarkStatusRequestHandler.ACTION, new BenchmarkStatusRequestHandler());
    }

    @Override
    protected void doStart() throws ElasticsearchException { }
    @Override
    protected void doStop() throws ElasticsearchException { }
    @Override
    protected void doClose() throws ElasticsearchException { }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);
        final BenchmarkMetaData prev = event.previousState().metaData().custom(BenchmarkMetaData.TYPE);

        if (!isBenchmarkNode() || !event.metaDataChanged() || meta == null || meta.entries().size() == 0) {
            return;
        }

        for (final BenchmarkMetaData.Entry entry : BenchmarkMetaData.delta(prev, meta)) {

            if (entry.nodeStateMap().get(nodeId()) == null) {   // Benchmark not assigned to this node. Skip it.
                continue;
            }

            final InternalExecutorState ies = benchmarks.get(entry.benchmarkId());

            switch (entry.state()) {
                case INITIALIZING:
                    if (ies != null || entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.INITIALIZING) {
                        break;  // Benchmark has already been initialized on this node
                    }

                    benchmarks.put(entry.benchmarkId(), new InternalExecutorState(entry.benchmarkId()));

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
                    break;
                case RUNNING:
                    if (entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.RUNNING) {
                        break;
                    }

                    if (ies.canStartExecution()) {

                        final BenchmarkExecutionListener listener = new BenchmarkExecutionListener() {
                            @Override
                            public void onResponse(final BenchmarkStartResponse response) {
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
                                    final BenchmarkStatusNodeActionResponse response = executor.start(ies.request);
                                    ies.response = response.response();
                                    ies.stopExecution();
                                    listener.onResponse(ies.response);
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

                    if (ies.canResumeExecution()) {
                        try {
                            logger.debug("benchmark [{}]: resuming execution", entry.benchmarkId());
                            executor.resume(entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.RUNNING);
                        } catch (Throwable t) {
                            logger.error("benchmark [{}]: failed to resume", t, entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                        }
                    }
                    break;
                case PAUSED:
                    if (entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.RUNNING &&
                        entry.nodeStateMap().get(nodeId()) != BenchmarkMetaData.Entry.NodeState.READY) {
                        break;
                    }

                    try {
                        logger.debug("benchmark [{}]: pausing execution", entry.benchmarkId());
                        executor.pause(entry.benchmarkId());
                        ies.pauseExecution();
                        updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.PAUSED);
                    } catch (Throwable t) {
                        logger.error("benchmark [{}]: failed to pause", t, entry.benchmarkId());
                        updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                    }
                    break;
                case ABORTED:
                    if (entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.COMPLETED &&
                        entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.ABORTED &&
                        entry.nodeStateMap().get(nodeId()) == BenchmarkMetaData.Entry.NodeState.FAILED) {
                        break;
                    }

                    if (ies.canAbortExecution()) {
                        try {
                            executor.abort(entry.benchmarkId());
                            logger.debug("benchmark [{}]: aborted", entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.ABORTED);
                        } catch (Throwable t) {
                            logger.error("benchmark [{}]: failed to abort", t, entry.benchmarkId());
                            updateNodeState(entry.benchmarkId(), nodeId(), BenchmarkMetaData.Entry.NodeState.FAILED);
                        }
                    }
                    break;
                case COMPLETED:
                    if (ies == null) {
                        break;
                    }

                    executor.clear(entry.benchmarkId());
                    benchmarks.remove(entry.benchmarkId());
                    logger.debug("benchmark [{}]: cleared", entry.benchmarkId());
                    break;
                default:
                    throw new BenchmarkIllegalStateException("benchmark [" + entry.benchmarkId() + "]: illegal state [" + entry.state() + "]");
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
            benchmarks.get(response.benchmarkId).request = response.benchmarkStartRequest;

            try {
                // Initialize the benchmark state inside the executor
                executor.create(response.benchmarkStartRequest);

            } catch (Throwable t) {

                logger.error("XXX FAILED TO CREATE BENCHMARK", t);
                // XXX - Make sure this is handled properly
            }

            // Notify the master we are ready to start executing
            final NodeStateUpdateResponseHandler handler = new NodeStateUpdateResponseHandler(benchmarkId);
            final NodeStateUpdateTransportRequest update =
                    new NodeStateUpdateTransportRequest(response.benchmarkId, response.nodeId, BenchmarkMetaData.Entry.NodeState.READY);

            transportService.sendRequest(master(), BenchmarkCoordinatorService.NodeStateUpdateRequestHandler.ACTION, update, handler);
        }

        @Override
        public void handleException(TransportException e) {
            logger.error("benchmark [{}]: failed to receive definition - cannot execute", e, benchmarkId);
            benchmarks.remove(benchmarkId);
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
            benchmarks.remove(benchmarkId);
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

        static final String ACTION = "benchmark/node/status";

        @Override
        public BenchmarkStatusTransportRequest newInstance() {
            return new BenchmarkStatusTransportRequest();
        }

        @Override
        public void messageReceived(final BenchmarkStatusTransportRequest request, final TransportChannel channel) throws Exception {

            final InternalExecutorState ies = benchmarks.get(request.benchmarkId);

            if (ies == null) {
                channel.sendResponse(new BenchmarkIllegalStateException("benchmark [" + request.benchmarkId + "]: missing internal state"));
                return;
            }

            try {
                if (ies.isComplete()) {
                    channel.sendResponse(new BenchmarkStatusNodeActionResponse(ies.benchmarkId, nodeId(), ies.response));
                } else {
                    final BenchmarkStatusNodeActionResponse status = executor.status(request.benchmarkId);
                    if (!status.hasErrors()) {
                        channel.sendResponse(status);
                    } else {
                        channel.sendResponse(new BenchmarkIllegalStateException("benchmark [" + request.benchmarkId + "]: " + status.error()));
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

    protected class InternalExecutorState {

        String                 benchmarkId;
        BenchmarkStartRequest  request;
        BenchmarkStartResponse response;

        private volatile boolean running;
        private volatile boolean paused;
        private volatile boolean complete;

        private final Object lock = new Object();

        InternalExecutorState(String benchmarkId) {
            this.benchmarkId = benchmarkId;
        }

        boolean canStartExecution() {
            synchronized (lock) {
                if (!running && !complete) {
                    running = true;
                    return true;
                }
            }
            return false;
        }

        void stopExecution() {
            synchronized (lock) {
                if (running) {
                    running = false;
                    complete = true;
                }
            }
        }

        void pauseExecution() {
            synchronized (lock) {
                if (running && !paused && !complete) {
                    paused = true;
                }
            }
        }

        boolean canResumeExecution() {
            synchronized (lock) {
                if (paused && running && !complete) {
                    paused = false;
                    return true;
                }
            }
            return false;
        }

        boolean canAbortExecution() {
            synchronized (lock) {
                if (running) {
                    running = false;
                    return true;
                }
            }
            return false;
        }

        boolean isComplete() {
            return complete;
        }
    }
}

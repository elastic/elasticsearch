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
package org.elasticsearch.action.bench;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service component for running benchmarks
 */
public class BenchmarkService extends AbstractLifecycleComponent<BenchmarkService> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportService transportService;
    protected final BenchmarkExecutor executor;

    /**
     * Constructs a service component for running benchmarks
     *
     * @param settings          Settings
     * @param clusterService    Cluster service
     * @param threadPool        Thread pool
     * @param client            Client
     * @param transportService  Transport service
     */
    @Inject
    public BenchmarkService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                            Client client, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.executor = new BenchmarkExecutor(client, clusterService);
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerHandler(BenchExecutionHandler.ACTION, new BenchExecutionHandler());
        transportService.registerHandler(AbortExecutionHandler.ACTION, new AbortExecutionHandler());
        transportService.registerHandler(StatusExecutionHandler.ACTION, new StatusExecutionHandler());
    }

    @Override
    protected void doStart() throws ElasticsearchException { }

    @Override
    protected void doStop() throws ElasticsearchException { }

    @Override
    protected void doClose() throws ElasticsearchException { }

    /**
     * Lists actively running benchmarks on the cluster
     *
     * @param request   Status request
     * @param listener  Response listener
     */
    public void listBenchmarks(final BenchmarkStatusRequest request, final ActionListener<BenchmarkStatusResponse> listener) {

        final List<DiscoveryNode> nodes = availableBenchmarkNodes();
        if (nodes.size() == 0) {
            listener.onResponse(new BenchmarkStatusResponse());
        } else {
            BenchmarkStatusAsyncHandler async = new BenchmarkStatusAsyncHandler(nodes.size(), request, listener);
            for (DiscoveryNode node : nodes) {
                assert isBenchmarkNode(node);
                transportService.sendRequest(node, StatusExecutionHandler.ACTION, new NodeStatusRequest(request), async);
            }
        }
    }

    /**
     * Aborts actively running benchmarks on the cluster
     *
     * @param benchmarkNames Benchmark name(s) to abort
     * @param listener       Response listener
     */
    public void abortBenchmark(final String[] benchmarkNames, final ActionListener<AbortBenchmarkResponse> listener) {

        final List<DiscoveryNode> nodes = availableBenchmarkNodes();
        if (nodes.size() == 0) {
            listener.onFailure(new BenchmarkNodeMissingException("No available nodes for executing benchmarks"));
        } else {
            BenchmarkStateListener benchmarkStateListener = new BenchmarkStateListener() {
                @Override
                public void onResponse(final ClusterState newState, final List<BenchmarkMetaData.Entry> changed) {
                    if (!changed.isEmpty()) {
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                            @Override
                            public void run() {
                                Set<String> names = new HashSet<>();
                                Set<String> nodeNames = new HashSet<>();
                                final ImmutableOpenMap<String, DiscoveryNode> nodes = newState.nodes().nodes();

                                for (BenchmarkMetaData.Entry e : changed) {
                                    names.add(e.benchmarkId());
                                    nodeNames.addAll(Arrays.asList(e.nodes()));
                                }
                                BenchmarkAbortAsyncHandler asyncHandler = new BenchmarkAbortAsyncHandler(nodeNames.size(), listener);
                                String[] benchmarkNames = names.toArray(new String[names.size()]);
                                for (String nodeId : nodeNames) {
                                    final DiscoveryNode node = nodes.get(nodeId);
                                    if (node != null) {
                                        transportService.sendRequest(node, AbortExecutionHandler.ACTION, new NodeAbortRequest(benchmarkNames), asyncHandler);
                                    } else {
                                        asyncHandler.countDown.countDown();
                                        logger.debug("Node for ID [" + nodeId + "] not found in cluster state - skipping");
                                    }
                                }
                            }
                        });
                    } else {
                        listener.onFailure(new BenchmarkMissingException("No benchmarks found for " + Arrays.toString(benchmarkNames)));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    listener.onFailure(t);
                }
            };
            clusterService.submitStateUpdateTask("abort_benchmark", new AbortBenchmarkTask(benchmarkNames, benchmarkStateListener));
        }
    }

    /**
     * Executes benchmarks on the cluster
     *
     * @param request   Benchmark request
     * @param listener  Response listener
     */
    public void startBenchmark(final BenchmarkRequest request, final ActionListener<BenchmarkResponse> listener) {

        final List<DiscoveryNode> nodes = availableBenchmarkNodes();
        if (nodes.size() == 0) {
            listener.onFailure(new BenchmarkNodeMissingException("No available nodes for executing benchmark [" +
                    request.benchmarkName() + "]"));
        } else {
            final BenchmarkStateListener benchListener = new BenchmarkStateListener() {
                @Override
                public void onResponse(final ClusterState newState, final List<BenchmarkMetaData.Entry> entries) {
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                        @Override
                        public void run() {
                            assert entries.size() == 1;
                            BenchmarkMetaData.Entry entry = entries.get(0);
                            final ImmutableOpenMap<String, DiscoveryNode> nodes = newState.nodes().nodes();
                            final BenchmarkSearchAsyncHandler async = new BenchmarkSearchAsyncHandler(entry.nodes().length, request, listener);
                            for (String nodeId : entry.nodes()) {
                                final DiscoveryNode node = nodes.get(nodeId);
                                if (node == null) {
                                    async.handleExceptionInternal(
                                            new ElasticsearchIllegalStateException("Node for ID [" + nodeId + "] not found in cluster state - skipping"));
                                } else {
                                    logger.debug("Starting benchmark [{}] node [{}]", request.benchmarkName(), node.name());
                                    transportService.sendRequest(node, BenchExecutionHandler.ACTION, new NodeBenchRequest(request), async);
                                }
                            }
                        }
                    });
                }

                @Override
                public void onFailure(Throwable t) {
                    listener.onFailure(t);
                }
            };

            clusterService.submitStateUpdateTask("start_benchmark", new StartBenchmarkTask(request, benchListener));
        }
    }

    private void finishBenchmark(final BenchmarkResponse benchmarkResponse, final String benchmarkId, final ActionListener<BenchmarkResponse> listener) {

        clusterService.submitStateUpdateTask("finish_benchmark", new FinishBenchmarkTask("finish_benchmark", benchmarkId, new BenchmarkStateListener() {
            @Override
            public void onResponse(ClusterState newClusterState, List<BenchmarkMetaData.Entry> changed) {
                listener.onResponse(benchmarkResponse);
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        }, (benchmarkResponse.state() != BenchmarkResponse.State.ABORTED) &&
           (benchmarkResponse.state() != BenchmarkResponse.State.FAILED)));
    }

    private final boolean isBenchmarkNode(DiscoveryNode node) {
        ImmutableMap<String, String> attributes = node.getAttributes();
        if (attributes.containsKey("bench")) {
            String bench = attributes.get("bench");
            return Boolean.parseBoolean(bench);
        }
        return false;
    }

    private List<DiscoveryNode> findNodes(BenchmarkRequest request) {
        final int numNodes = request.numExecutorNodes();
        final DiscoveryNodes nodes = clusterService.state().nodes();
        DiscoveryNode localNode = nodes.localNode();
        List<DiscoveryNode> benchmarkNodes = new ArrayList<DiscoveryNode>();
        if (isBenchmarkNode(localNode)) {
            benchmarkNodes.add(localNode);
        }
        for (DiscoveryNode node : nodes) {
            if (benchmarkNodes.size() >= numNodes) {
                return benchmarkNodes;
            }
            if (node != localNode && isBenchmarkNode(node)) {
                benchmarkNodes.add(node);
            }
        }
        return benchmarkNodes;
    }

    private class BenchExecutionHandler extends BaseTransportRequestHandler<NodeBenchRequest> {

        static final String ACTION = "benchmark/executor/start";

        @Override
        public NodeBenchRequest newInstance() {
            return new NodeBenchRequest();
        }

        @Override
        public void messageReceived(NodeBenchRequest request, TransportChannel channel) throws Exception {
            BenchmarkResponse response = executor.benchmark(request.request);
            channel.sendResponse(response);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.BENCH;
        }
    }

    private class StatusExecutionHandler extends BaseTransportRequestHandler<NodeStatusRequest> {

        static final String ACTION = "benchmark/executor/status";

        @Override
        public NodeStatusRequest newInstance() {
            return new NodeStatusRequest();
        }

        @Override
        public void messageReceived(NodeStatusRequest request, TransportChannel channel) throws Exception {
            BenchmarkStatusNodeResponse nodeResponse = executor.benchmarkStatus();
            nodeResponse.nodeName(clusterService.localNode().name());
            channel.sendResponse(nodeResponse);
        }

        @Override
        public String executor() {
            // Perform management tasks on GENERIC so as not to block pending acquisition of a thread from BENCH.
            return ThreadPool.Names.GENERIC;
        }
    }

    private class AbortExecutionHandler extends BaseTransportRequestHandler<NodeAbortRequest> {

        static final String ACTION = "benchmark/executor/abort";

        @Override
        public NodeAbortRequest newInstance() {
            return new NodeAbortRequest();
        }

        @Override
        public void messageReceived(NodeAbortRequest request, TransportChannel channel) throws Exception {
            AbortBenchmarkResponse nodeResponse = executor.abortBenchmark(request.benchmarkNames);
            channel.sendResponse(nodeResponse);
        }

        @Override
        public String executor() {
            // Perform management tasks on GENERIC so as not to block pending acquisition of a thread from BENCH.
            return ThreadPool.Names.GENERIC;
        }
    }

    public static class NodeAbortRequest extends TransportRequest {
        private String[] benchmarkNames;

        public NodeAbortRequest(String[] benchmarkNames) {
            this.benchmarkNames = benchmarkNames;
        }

        public NodeAbortRequest() {
            this(null);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            benchmarkNames = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(benchmarkNames);
        }
    }

    public static class NodeStatusRequest extends TransportRequest {

        final BenchmarkStatusRequest request;

        public NodeStatusRequest(BenchmarkStatusRequest request) {
            this.request = request;
        }

        public NodeStatusRequest() {
            this(new BenchmarkStatusRequest());
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeBenchRequest extends TransportRequest {
        final BenchmarkRequest request;

        public NodeBenchRequest(BenchmarkRequest request) {
            this.request = request;
        }

        public NodeBenchRequest() {
            this(new BenchmarkRequest());
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    private abstract class CountDownAsyncHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        protected final CountDown countDown;
        protected final CopyOnWriteArrayList<T> responses = new CopyOnWriteArrayList<T>();
        protected final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();

        protected CountDownAsyncHandler(int size) {
            countDown = new CountDown(size);
        }

        public abstract T newInstance();
        protected abstract void sendResponse();

        @Override
        public void handleResponse(T t) {
            responses.add(t);
            if (countDown.countDown()) {
                sendResponse();
            }
        }

        @Override
        public void handleException(TransportException t) {
            failures.add(t);
            logger.error(t.getMessage(), t);
            if (countDown.countDown()) {
                sendResponse();
            }
        }

        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    private class BenchmarkAbortAsyncHandler extends CountDownAsyncHandler<AbortBenchmarkResponse> {

        private final ActionListener<AbortBenchmarkResponse> listener;

        public BenchmarkAbortAsyncHandler(int size, ActionListener<AbortBenchmarkResponse> listener) {
            super(size);
            this.listener = listener;
        }

        @Override
        public AbortBenchmarkResponse newInstance() {
            return new AbortBenchmarkResponse();
        }

        @Override
        protected void sendResponse() {
            boolean acked = true;
            for (AbortBenchmarkResponse nodeResponse : responses) {
                if (!nodeResponse.isAcknowledged()) {
                    acked = false;
                    break;
                }
            }
            listener.onResponse(new AbortBenchmarkResponse(acked));
        }
    }

    private class BenchmarkStatusAsyncHandler extends CountDownAsyncHandler<BenchmarkStatusNodeResponse> {

        private final BenchmarkStatusRequest request;
        private final ActionListener<BenchmarkStatusResponse> listener;

        public BenchmarkStatusAsyncHandler(int nodeCount, final BenchmarkStatusRequest request, ActionListener<BenchmarkStatusResponse> listener) {
            super(nodeCount);
            this.request = request;
            this.listener = listener;
        }

        @Override
        public BenchmarkStatusNodeResponse newInstance() {
            return new BenchmarkStatusNodeResponse();
        }

        @Override
        protected void sendResponse() {
            int activeBenchmarks = 0;
            BenchmarkStatusResponse consolidatedResponse = new BenchmarkStatusResponse();
            Map<String, List<BenchmarkResponse>> nameNodeResponseMap = new HashMap<>();

            // Group node responses by benchmark name
            for (BenchmarkStatusNodeResponse nodeResponse : responses) {
                for (BenchmarkResponse benchmarkResponse : nodeResponse.benchResponses()) {
                    List<BenchmarkResponse> benchmarkResponses = nameNodeResponseMap.get(benchmarkResponse.benchmarkName());
                    if (benchmarkResponses == null) {
                        benchmarkResponses = new ArrayList<>();
                        nameNodeResponseMap.put(benchmarkResponse.benchmarkName(), benchmarkResponses);
                    }
                    benchmarkResponses.add(benchmarkResponse);
                }
                activeBenchmarks += nodeResponse.activeBenchmarks();
            }

            for (Map.Entry<String, List<BenchmarkResponse>> entry : nameNodeResponseMap.entrySet()) {
                BenchmarkResponse consolidated = consolidateBenchmarkResponses(entry.getValue());
                consolidatedResponse.addBenchResponse(consolidated);
            }

            consolidatedResponse.totalActiveBenchmarks(activeBenchmarks);
            listener.onResponse(consolidatedResponse);
        }
    }

    private BenchmarkResponse consolidateBenchmarkResponses(List<BenchmarkResponse> responses) {
        BenchmarkResponse response = new BenchmarkResponse();

        // Merge node responses into a single consolidated response
        List<String> errors = new ArrayList<>();
        for (BenchmarkResponse r : responses) {
            for (Map.Entry<String, CompetitionResult> entry : r.competitionResults.entrySet()) {
                if (!response.competitionResults.containsKey(entry.getKey())) {
                    response.competitionResults.put(entry.getKey(),
                            new CompetitionResult(
                                    entry.getKey(), entry.getValue().concurrency(), entry.getValue().multiplier(),
                                    false, entry.getValue().percentiles()));
                }
                CompetitionResult cr = response.competitionResults.get(entry.getKey());
                cr.nodeResults().addAll(entry.getValue().nodeResults());
            }
            if (r.hasErrors()) {
                for (String error : r.errors()) {
                    errors.add(error);
                }
            }

            if (response.benchmarkName() == null) {
                response.benchmarkName(r.benchmarkName());
            }
            assert response.benchmarkName().equals(r.benchmarkName());
            if (!errors.isEmpty()) {
                response.errors(errors.toArray(new String[errors.size()]));
            }
            response.mergeState(r.state());
            assert errors.isEmpty() || response.state() != BenchmarkResponse.State.COMPLETE : "Response can't be complete since it has errors";
        }

        return response;
    }

    private class BenchmarkSearchAsyncHandler extends CountDownAsyncHandler<BenchmarkResponse> {

        private final ActionListener<BenchmarkResponse> listener;
        private final BenchmarkRequest request;

        public BenchmarkSearchAsyncHandler(int size, BenchmarkRequest request, ActionListener<BenchmarkResponse> listener) {
            super(size);
            this.listener = listener;
            this.request = request;
        }

        @Override
        public BenchmarkResponse newInstance() {
            return new BenchmarkResponse();
        }

        @Override
        protected void sendResponse() {
            BenchmarkResponse response = consolidateBenchmarkResponses(responses);
            response.benchmarkName(request.benchmarkName());
            response.verbose(request.verbose());
            finishBenchmark(response, request.benchmarkName(), listener);
        }

        public void handleExceptionInternal(Throwable t) {
            failures.add(t);
            if (countDown.countDown()) {
                sendResponse();
            }
        }
    }

    public static interface BenchmarkStateListener {

        void onResponse(ClusterState newClusterState, List<BenchmarkMetaData.Entry> changed);

        void onFailure(Throwable t);
    }

    public final class StartBenchmarkTask extends BenchmarkStateChangeAction<BenchmarkRequest> {

        private final BenchmarkStateListener stateListener;
        private List<BenchmarkMetaData.Entry> newBenchmark = new ArrayList<>();

        public StartBenchmarkTask(BenchmarkRequest request, BenchmarkStateListener stateListener) {
            super(request);
            this.stateListener = stateListener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            MetaData metaData = currentState.getMetaData();
            BenchmarkMetaData bmd = metaData.custom(BenchmarkMetaData.TYPE);
            MetaData.Builder mdBuilder = MetaData.builder(metaData);
            ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();

            if (bmd != null) {
                for (BenchmarkMetaData.Entry entry : bmd.entries()) {
                    if (request.benchmarkName().equals(entry.benchmarkId())){
                        if (entry.state() != BenchmarkMetaData.State.SUCCESS && entry.state() != BenchmarkMetaData.State.FAILED) {
                            throw new ElasticsearchException("A benchmark with ID [" + request.benchmarkName() + "] is already running in state [" + entry.state() + "]");
                        }
                        // just drop the entry it it has finished successfully or it failed!
                    } else {
                        builder.add(entry);
                    }
                }
            }
            List<DiscoveryNode> nodes = findNodes(request);
            String[] nodeIds = new String[nodes.size()];
            int i = 0;
            for (DiscoveryNode node : nodes) {
                nodeIds[i++] = node.getId();
            }
            BenchmarkMetaData.Entry entry = new BenchmarkMetaData.Entry(request.benchmarkName(), BenchmarkMetaData.State.STARTED, nodeIds);
            newBenchmark.add(entry);
            bmd = new BenchmarkMetaData(builder.add(entry).build());
            mdBuilder.putCustom(BenchmarkMetaData.TYPE, bmd);
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.warn("Failed to start benchmark: [{}]", t, request.benchmarkName());
            newBenchmark = null;
            stateListener.onFailure(t);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
            if (newBenchmark != null) {
                stateListener.onResponse(newState, newBenchmark);
            }
        }

        @Override
        public TimeValue timeout() {
            return request.masterNodeTimeout();
        }
    }

    public final class FinishBenchmarkTask extends UpdateBenchmarkStateTask {

        private final boolean success;

        public FinishBenchmarkTask(String reason, String benchmarkId, BenchmarkStateListener listener, boolean success) {
            super(reason, benchmarkId, listener);
            this.success = success;
        }

        @Override
        protected BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry) {
            BenchmarkMetaData.State state = entry.state();
            assert state == BenchmarkMetaData.State.STARTED || state == BenchmarkMetaData.State.ABORTED :  "Expected state: STARTED or ABORTED but was: " + entry.state();
            if (success) {
                return new BenchmarkMetaData.Entry(entry, BenchmarkMetaData.State.SUCCESS);
            } else {
                return new BenchmarkMetaData.Entry(entry, BenchmarkMetaData.State.FAILED);
            }
        }
    }

    public final class AbortBenchmarkTask extends UpdateBenchmarkStateTask {
        private final String[] patterns;

        public AbortBenchmarkTask(String[] patterns, BenchmarkStateListener listener) {
            super("abort_benchmark", null , listener);
            this.patterns = patterns;
        }

        protected boolean match(BenchmarkMetaData.Entry entry) {
            return entry.state() == BenchmarkMetaData.State.STARTED && Regex.simpleMatch(this.patterns, benchmarkId);
        }

        @Override
        protected BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry) {
            return new BenchmarkMetaData.Entry(entry, BenchmarkMetaData.State.ABORTED);
        }
    }

    public abstract class UpdateBenchmarkStateTask implements ProcessedClusterStateUpdateTask {

        private final String reason;
        protected final String benchmarkId;
        private final BenchmarkStateListener listener;
        private final List<BenchmarkMetaData.Entry> instances = new ArrayList<>();


        protected UpdateBenchmarkStateTask(String reason, String benchmarkId, BenchmarkStateListener listener) {
            this.reason = reason;
            this.listener = listener;
            this.benchmarkId = benchmarkId;
        }

        protected boolean match(BenchmarkMetaData.Entry entry) {
            return entry.benchmarkId().equals(this.benchmarkId);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            MetaData metaData = currentState.getMetaData();
            BenchmarkMetaData bmd = metaData.custom(BenchmarkMetaData.TYPE);
            MetaData.Builder mdBuilder = MetaData.builder(metaData);
            if (bmd != null && !bmd.entries().isEmpty()) {
                ImmutableList.Builder<BenchmarkMetaData.Entry> builder = new ImmutableList.Builder<BenchmarkMetaData.Entry>();
                for (BenchmarkMetaData.Entry e : bmd.entries()) {
                    if (benchmarkId == null || match(e)) {
                        e = process(e) ;
                        instances.add(e);
                    }
                    // Don't keep finished benchmarks around in cluster state
                    if (e != null && (e.state() != BenchmarkMetaData.State.SUCCESS &&
                            e.state() != BenchmarkMetaData.State.ABORTED &&
                            e.state() != BenchmarkMetaData.State.FAILED)) {
                        builder.add(e);
                    }
                }
                if (instances.isEmpty()) {
                    throw new ElasticsearchException("No Benchmark found for id: [" + benchmarkId + "]");
                }
                bmd = new BenchmarkMetaData(builder.build());
            }
            if (bmd != null) {
                mdBuilder.putCustom(BenchmarkMetaData.TYPE, bmd);
            }
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }

        protected abstract BenchmarkMetaData.Entry process(BenchmarkMetaData.Entry entry);

        @Override
        public void onFailure(String source, Throwable t) {
            logger.warn("Failed updating benchmark state for ID [{}] triggered by: [{}]", t, benchmarkId, reason);
            listener.onFailure(t);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
            listener.onResponse(newState, instances);
        }

        public String reason() {
            return reason;
        }
    }

    public abstract class BenchmarkStateChangeAction<R extends MasterNodeOperationRequest> implements TimeoutClusterStateUpdateTask {
        protected final R request;

        public BenchmarkStateChangeAction(R request) {
            this.request = request;
        }

        @Override
        public TimeValue timeout() {
            return request.masterNodeTimeout();
        }
    }

    private List<DiscoveryNode> availableBenchmarkNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        List<DiscoveryNode> benchmarkNodes = new ArrayList<>(nodes.size());
        for (DiscoveryNode node : nodes) {
            if (isBenchmarkNode(node)) {
                benchmarkNodes.add(node);
            }
        }
        return benchmarkNodes;
    }
}

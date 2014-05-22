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

import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.benchmark.exception.BenchmarkIllegalStateException;
import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for benchmark coordinator and executor services.
 */
public abstract class AbstractBenchmarkService<T> extends AbstractLifecycleComponent<T> implements ClusterStateListener {

    protected final ClusterService   clusterService;
    protected final TransportService transportService;
    protected final ThreadPool       threadPool;
    protected final BenchmarkUtility utility;

    protected AbstractBenchmarkService(Settings settings, ClusterService clusterService, TransportService transportService,
                                       ThreadPool threadPool, BenchmarkUtility utility) {

        super(settings);

        this.clusterService   = clusterService;
        this.transportService = transportService;
        this.threadPool       = threadPool;
        this.utility          = utility;

        clusterService.add(this);
    }

    /**
     * Request class for fetching benchmark definition from master to executor nodes
     */
    public class BenchmarkDefinitionTransportRequest extends BaseNodeTransportRequest {

        public BenchmarkDefinitionTransportRequest() { }

        public BenchmarkDefinitionTransportRequest(String benchmarkId, String nodeId) {
            super(benchmarkId, nodeId);
        }
    }

    /**
     * Response class for transmitting benchmark definitions to executor nodes
     */
    public class BenchmarkDefinitionActionResponse extends BaseNodeActionResponse {

        protected BenchmarkStartRequest benchmarkStartRequest;

        public BenchmarkDefinitionActionResponse() { }

        public BenchmarkDefinitionActionResponse(BenchmarkStartRequest benchmarkStartRequest, String nodeId) {
            super(benchmarkStartRequest.benchmarkId(), nodeId);
            this.benchmarkStartRequest = benchmarkStartRequest;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            benchmarkStartRequest = new BenchmarkStartRequest();
            benchmarkStartRequest.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            benchmarkStartRequest.writeTo(out);
        }
    }

    /**
     * Request class for informing master of node state changes
     */
    public class NodeStateUpdateTransportRequest extends BaseNodeTransportRequest {

        protected BenchmarkMetaData.Entry.NodeState state;

        public NodeStateUpdateTransportRequest() { }

        public NodeStateUpdateTransportRequest(String benchmarkId, String nodeId, BenchmarkMetaData.Entry.NodeState state) {
            super(benchmarkId, nodeId);
            this.state = state;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            state = BenchmarkMetaData.Entry.NodeState.fromId(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeByte(state.id());
        }
    }

    /**
     * Response class for acknowledging status update requests
     */
    public class NodeStateUpdateActionResponse extends BaseNodeActionResponse {

        public NodeStateUpdateActionResponse() { }

        public NodeStateUpdateActionResponse(String benchmarkId, String nodeId) {
            super(benchmarkId, nodeId);
        }
    }

    /* ** Utilities ** */

    public static interface BenchmarkExecutionListener {
        void onResponse(BenchmarkStartResponse response);
        void onFailure(Throwable t);
    }

    protected boolean isMasterNode() {
        return clusterService.state().nodes().localNodeMaster();
    }

    protected boolean isBenchmarkNode() {
        return BenchmarkUtility.isBenchmarkNode(clusterService.localNode());
    }

    protected String nodeId() {
        return clusterService.localNode().id();
    }

    protected DiscoveryNode master() {
        return clusterService.state().nodes().masterNode();
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
                throw new BenchmarkIllegalStateException("unhandled benchmark response state: " + state);
        }
    }

    /* ** */

    public class NodeStateUpdateRequestHandler extends BaseTransportRequestHandler<NodeStateUpdateTransportRequest> {

        static final String ACTION = "benchmark/node/update_state";

        @Override
        public NodeStateUpdateTransportRequest newInstance() {
            return new NodeStateUpdateTransportRequest();
        }

        @Override
        public void messageReceived(final NodeStateUpdateTransportRequest request, final TransportChannel channel) throws Exception {

            logger.debug("benchmark [{}]: state update request [{}] from [{}]", request.benchmarkId, request.state, request.nodeId);

            final String cause = "benchmark-update-node-state (" + request.benchmarkId + ":" + request.nodeId + ":" + request.state + ")";

            clusterService.submitStateUpdateTask(cause, new ProcessedClusterStateUpdateTask() {

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        channel.sendResponse(new NodeStateUpdateActionResponse(request.benchmarkId, request.nodeId));
                    }
                    catch (Throwable t) {
                        onFailure(source, t);
                    }
                }

                @Override
                public ClusterState execute(ClusterState state) throws Exception {

                    final BenchmarkMetaData                              meta = state.metaData().custom(BenchmarkMetaData.TYPE);
                    final ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();

                    for (BenchmarkMetaData.Entry entry : meta.entries()) {
                        if (!entry.benchmarkId().equals(request.benchmarkId)) {
                            builder.add(entry);
                        } else {
                            final Map<String, BenchmarkMetaData.Entry.NodeState> map = new HashMap<>();
                            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> mapEntry : entry.nodeStateMap().entrySet()) {
                                if (mapEntry.getKey().equals(request.nodeId)) {
                                    map.put(mapEntry.getKey(), request.state);  // Update node state with requested state
                                } else {
                                    map.put(mapEntry.getKey(), mapEntry.getValue());
                                }
                            }
                            builder.add(new BenchmarkMetaData.Entry(request.benchmarkId, entry.state(), map));
                        }
                    }

                    final MetaData.Builder metabuilder = MetaData.builder(state.metaData());
                    metabuilder.putCustom(BenchmarkMetaData.TYPE, new BenchmarkMetaData(builder.build()));

                    return ClusterState.builder(state).metaData(metabuilder).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    try {
                        logger.error(source, t);
                        channel.sendResponse(t);
                    } catch (Throwable e) {
                        logger.error("Unable to send failure response: {}", source, e);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

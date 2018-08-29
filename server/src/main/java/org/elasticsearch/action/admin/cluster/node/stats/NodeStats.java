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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.AdaptiveSelectionStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Node statistics (dynamic, changes depending on when created).
 */
public class NodeStats extends BaseNodeResponse implements ToXContentFragment {

    private long timestamp;

    @Nullable
    private NodeIndicesStats indices;

    @Nullable
    private OsStats os;

    @Nullable
    private ProcessStats process;

    @Nullable
    private JvmStats jvm;

    @Nullable
    private ThreadPoolStats threadPool;

    @Nullable
    private FsInfo fs;

    @Nullable
    private TransportStats transport;

    @Nullable
    private HttpStats http;

    @Nullable
    private AllCircuitBreakerStats breaker;

    @Nullable
    private ScriptStats scriptStats;

    @Nullable
    private DiscoveryStats discoveryStats;

    @Nullable
    private IngestStats ingestStats;

    @Nullable
    private AdaptiveSelectionStats adaptiveSelectionStats;

    @Nullable
    private List<ShardStats> shardsStats;

    NodeStats() {
    }

    public NodeStats(DiscoveryNode node, long timestamp, @Nullable NodeIndicesStats indices,
                     @Nullable OsStats os, @Nullable ProcessStats process, @Nullable JvmStats jvm, @Nullable ThreadPoolStats threadPool,
                     @Nullable FsInfo fs, @Nullable TransportStats transport, @Nullable HttpStats http,
                     @Nullable AllCircuitBreakerStats breaker,
                     @Nullable ScriptStats scriptStats,
                     @Nullable DiscoveryStats discoveryStats,
                     @Nullable IngestStats ingestStats,
                     @Nullable AdaptiveSelectionStats adaptiveSelectionStats,
                     @Nullable List<ShardStats> shardsStats) {
        super(node);
        this.timestamp = timestamp;
        this.indices = indices;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.fs = fs;
        this.transport = transport;
        this.http = http;
        this.breaker = breaker;
        this.scriptStats = scriptStats;
        this.discoveryStats = discoveryStats;
        this.ingestStats = ingestStats;
        this.adaptiveSelectionStats = adaptiveSelectionStats;
        this.shardsStats = shardsStats;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    /**
     * Indices level stats.
     */
    @Nullable
    public NodeIndicesStats getIndices() {
        return this.indices;
    }

    /**
     * Operating System level statistics.
     */
    @Nullable
    public OsStats getOs() {
        return this.os;
    }

    /**
     * Process level statistics.
     */
    @Nullable
    public ProcessStats getProcess() {
        return process;
    }

    /**
     * JVM level statistics.
     */
    @Nullable
    public JvmStats getJvm() {
        return jvm;
    }

    /**
     * Thread Pool level statistics.
     */
    @Nullable
    public ThreadPoolStats getThreadPool() {
        return this.threadPool;
    }

    /**
     * File system level stats.
     */
    @Nullable
    public FsInfo getFs() {
        return fs;
    }

    @Nullable
    public TransportStats getTransport() {
        return this.transport;
    }

    @Nullable
    public HttpStats getHttp() {
        return this.http;
    }

    @Nullable
    public AllCircuitBreakerStats getBreaker() {
        return this.breaker;
    }

    @Nullable
    public ScriptStats getScriptStats() {
        return this.scriptStats;
    }

    @Nullable
    public DiscoveryStats getDiscoveryStats() {
        return this.discoveryStats;
    }

    @Nullable
    public IngestStats getIngestStats() {
        return ingestStats;
    }

    @Nullable
    public AdaptiveSelectionStats getAdaptiveSelectionStats() {
        return adaptiveSelectionStats;
    }

    @Nullable
    public List<ShardStats> getShardsStats() {
        return shardsStats;
    }

    public static NodeStats readNodeStats(StreamInput in) throws IOException {
        NodeStats nodeInfo = new NodeStats();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timestamp = in.readVLong();
        if (in.readBoolean()) {
            indices = NodeIndicesStats.readIndicesStats(in);
        }
        os = in.readOptionalWriteable(OsStats::new);
        process = in.readOptionalWriteable(ProcessStats::new);
        jvm = in.readOptionalWriteable(JvmStats::new);
        threadPool = in.readOptionalWriteable(ThreadPoolStats::new);
        fs = in.readOptionalWriteable(FsInfo::new);
        transport = in.readOptionalWriteable(TransportStats::new);
        http = in.readOptionalWriteable(HttpStats::new);
        breaker = in.readOptionalWriteable(AllCircuitBreakerStats::new);
        scriptStats = in.readOptionalWriteable(ScriptStats::new);
        discoveryStats = in.readOptionalWriteable(DiscoveryStats::new);
        ingestStats = in.readOptionalWriteable(IngestStats::new);
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            adaptiveSelectionStats = in.readOptionalWriteable(AdaptiveSelectionStats::new);
        } else {
            adaptiveSelectionStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_6_5_0) && in.readBoolean()) {
            shardsStats = in.readList(ShardStats::readShardStats);
        } else {
            shardsStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        if (indices == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indices.writeTo(out);
        }
        out.writeOptionalWriteable(os);
        out.writeOptionalWriteable(process);
        out.writeOptionalWriteable(jvm);
        out.writeOptionalWriteable(threadPool);
        out.writeOptionalWriteable(fs);
        out.writeOptionalWriteable(transport);
        out.writeOptionalWriteable(http);
        out.writeOptionalWriteable(breaker);
        out.writeOptionalWriteable(scriptStats);
        out.writeOptionalWriteable(discoveryStats);
        out.writeOptionalWriteable(ingestStats);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeOptionalWriteable(adaptiveSelectionStats);
        }
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            if (shardsStats == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeList(shardsStats);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getAddress());

        builder.startArray("roles");
        for (DiscoveryNode.Role role : getNode().getRoles()) {
            builder.value(role.getRoleName());
        }
        builder.endArray();

        if (!getNode().getAttributes().isEmpty()) {
            builder.startObject("attributes");
            for (Map.Entry<String, String> attrEntry : getNode().getAttributes().entrySet()) {
                builder.field(attrEntry.getKey(), attrEntry.getValue());
            }
            builder.endObject();
        }

        if (getIndices() != null) {
            getIndices().toXContent(builder, params);
        }
        if (getOs() != null) {
            getOs().toXContent(builder, params);
        }
        if (getProcess() != null) {
            getProcess().toXContent(builder, params);
        }
        if (getJvm() != null) {
            getJvm().toXContent(builder, params);
        }
        if (getThreadPool() != null) {
            getThreadPool().toXContent(builder, params);
        }
        if (getFs() != null) {
            getFs().toXContent(builder, params);
        }
        if (getTransport() != null) {
            getTransport().toXContent(builder, params);
        }
        if (getHttp() != null) {
            getHttp().toXContent(builder, params);
        }
        if (getBreaker() != null) {
            getBreaker().toXContent(builder, params);
        }
        if (getScriptStats() != null) {
            getScriptStats().toXContent(builder, params);
        }
        if (getDiscoveryStats() != null) {
            getDiscoveryStats().toXContent(builder, params);
        }
        if (getIngestStats() != null) {
            getIngestStats().toXContent(builder, params);
        }
        if (getAdaptiveSelectionStats() != null) {
            getAdaptiveSelectionStats().toXContent(builder, params);
        }
        if (getShardsStats() != null) {
            builder.startObject("shards");
            {
                final BiFunction<String, Integer, Integer> increment = (k, v) -> v + 1;
                final Map<String, Integer> routingStats = new HashMap<>(8);

                routingStats.put("total", shardsStats.size());
                routingStats.put("primaries", 0);
                routingStats.put("replicas", 0);
                routingStats.put("unassigned", 0);
                routingStats.put("initializing", 0);
                routingStats.put("active", 0);
                routingStats.put("relocating_to_node", 0);
                routingStats.put("relocating_from_node", 0);

                builder.startArray("stats");
                for (final ShardStats shard : getShardsStats()) {
                    final ShardRouting routing = shard.getShardRouting();

                    // accumulate routing stats; note: node_stats already has index stats rolled up
                    if (routing.active()) {
                        routingStats.compute("active", increment);
                        if (routing.relocating()) {
                            routingStats.compute("relocating_from_node", increment);
                        }
                    } else if (routing.initializing()) {
                        routingStats.compute("initializing", increment);
                        if (routing.relocatingNodeId() != null) {
                            routingStats.compute("relocating_to_node", increment);
                        }
                    } else if (routing.unassigned()) {
                        routingStats.compute("unassigned", increment);
                    }

                    if (routing.primary()) {
                        routingStats.compute("primaries", increment);
                    } else {
                        routingStats.compute("replicas", increment);
                    }

                    builder.startObject();
                    {
                        builder.field("routing", routing);
                        // inactive (e.g., UNASSIGNED, INITIALIZING) shards have no stats, so do not report false zeros
                        if (routing.active()) {
                            shard.getStats().toXContent(builder, params);
                            if (shard.getCommitStats() != null) {
                                shard.getCommitStats().toXContent(builder, params);
                            }
                            if (shard.getSeqNoStats() != null) {
                                shard.getSeqNoStats().toXContent(builder, params);
                            }
                        }
                    }
                    builder.endObject();
                }
                builder.endArray();

                // add rolled up routing stats
                builder.field("routing")
                       .map(routingStats);
            }
            builder.endObject();
        }
        return builder;
    }
}

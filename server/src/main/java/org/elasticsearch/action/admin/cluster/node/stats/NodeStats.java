/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.stats.IndexingPressureStats;
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
import java.util.Map;

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
    private IndexingPressureStats indexingPressureStats;

    public NodeStats(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        if (in.readBoolean()) {
            indices = new NodeIndicesStats(in);
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
        adaptiveSelectionStats = in.readOptionalWriteable(AdaptiveSelectionStats::new);
        indexingPressureStats = in.readOptionalWriteable(IndexingPressureStats::new);
    }

    public NodeStats(DiscoveryNode node, long timestamp, @Nullable NodeIndicesStats indices,
                     @Nullable OsStats os, @Nullable ProcessStats process, @Nullable JvmStats jvm, @Nullable ThreadPoolStats threadPool,
                     @Nullable FsInfo fs, @Nullable TransportStats transport, @Nullable HttpStats http,
                     @Nullable AllCircuitBreakerStats breaker,
                     @Nullable ScriptStats scriptStats,
                     @Nullable DiscoveryStats discoveryStats,
                     @Nullable IngestStats ingestStats,
                     @Nullable AdaptiveSelectionStats adaptiveSelectionStats,
                     @Nullable IndexingPressureStats indexingPressureStats) {
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
        this.indexingPressureStats = indexingPressureStats;
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
    public IndexingPressureStats getIndexingPressureStats() {
        return indexingPressureStats;
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
        out.writeOptionalWriteable(adaptiveSelectionStats);
        out.writeOptionalWriteable(indexingPressureStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getAddress());

        builder.startArray("roles");
        for (DiscoveryNodeRole role : getNode().getRoles()) {
            builder.value(role.roleName());
        }
        builder.endArray();

        if (getNode().getAttributes().isEmpty() == false) {
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
        if (getIndexingPressureStats() != null) {
            getIndexingPressureStats().toXContent(builder, params);
        }
        return builder;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.script.ScriptCacheStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.singleChunk;

/**
 * Node statistics (dynamic, changes depending on when created).
 */
public class NodeStats extends BaseNodeResponse implements ChunkedToXContent {

    private final long timestamp;

    @Nullable
    private NodeIndicesStats indices;

    @Nullable
    private final OsStats os;

    @Nullable
    private final ProcessStats process;

    @Nullable
    private final JvmStats jvm;

    @Nullable
    private final ThreadPoolStats threadPool;

    @Nullable
    private final FsInfo fs;

    @Nullable
    private final TransportStats transport;

    @Nullable
    private final HttpStats http;

    @Nullable
    private final AllCircuitBreakerStats breaker;

    @Nullable
    private final ScriptStats scriptStats;

    @Nullable
    private final ScriptCacheStats scriptCacheStats;

    @Nullable
    private final DiscoveryStats discoveryStats;

    @Nullable
    private final IngestStats ingestStats;

    @Nullable
    private final AdaptiveSelectionStats adaptiveSelectionStats;

    @Nullable
    private final IndexingPressureStats indexingPressureStats;

    @Nullable
    private final RepositoriesStats repositoriesStats;

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
        scriptStats = in.readOptionalWriteable(ScriptStats::read);
        scriptCacheStats = scriptStats != null ? scriptStats.toScriptCacheStats() : null;
        discoveryStats = in.readOptionalWriteable(DiscoveryStats::new);
        ingestStats = in.readOptionalWriteable(IngestStats::read);
        adaptiveSelectionStats = in.readOptionalWriteable(AdaptiveSelectionStats::new);
        indexingPressureStats = in.readOptionalWriteable(IndexingPressureStats::new);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_011)) {
            repositoriesStats = in.readOptionalWriteable(RepositoriesStats::new);
        } else {
            repositoriesStats = null;
        }
    }

    public NodeStats(
        DiscoveryNode node,
        long timestamp,
        @Nullable NodeIndicesStats indices,
        @Nullable OsStats os,
        @Nullable ProcessStats process,
        @Nullable JvmStats jvm,
        @Nullable ThreadPoolStats threadPool,
        @Nullable FsInfo fs,
        @Nullable TransportStats transport,
        @Nullable HttpStats http,
        @Nullable AllCircuitBreakerStats breaker,
        @Nullable ScriptStats scriptStats,
        @Nullable DiscoveryStats discoveryStats,
        @Nullable IngestStats ingestStats,
        @Nullable AdaptiveSelectionStats adaptiveSelectionStats,
        @Nullable ScriptCacheStats scriptCacheStats,
        @Nullable IndexingPressureStats indexingPressureStats,
        @Nullable RepositoriesStats repositoriesStats
    ) {
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
        this.scriptCacheStats = scriptCacheStats;
        this.indexingPressureStats = indexingPressureStats;
        this.repositoriesStats = repositoriesStats;
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
    public ScriptCacheStats getScriptCacheStats() {
        return scriptCacheStats;
    }

    @Nullable
    public IndexingPressureStats getIndexingPressureStats() {
        return indexingPressureStats;
    }

    @Nullable
    public RepositoriesStats getRepositoriesStats() {
        return repositoriesStats;
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
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_011)) {
            out.writeOptionalWriteable(repositoriesStats);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {

        return Iterators.concat(

            Iterators.single((builder, params) -> {
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

                return builder;
            }),

            ifPresent(getIndices()).toXContentChunked(outerParams),

            singleChunk(ifPresent(getOs()), ifPresent(getProcess()), ifPresent(getJvm())),

            ifPresent(getThreadPool()).toXContentChunked(outerParams),
            singleChunk(ifPresent(getFs())),
            ifPresent(getTransport()).toXContentChunked(outerParams),
            ifPresent(getHttp()).toXContentChunked(outerParams),
            singleChunk(ifPresent(getBreaker())),
            ifPresent(getScriptStats()).toXContentChunked(outerParams),
            singleChunk(ifPresent(getDiscoveryStats())),
            ifPresent(getIngestStats()).toXContentChunked(outerParams),
            singleChunk(ifPresent(getAdaptiveSelectionStats())),
            ifPresent(getScriptCacheStats()).toXContentChunked(outerParams),
            singleChunk(ifPresent(getIndexingPressureStats())),
            singleChunk(ifPresent(getRepositoriesStats()))
        );
    }

    private static ChunkedToXContent ifPresent(@Nullable ChunkedToXContent chunkedToXContent) {
        return Objects.requireNonNullElse(chunkedToXContent, ChunkedToXContent.EMPTY);
    }

    private static ToXContent ifPresent(@Nullable ToXContent toXContent) {
        return Objects.requireNonNullElse(toXContent, ToXContent.EMPTY);
    }
}

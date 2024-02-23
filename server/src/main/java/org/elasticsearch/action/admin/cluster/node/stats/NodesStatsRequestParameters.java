/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class encapsulates the metrics and other information needed to define scope when we are requesting node stats.
 */
public class NodesStatsRequestParameters implements Writeable {
    private CommonStatsFlags indices = new CommonStatsFlags();
    private final Set<String> requestedMetrics = new HashSet<>();
    private boolean includeShardsStats = true;

    public NodesStatsRequestParameters() {}

    public NodesStatsRequestParameters(StreamInput in) throws IOException {
        indices = new CommonStatsFlags(in);
        requestedMetrics.clear();
        requestedMetrics.addAll(in.readStringCollectionAsList());
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            includeShardsStats = in.readBoolean();
        } else {
            includeShardsStats = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indices.writeTo(out);
        out.writeStringCollection(requestedMetrics);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeBoolean(includeShardsStats);
        }
    }

    public CommonStatsFlags indices() {
        return indices;
    }

    public void setIndices(CommonStatsFlags indices) {
        this.indices = indices;
    }

    public Set<String> requestedMetrics() {
        return requestedMetrics;
    }

    public boolean includeShardsStats() {
        return includeShardsStats;
    }

    public void setIncludeShardsStats(boolean includeShardsStats) {
        this.includeShardsStats = includeShardsStats;
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes stats endpoint. Eventually this list will be pluggable.
     */
    public enum Metric {
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("thread_pool"),
        FS("fs"),
        TRANSPORT("transport"),
        HTTP("http"),
        BREAKER("breaker"),
        SCRIPT("script"),
        DISCOVERY("discovery"),
        INGEST("ingest"),
        ADAPTIVE_SELECTION("adaptive_selection"),
        SCRIPT_CACHE("script_cache"),
        INDEXING_PRESSURE("indexing_pressure"),
        REPOSITORIES("repositories");

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

        static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }
}

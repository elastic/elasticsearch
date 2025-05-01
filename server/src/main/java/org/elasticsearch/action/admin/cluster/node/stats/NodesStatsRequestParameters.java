/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * This class encapsulates the metrics and other information needed to define scope when we are requesting node stats.
 */
public class NodesStatsRequestParameters implements Writeable {
    private CommonStatsFlags indices = new CommonStatsFlags();
    private final EnumSet<Metric> requestedMetrics;
    private boolean includeShardsStats = true;

    public NodesStatsRequestParameters() {
        this.requestedMetrics = EnumSet.noneOf(Metric.class);
    }

    public NodesStatsRequestParameters(StreamInput in) throws IOException {
        indices = new CommonStatsFlags(in);
        requestedMetrics = Metric.readSetFrom(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            includeShardsStats = in.readBoolean();
        } else {
            includeShardsStats = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indices.writeTo(out);
        Metric.writeSetTo(out, requestedMetrics);
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

    public EnumSet<Metric> requestedMetrics() {
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
        REPOSITORIES("repositories"),
        ALLOCATIONS("allocations");

        public static final Set<Metric> ALL = Collections.unmodifiableSet(EnumSet.allOf(Metric.class));
        public static final Set<String> ALL_NAMES = ALL.stream().map(Metric::metricName).collect(toUnmodifiableSet());
        public static final Map<String, Metric> NAMES_MAP = ALL.stream().collect(toUnmodifiableMap(Metric::metricName, m -> m));
        private final String metricName;

        Metric(String metricName) {
            this.metricName = metricName;
        }

        public static boolean isValid(String name) {
            return NAMES_MAP.containsKey(name);
        }

        public static Metric get(String name) {
            var metric = NAMES_MAP.get(name);
            assert metric != null;
            return metric;
        }

        public static void writeSetTo(StreamOutput out, EnumSet<Metric> metrics) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeEnumSet(metrics);
            } else {
                out.writeCollection(metrics, (output, metric) -> output.writeString(metric.metricName));
            }
        }

        public static EnumSet<Metric> readSetFrom(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                return in.readEnumSet(Metric.class);
            } else {
                return in.readCollection((i) -> EnumSet.noneOf(Metric.class), (is, out) -> {
                    var name = is.readString();
                    var metric = Metric.get(name);
                    out.add(metric);
                });
            }
        }

        public String metricName() {
            return metricName;
        }

        @Override
        public String toString() {
            return metricName;
        }
    }
}

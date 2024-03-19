/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is a container that encapsulates the necessary information needed to indicate which node information is requested.
 */
public class NodesInfoMetrics implements Writeable {
    private final Set<String> requestedMetrics;

    public NodesInfoMetrics() {
        requestedMetrics = new HashSet<>(Metric.allMetrics());
    }

    public NodesInfoMetrics(StreamInput in) throws IOException {
        requestedMetrics = in.readCollectionAsImmutableSet(StreamInput::readString);
    }

    public Set<String> requestedMetrics() {
        return requestedMetrics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(requestedMetrics);
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes information endpoint. Eventually this list will be
     * pluggable.
     */
    public enum Metric {
        SETTINGS("settings"),
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("thread_pool"),
        TRANSPORT("transport"),
        HTTP("http"),
        REMOTE_CLUSTER_SERVER("remote_cluster_server"),
        PLUGINS("plugins"),
        INGEST("ingest"),
        AGGREGATIONS("aggregations"),
        INDICES("indices");

        private static final Set<String> ALL_METRICS = Arrays.stream(values())
            .map(Metric::metricName)
            .collect(Collectors.toUnmodifiableSet());

        private final String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        public static Set<String> allMetrics() {
            return ALL_METRICS;
        }
    }
}

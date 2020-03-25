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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level information.
 */
public class NodesInfoRequest extends BaseNodesRequest<NodesInfoRequest> {

    private Set<String> requestedMetrics = Metric.allMetrics();

    /**
     * Create a new NodeInfoRequest from a {@link StreamInput} object.
     *
     * @param in A stream input object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public NodesInfoRequest(StreamInput in) throws IOException {
        super(in);
        requestedMetrics.clear();
        if (in.getVersion().before(Version.V_7_7_0)){
            // prior to version 8.x, a NodesInfoRequest was serialized as a list
            // of booleans in a fixed order
            optionallyAddMetric(in.readBoolean(), Metric.SETTINGS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.OS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.PROCESS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.JVM.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.THREAD_POOL.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.TRANSPORT.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.HTTP.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.PLUGINS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.INGEST.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.INDICES.metricName());
        } else {
            requestedMetrics.addAll(Arrays.asList(in.readStringArray()));
        }
    }

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    public NodesInfoRequest(String... nodesIds) {
        super(nodesIds);
        all();
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequest clear() {
        requestedMetrics.clear();
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequest all() {
        requestedMetrics.addAll(Metric.allMetrics());
        return this;
    }

    /**
     * Get the names of requested metrics
     */
    public Set<String> requestedMetrics() {
        return Set.copyOf(requestedMetrics);
    }

    /**
     * Add metric
     */
    public NodesInfoRequest addMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Add multiple metrics
     */
    public NodesInfoRequest addMetrics(String... metrics) {
        SortedSet<String> metricsSet = new TreeSet<>(Set.of(metrics));
        if (Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        requestedMetrics.addAll(metricsSet);
        return this;
    }

    /**
     * Remove metric
     */
    public NodesInfoRequest removeMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.remove(metric);
        return this;
    }

    /**
     * Helper method for adding and removing metrics. Used when deserializing
     * a NodesInfoRequest from an ordered list of booleans.
     *
     * @param addMetric Whether or not to include a metric.
     * @param metricName Name of the metric to include or remove.
     */
    private void optionallyAddMetric(boolean addMetric, String metricName) {
        if (addMetric) {
            requestedMetrics.add(metricName);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_7_0)){
            // prior to version 8.x, a NodesInfoRequest was serialized as a list
            // of booleans in a fixed order
            out.writeBoolean(Metric.SETTINGS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.OS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.PROCESS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.JVM.containedIn(requestedMetrics));
            out.writeBoolean(Metric.THREAD_POOL.containedIn(requestedMetrics));
            out.writeBoolean(Metric.TRANSPORT.containedIn(requestedMetrics));
            out.writeBoolean(Metric.HTTP.containedIn(requestedMetrics));
            out.writeBoolean(Metric.PLUGINS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.INGEST.containedIn(requestedMetrics));
            out.writeBoolean(Metric.INDICES.containedIn(requestedMetrics));
        } else {
            out.writeStringArray(requestedMetrics.toArray(String[]::new));
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes information endpoint. Eventually this list list will be
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
        PLUGINS("plugins"),
        INGEST("ingest"),
        INDICES("indices");

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

        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }
}

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
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level information.
 */
public class NodesInfoRequest extends BaseNodesRequest<NodesInfoRequest> {

    private Set<String> infoSections = new TreeSet<>(Metrics.allMetrics());

    public NodesInfoRequest(StreamInput in) throws IOException {
        super(in);
        infoSections.clear();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            infoSections.addAll(Arrays.asList(in.readStringArray()));
        } else {
            if (in.readBoolean()) {
                infoSections.add(Metrics.SETTINGS.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.OS.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.PROCESS.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.JVM.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.THREAD_POOL.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.TRANSPORT.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.HTTP.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.PLUGINS.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.INGEST.metricName());
            }
            if (in.readBoolean()) {
                infoSections.add(Metrics.INDICES.metricName());
            }
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
        infoSections.clear();
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequest all() {
        infoSections.addAll(Arrays.stream(Metrics.values()).map(Metrics::metricName).collect(Collectors.toSet()));
        return this;
    }

    /**
     * Should the node settings be returned.
     */
    public boolean settings() {
        return infoSections.contains(Metrics.SETTINGS.metricName());
    }

    /**
     * Should the node settings be returned.
     */
    public NodesInfoRequest settings(boolean settings) {
        setSection(settings, Metrics.SETTINGS.metricName());
        return this;
    }

    /**
     * Should the node OS be returned.
     */
    public boolean os() {
        return infoSections.contains(Metrics.OS.metricName());
    }

    /**
     * Should the node OS be returned.
     */
    public NodesInfoRequest os(boolean os) {
        setSection(os, Metrics.OS.metricName());
        return this;
    }

    /**
     * Should the node Process be returned.
     */
    public boolean process() {
        return infoSections.contains(Metrics.PROCESS.metricName());
    }

    /**
     * Should the node Process be returned.
     */
    public NodesInfoRequest process(boolean process) {
        setSection(process, Metrics.PROCESS.metricName());
        return this;
    }

    /**
     * Should the node JVM be returned.
     */
    public boolean jvm() {
        return infoSections.contains(Metrics.JVM.metricName());
    }

    /**
     * Should the node JVM be returned.
     */
    public NodesInfoRequest jvm(boolean jvm) {
        setSection(jvm, Metrics.JVM.metricName());
        return this;
    }

    /**
     * Should the node Thread Pool info be returned.
     */
    public boolean threadPool() {
        return infoSections.contains(Metrics.THREAD_POOL.metricName());
    }

    /**
     * Should the node Thread Pool info be returned.
     */
    public NodesInfoRequest threadPool(boolean threadPool) {
        setSection(threadPool, Metrics.THREAD_POOL.metricName());
        return this;
    }

    /**
     * Should the node Transport be returned.
     */
    public boolean transport() {
        return infoSections.contains(Metrics.TRANSPORT.metricName());
    }

    /**
     * Should the node Transport be returned.
     */
    public NodesInfoRequest transport(boolean transport) {
        setSection(transport, Metrics.TRANSPORT.metricName());
        return this;
    }

    /**
     * Should the node HTTP be returned.
     */
    public boolean http() {
        return infoSections.contains(Metrics.HTTP.metricName());
    }

    /**
     * Should the node HTTP be returned.
     */
    public NodesInfoRequest http(boolean http) {
        setSection(http, Metrics.HTTP.metricName());
        return this;
    }

    /**
     * Should information about plugins be returned
     * @param plugins true if you want info
     * @return The request
     */
    public NodesInfoRequest plugins(boolean plugins) {
        setSection(plugins, Metrics.PLUGINS.metricName());
        return this;
    }

    /**
     * @return true if information about plugins is requested
     */
    public boolean plugins() {
        return infoSections.contains(Metrics.PLUGINS.metricName());
    }

    /**
     * Should information about ingest be returned
     * @param ingest true if you want info
     */
    public NodesInfoRequest ingest(boolean ingest) {
        setSection(ingest, Metrics.INGEST.metricName());
        return this;
    }

    /**
     * @return true if information about ingest is requested
     */
    public boolean ingest() {
        return infoSections.contains(Metrics.INGEST.metricName());
    }

    /**
     * Should information about indices (currently just indexing buffers) be returned
     * @param indices true if you want info
     */
    public NodesInfoRequest indices(boolean indices) {
        setSection(indices, Metrics.INDICES.metricName());
        return this;
    }

    /**
     * @return true if information about indices (currently just indexing buffers)
     */
    public boolean indices() {
        return infoSections.contains(Metrics.INDICES.metricName());
    }

    private void setSection(boolean includeSection, String sectionName) {
        if (includeSection) {
            infoSections.add(sectionName);
        } else {
            infoSections.remove(sectionName);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeStringArray(infoSections.toArray(String[]::new));
        } else {
            out.writeBoolean(infoSections.contains(Metrics.SETTINGS.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.OS.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.PROCESS.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.JVM.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.THREAD_POOL.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.TRANSPORT.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.HTTP.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.PLUGINS.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.INGEST.metricName()));
            out.writeBoolean(infoSections.contains(Metrics.INDICES.metricName()));
        }
    }

    enum Metrics {
        SETTINGS("settings"),
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("threadPool"),
        TRANSPORT("transport"),
        HTTP("http"),
        PLUGINS("plugins"),
        INGEST("ingest"),
        INDICES("indices");

        private String metricName;

        Metrics(String name) {
            this.metricName = name;
        }

        String metricName() {
            return this.metricName;
        }

        static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metrics::metricName).collect(Collectors.toSet());
        }
    }
}

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

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get node (cluster) level stats.
 */
public class NodesStatsRequest extends BaseNodesRequest<NodesStatsRequest> {

    private CommonStatsFlags indices = new CommonStatsFlags();
    private boolean os;
    private boolean process;
    private boolean jvm;
    private boolean threadPool;
    private boolean fs;
    private boolean transport;
    private boolean http;
    private boolean breaker;
    private boolean script;
    private boolean discovery;
    private boolean ingest;
    private boolean adaptiveSelection;

    public NodesStatsRequest() {
        super((String[]) null);
    }

    public NodesStatsRequest(StreamInput in) throws IOException {
        super(in);
        indices = new CommonStatsFlags(in);
        os = in.readBoolean();
        process = in.readBoolean();
        jvm = in.readBoolean();
        threadPool = in.readBoolean();
        fs = in.readBoolean();
        transport = in.readBoolean();
        http = in.readBoolean();
        breaker = in.readBoolean();
        script = in.readBoolean();
        discovery = in.readBoolean();
        ingest = in.readBoolean();
        adaptiveSelection = in.readBoolean();
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public NodesStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsRequest all() {
        this.indices.all();
        this.os = true;
        this.process = true;
        this.jvm = true;
        this.threadPool = true;
        this.fs = true;
        this.transport = true;
        this.http = true;
        this.breaker = true;
        this.script = true;
        this.discovery = true;
        this.ingest = true;
        this.adaptiveSelection = true;
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesStatsRequest clear() {
        this.indices.clear();
        this.os = false;
        this.process = false;
        this.jvm = false;
        this.threadPool = false;
        this.fs = false;
        this.transport = false;
        this.http = false;
        this.breaker = false;
        this.script = false;
        this.discovery = false;
        this.ingest = false;
        this.adaptiveSelection = false;
        return this;
    }

    public CommonStatsFlags indices() {
        return indices;
    }

    public NodesStatsRequest indices(CommonStatsFlags indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Should indices stats be returned.
     */
    public NodesStatsRequest indices(boolean indices) {
        if (indices) {
            this.indices.all();
        } else {
            this.indices.clear();
        }
        return this;
    }

    /**
     * Should the node OS be returned.
     */
    public boolean os() {
        return this.os;
    }

    /**
     * Should the node OS be returned.
     */
    public NodesStatsRequest os(boolean os) {
        this.os = os;
        return this;
    }

    /**
     * Should the node Process be returned.
     */
    public boolean process() {
        return this.process;
    }

    /**
     * Should the node Process be returned.
     */
    public NodesStatsRequest process(boolean process) {
        this.process = process;
        return this;
    }

    /**
     * Should the node JVM be returned.
     */
    public boolean jvm() {
        return this.jvm;
    }

    /**
     * Should the node JVM be returned.
     */
    public NodesStatsRequest jvm(boolean jvm) {
        this.jvm = jvm;
        return this;
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public boolean threadPool() {
        return this.threadPool;
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public NodesStatsRequest threadPool(boolean threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public boolean fs() {
        return this.fs;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequest fs(boolean fs) {
        this.fs = fs;
        return this;
    }

    /**
     * Should the node Transport be returned.
     */
    public boolean transport() {
        return this.transport;
    }

    /**
     * Should the node Transport be returned.
     */
    public NodesStatsRequest transport(boolean transport) {
        this.transport = transport;
        return this;
    }

    /**
     * Should the node HTTP be returned.
     */
    public boolean http() {
        return this.http;
    }

    /**
     * Should the node HTTP be returned.
     */
    public NodesStatsRequest http(boolean http) {
        this.http = http;
        return this;
    }

    public boolean breaker() {
        return this.breaker;
    }

    /**
     * Should the node's circuit breaker stats be returned.
     */
    public NodesStatsRequest breaker(boolean breaker) {
        this.breaker = breaker;
        return this;
    }

    public boolean script() {
        return script;
    }

    public NodesStatsRequest script(boolean script) {
        this.script = script;
        return this;
    }


    public boolean discovery() {
        return this.discovery;
    }

    /**
     * Should the node's discovery stats be returned.
     */
    public NodesStatsRequest discovery(boolean discovery) {
        this.discovery = discovery;
        return this;
    }

    public boolean ingest() {
        return ingest;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequest ingest(boolean ingest) {
        this.ingest = ingest;
        return this;
    }

    public boolean adaptiveSelection() {
        return adaptiveSelection;
    }

    /**
     * Should adaptiveSelection statistics be returned.
     */
    public NodesStatsRequest adaptiveSelection(boolean adaptiveSelection) {
        this.adaptiveSelection = adaptiveSelection;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        indices.writeTo(out);
        out.writeBoolean(os);
        out.writeBoolean(process);
        out.writeBoolean(jvm);
        out.writeBoolean(threadPool);
        out.writeBoolean(fs);
        out.writeBoolean(transport);
        out.writeBoolean(http);
        out.writeBoolean(breaker);
        out.writeBoolean(script);
        out.writeBoolean(discovery);
        out.writeBoolean(ingest);
        out.writeBoolean(adaptiveSelection);
    }
}

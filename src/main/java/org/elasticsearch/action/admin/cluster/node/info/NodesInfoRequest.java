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

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get node (cluster) level information.
 */
public class NodesInfoRequest extends NodesOperationRequest<NodesInfoRequest> {

    private boolean settings = true;
    private boolean os = true;
    private boolean process = true;
    private boolean jvm = true;
    private boolean threadPool = true;
    private boolean network = true;
    private boolean transport = true;
    private boolean http = true;
    private boolean plugins = true;

    public NodesInfoRequest() {
    }

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    public NodesInfoRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequest clear() {
        settings = false;
        os = false;
        process = false;
        jvm = false;
        threadPool = false;
        network = false;
        transport = false;
        http = false;
        plugins = false;
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequest all() {
        settings = true;
        os = true;
        process = true;
        jvm = true;
        threadPool = true;
        network = true;
        transport = true;
        http = true;
        plugins = true;
        return this;
    }

    /**
     * Should the node settings be returned.
     */
    public boolean settings() {
        return this.settings;
    }

    /**
     * Should the node settings be returned.
     */
    public NodesInfoRequest settings(boolean settings) {
        this.settings = settings;
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
    public NodesInfoRequest os(boolean os) {
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
    public NodesInfoRequest process(boolean process) {
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
    public NodesInfoRequest jvm(boolean jvm) {
        this.jvm = jvm;
        return this;
    }

    /**
     * Should the node Thread Pool info be returned.
     */
    public boolean threadPool() {
        return this.threadPool;
    }

    /**
     * Should the node Thread Pool info be returned.
     */
    public NodesInfoRequest threadPool(boolean threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    /**
     * Should the node Network be returned.
     */
    public boolean network() {
        return this.network;
    }

    /**
     * Should the node Network be returned.
     */
    public NodesInfoRequest network(boolean network) {
        this.network = network;
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
    public NodesInfoRequest transport(boolean transport) {
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
    public NodesInfoRequest http(boolean http) {
        this.http = http;
        return this;
    }

    /**
     * Should information about plugins be returned
     * @param plugins true if you want info
     * @return The request
     */
    public NodesInfoRequest plugins(boolean plugins) {
        this.plugins = plugins;
        return this;
    }

    /**
     * @return true if information about plugins is requested
     */
    public boolean plugins() {
        return plugins;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        settings = in.readBoolean();
        os = in.readBoolean();
        process = in.readBoolean();
        jvm = in.readBoolean();
        threadPool = in.readBoolean();
        network = in.readBoolean();
        transport = in.readBoolean();
        http = in.readBoolean();
        plugins = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(settings);
        out.writeBoolean(os);
        out.writeBoolean(process);
        out.writeBoolean(jvm);
        out.writeBoolean(threadPool);
        out.writeBoolean(network);
        out.writeBoolean(transport);
        out.writeBoolean(http);
        out.writeBoolean(plugins);
    }
}

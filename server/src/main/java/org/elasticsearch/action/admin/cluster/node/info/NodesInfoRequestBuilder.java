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

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class NodesInfoRequestBuilder extends NodesOperationRequestBuilder<NodesInfoRequest, NodesInfoResponse, NodesInfoRequestBuilder> {

    public NodesInfoRequestBuilder(ElasticsearchClient client, NodesInfoAction action) {
        super(client, action, new NodesInfoRequest());
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Should the node settings be returned.
     */
    public NodesInfoRequestBuilder setSettings(boolean settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Should the node OS info be returned.
     */
    public NodesInfoRequestBuilder setOs(boolean os) {
        request.os(os);
        return this;
    }

    /**
     * Should the node OS process be returned.
     */
    public NodesInfoRequestBuilder setProcess(boolean process) {
        request.process(process);
        return this;
    }

    /**
     * Should the node JVM info be returned.
     */
    public NodesInfoRequestBuilder setJvm(boolean jvm) {
        request.jvm(jvm);
        return this;
    }

    /**
     * Should the node thread pool info be returned.
     */
    public NodesInfoRequestBuilder setThreadPool(boolean threadPool) {
        request.threadPool(threadPool);
        return this;
    }

    /**
     * Should the node Transport info be returned.
     */
    public NodesInfoRequestBuilder setTransport(boolean transport) {
        request.transport(transport);
        return this;
    }

    /**
     * Should the node HTTP info be returned.
     */
    public NodesInfoRequestBuilder setHttp(boolean http) {
        request.http(http);
        return this;
    }

    /**
     * Should the node plugins info be returned.
     */
    public NodesInfoRequestBuilder setPlugins(boolean plugins) {
        request().plugins(plugins);
        return this;
    }

    /**
     * Should the node ingest info be returned.
     */
    public NodesInfoRequestBuilder setIngest(boolean ingest) {
        request().ingest(ingest);
        return this;
    }

    /**
     * Should the node indices info be returned.
     */
    public NodesInfoRequestBuilder setIndices(boolean indices) {
        request().indices(indices);
        return this;
    }
}

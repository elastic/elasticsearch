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
     * Add a single metric to the request.
     *
     * @param metric Name of metric as a string.
     * @return This, for request chaining.
     */
    public NodesInfoRequestBuilder addMetric(String metric) {
        request.addMetric(metric);
        return this;
    }

    /**
     * Add an array of metrics to the request.
     *
     * @param metrics Metric names as strings.
     * @return This, for request chaining.
     */
    public NodesInfoRequestBuilder addMetrics(String... metrics) {
        request.addMetrics(metrics);
        return this;
    }
}

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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class ClusterHealthRequestBuilder extends MasterNodeReadOperationRequestBuilder<ClusterHealthRequest, ClusterHealthResponse, ClusterHealthRequestBuilder> {

    public ClusterHealthRequestBuilder(ElasticsearchClient client, ClusterHealthAction action) {
        super(client, action, new ClusterHealthRequest());
    }

    public ClusterHealthRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public ClusterHealthRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    public ClusterHealthRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForStatus(ClusterHealthStatus waitForStatus) {
        request.waitForStatus(waitForStatus);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForGreenStatus() {
        request.waitForGreenStatus();
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForYellowStatus() {
        request.waitForYellowStatus();
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForRelocatingShards(int waitForRelocatingShards) {
        request.waitForRelocatingShards(waitForRelocatingShards);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForActiveShards(int waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, ">12" and "<12" for range.
     */
    public ClusterHealthRequestBuilder setWaitForNodes(String waitForNodes) {
        request.waitForNodes(waitForNodes);
        return this;
    }

    public ClusterHealthRequestBuilder setWaitForEvents(Priority waitForEvents) {
        request.waitForEvents(waitForEvents);
        return this;
    }
}

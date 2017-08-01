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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Explanation response for a shard in the cluster
 */
public class ClusterAllocationExplainResponse extends ActionResponse {

    private ClusterAllocationExplanation cae;

    public ClusterAllocationExplainResponse() {
    }

    public ClusterAllocationExplainResponse(ClusterAllocationExplanation cae) {
        this.cae = cae;
    }

    /**
     * Return the explanation for shard allocation in the cluster
     */
    public ClusterAllocationExplanation getExplanation() {
        return this.cae;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.cae = new ClusterAllocationExplanation(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        cae.writeTo(out);
    }
}

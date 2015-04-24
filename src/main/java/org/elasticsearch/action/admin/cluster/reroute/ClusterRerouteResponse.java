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

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response returned after a cluster reroute request
 */
public class ClusterRerouteResponse extends AcknowledgedResponse {

    private ClusterState state;
    private RoutingExplanations explanations;

    ClusterRerouteResponse() {

    }

    ClusterRerouteResponse(boolean acknowledged, ClusterState state, RoutingExplanations explanations) {
        super(acknowledged);
        this.state = state;
        this.explanations = explanations;
    }

    /**
     * Returns the cluster state resulted from the cluster reroute request execution
     */
    public ClusterState getState() {
        return this.state;
    }

    public RoutingExplanations getExplanations() {
        return this.explanations;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        state = ClusterState.Builder.readFrom(in, null);
        readAcknowledged(in);
        explanations = RoutingExplanations.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ClusterState.Builder.writeTo(state, out);
        writeAcknowledged(out);
        RoutingExplanations.writeTo(explanations, out);
    }
}

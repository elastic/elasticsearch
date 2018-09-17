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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A master node sends this request to its peers to inform them that it could commit the
 * cluster state with the given term and version. Peers that have accepted the given cluster
 * state will then consider it as committed and proceed to apply the state locally.
 */
public class ApplyCommitRequest extends TermVersionRequest {

    public ApplyCommitRequest(DiscoveryNode sourceNode, long term, long version) {
        super(sourceNode, term, version);
    }

    public ApplyCommitRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public String toString() {
        return "ApplyCommitRequest{" +
            "term=" + term +
            ", version=" + version +
            ", sourceNode=" + sourceNode +
            '}';
    }
}

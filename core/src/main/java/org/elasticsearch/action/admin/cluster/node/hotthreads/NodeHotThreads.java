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

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class NodeHotThreads extends BaseNodeResponse {

    private String hotThreads;

    NodeHotThreads() {
    }

    public NodeHotThreads(DiscoveryNode node, String hotThreads) {
        super(node);
        this.hotThreads = hotThreads;
    }

    public String getHotThreads() {
        return this.hotThreads;
    }

    public static NodeHotThreads readNodeHotThreads(StreamInput in) throws IOException {
        NodeHotThreads node = new NodeHotThreads();
        node.readFrom(in);
        return node;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        hotThreads = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(hotThreads);
    }
}

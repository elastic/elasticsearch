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

package org.elasticsearch.river.routing;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.river.RiverName;

import java.io.IOException;

/**
 *
 */
public class RiverRouting implements Streamable {

    private RiverName riverName;

    private DiscoveryNode node;

    private RiverRouting() {
    }

    RiverRouting(RiverName riverName, DiscoveryNode node) {
        this.riverName = riverName;
        this.node = node;
    }

    public RiverName riverName() {
        return riverName;
    }

    /**
     * The node the river is allocated to, <tt>null</tt> if its not allocated.
     */
    public DiscoveryNode node() {
        return node;
    }

    void node(DiscoveryNode node) {
        this.node = node;
    }

    public static RiverRouting readRiverRouting(StreamInput in) throws IOException {
        RiverRouting routing = new RiverRouting();
        routing.readFrom(in);
        return routing;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        riverName = new RiverName(in.readString(), in.readString());
        if (in.readBoolean()) {
            node = DiscoveryNode.readNode(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(riverName.type());
        out.writeString(riverName.name());
        if (node == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            node.writeTo(out);
        }
    }
}

/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indexer.routing;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.indexer.IndexerName;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerRouting implements Streamable {

    private IndexerName indexerName;

    private DiscoveryNode node;

    private IndexerRouting() {
    }

    IndexerRouting(IndexerName indexerName, DiscoveryNode node) {
        this.indexerName = indexerName;
        this.node = node;
    }

    public IndexerName indexerName() {
        return indexerName;
    }

    /**
     * The node the indexer is allocated to, <tt>null</tt> if its not allocated.
     */
    public DiscoveryNode node() {
        return node;
    }

    void node(DiscoveryNode node) {
        this.node = node;
    }

    public static IndexerRouting readIndexerRouting(StreamInput in) throws IOException {
        IndexerRouting routing = new IndexerRouting();
        routing.readFrom(in);
        return routing;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        indexerName = new IndexerName(in.readUTF(), in.readUTF());
        if (in.readBoolean()) {
            node = DiscoveryNode.readNode(in);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(indexerName.type());
        out.writeUTF(indexerName.name());
        if (node == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            node.writeTo(out);
        }
    }
}

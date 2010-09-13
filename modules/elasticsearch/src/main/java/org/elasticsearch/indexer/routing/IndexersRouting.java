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
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indexer.IndexerName;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author kimchy (shay.banon)
 */
public class IndexersRouting implements Iterable<IndexerRouting> {

    private final ImmutableMap<IndexerName, IndexerRouting> indexers;

    private IndexersRouting(ImmutableMap<IndexerName, IndexerRouting> indexers) {
        this.indexers = indexers;
    }

    @Override public Iterator<IndexerRouting> iterator() {
        return indexers.values().iterator();
    }

    public static class Builder {

        private MapBuilder<IndexerName, IndexerRouting> indexers = MapBuilder.newMapBuilder();

        public Builder routing(IndexersRouting routing) {
            indexers.putAll(routing.indexers);
            return this;
        }

        public Builder put(IndexerRouting routing) {
            indexers.put(routing.indexerName(), routing);
            return this;
        }

        public Builder remove(IndexerRouting routing) {
            indexers.remove(routing.indexerName());
            return this;
        }

        public IndexersRouting build() {
            return new IndexersRouting(indexers.immutableMap());
        }

        public static IndexersRouting readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(new IndexerRouting(new IndexerName(in.readUTF(), in.readUTF()), DiscoveryNode.readNode(in)));
            }
            return builder.build();
        }

        public static void writeTo(IndexersRouting routing, StreamOutput out) throws IOException {
            out.writeVInt(routing.indexers.size());
            for (IndexerRouting indexerRouting : routing) {
                out.writeUTF(indexerRouting.indexerName().type());
                out.writeUTF(indexerRouting.indexerName().name());

                indexerRouting.node().writeTo(out);
            }
        }
    }
}

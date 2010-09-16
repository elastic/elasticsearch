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

package org.elasticsearch.indexer.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indexer.routing.IndexersRouting;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerClusterState {

    private final long version;

    private final IndexersRouting routing;

    public IndexerClusterState(long version, IndexerClusterState state) {
        this.version = version;
        this.routing = state.routing();
    }

    IndexerClusterState(long version, IndexersRouting routing) {
        this.version = version;
        this.routing = routing;
    }

    public long version() {
        return this.version;
    }

    public IndexersRouting routing() {
        return routing;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long version = 0;

        private IndexersRouting routing = IndexersRouting.EMPTY;

        public Builder state(IndexerClusterState state) {
            this.version = state.version();
            this.routing = state.routing();
            return this;
        }

        public Builder routing(IndexersRouting.Builder builder) {
            return routing(builder.build());
        }

        public Builder routing(IndexersRouting routing) {
            this.routing = routing;
            return this;
        }

        public IndexerClusterState build() {
            return new IndexerClusterState(version, routing);
        }

        public static IndexerClusterState readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            builder.version = in.readVLong();
            builder.routing = IndexersRouting.Builder.readFrom(in);
            return builder.build();
        }

        public static void writeTo(IndexerClusterState clusterState, StreamOutput out) throws IOException {
            out.writeVLong(clusterState.version);
            IndexersRouting.Builder.writeTo(clusterState.routing, out);
        }
    }
}

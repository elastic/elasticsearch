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

package org.elasticsearch.river.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.river.routing.RiversRouting;

import java.io.IOException;

/**
 *
 */
public class RiverClusterState {

    private final long version;

    private final RiversRouting routing;

    public RiverClusterState(long version, RiverClusterState state) {
        this.version = version;
        this.routing = state.routing();
    }

    RiverClusterState(long version, RiversRouting routing) {
        this.version = version;
        this.routing = routing;
    }

    public long version() {
        return this.version;
    }

    public RiversRouting routing() {
        return routing;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long version = 0;

        private RiversRouting routing = RiversRouting.EMPTY;

        public Builder state(RiverClusterState state) {
            this.version = state.version();
            this.routing = state.routing();
            return this;
        }

        public Builder routing(RiversRouting.Builder builder) {
            return routing(builder.build());
        }

        public Builder routing(RiversRouting routing) {
            this.routing = routing;
            return this;
        }

        public RiverClusterState build() {
            return new RiverClusterState(version, routing);
        }

        public static RiverClusterState readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            builder.version = in.readVLong();
            builder.routing = RiversRouting.Builder.readFrom(in);
            return builder.build();
        }

        public static void writeTo(RiverClusterState clusterState, StreamOutput out) throws IOException {
            out.writeVLong(clusterState.version);
            RiversRouting.Builder.writeTo(clusterState.routing, out);
        }
    }
}

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

package org.elasticsearch.discovery.zen.ping;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;

import java.io.IOException;

import static org.elasticsearch.cluster.ClusterName.readClusterName;
import static org.elasticsearch.cluster.node.DiscoveryNode.readNode;

/**
 *
 */
public interface ZenPing extends LifecycleComponent<ZenPing> {

    void setNodesProvider(DiscoveryNodesProvider nodesProvider);

    void ping(PingListener listener, TimeValue timeout) throws ElasticsearchException;

    public interface PingListener {

        void onPing(PingResponse[] pings);
    }

    public static class PingResponse implements Streamable {
        
        public static final PingResponse[] EMPTY = new PingResponse[0];

        private ClusterName clusterName;

        private DiscoveryNode target;

        private DiscoveryNode master;

        private PingResponse() {
        }

        public PingResponse(DiscoveryNode target, DiscoveryNode master, ClusterName clusterName) {
            this.target = target;
            this.master = master;
            this.clusterName = clusterName;
        }

        public ClusterName clusterName() {
            return this.clusterName;
        }

        public DiscoveryNode target() {
            return target;
        }

        public DiscoveryNode master() {
            return master;
        }

        public static PingResponse readPingResponse(StreamInput in) throws IOException {
            PingResponse response = new PingResponse();
            response.readFrom(in);
            return response;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            clusterName = readClusterName(in);
            target = readNode(in);
            if (in.readBoolean()) {
                master = readNode(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            target.writeTo(out);
            if (master == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                master.writeTo(out);
            }
        }

        @Override
        public String toString() {
            return "ping_response{target [" + target + "], master [" + master + "], cluster_name[" + clusterName.value() + "]}";
        }
    }
}

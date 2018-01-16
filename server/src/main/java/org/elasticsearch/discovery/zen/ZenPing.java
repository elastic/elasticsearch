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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public interface ZenPing extends Releasable {

    void start();

    void ping(Consumer<PingCollection> resultsConsumer, TimeValue timeout);

    class PingResponse implements Streamable {

        public static final PingResponse[] EMPTY = new PingResponse[0];

        private static final AtomicLong idGenerator = new AtomicLong();

        // an always increasing unique identifier for this ping response.
        // lower values means older pings.
        private long id;

        private ClusterName clusterName;

        private DiscoveryNode node;

        private DiscoveryNode master;

        private long clusterStateVersion;

        private PingResponse() {
        }

        /**
         * @param node                the node which this ping describes
         * @param master              the current master of the node
         * @param clusterName         the cluster name of the node
         * @param clusterStateVersion the current cluster state version of that node
         *                            ({@link ElectMasterService.MasterCandidate#UNRECOVERED_CLUSTER_VERSION} for not recovered)
         */
        PingResponse(DiscoveryNode node, DiscoveryNode master, ClusterName clusterName, long clusterStateVersion) {
            this.id = idGenerator.incrementAndGet();
            this.node = node;
            this.master = master;
            this.clusterName = clusterName;
            this.clusterStateVersion = clusterStateVersion;
        }

        public PingResponse(DiscoveryNode node, DiscoveryNode master, ClusterState state) {
            this(node, master, state.getClusterName(),
                state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) ?
                    ElectMasterService.MasterCandidate.UNRECOVERED_CLUSTER_VERSION : state.version());
        }

            /**
             * an always increasing unique identifier for this ping response.
             * lower values means older pings.
             */
        public long id() {
            return this.id;
        }

        public ClusterName clusterName() {
            return this.clusterName;
        }

        /** the node which this ping describes */
        public DiscoveryNode node() {
            return node;
        }

        /** the current master of the node */
        public DiscoveryNode master() {
            return master;
        }

        /**
         * the current cluster state version of that node ({@link ElectMasterService.MasterCandidate#UNRECOVERED_CLUSTER_VERSION}
         * for not recovered) */
        public long getClusterStateVersion() {
            return clusterStateVersion;
        }

        public static PingResponse readPingResponse(StreamInput in) throws IOException {
            PingResponse response = new PingResponse();
            response.readFrom(in);
            return response;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            clusterName = new ClusterName(in);
            node = new DiscoveryNode(in);
            if (in.readBoolean()) {
                master = new DiscoveryNode(in);
            }
            this.clusterStateVersion = in.readLong();
            this.id = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            node.writeTo(out);
            if (master == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                master.writeTo(out);
            }
            out.writeLong(clusterStateVersion);
            out.writeLong(id);
        }

        @Override
        public String toString() {
            return "ping_response{node [" + node + "], id[" + id + "], master [" + master + "]," +
                   "cluster_state_version [" + clusterStateVersion + "], cluster_name[" + clusterName.value() + "]}";
        }
    }


    /**
     * a utility collection of pings where only the most recent ping is stored per node
     */
    class PingCollection {

        Map<DiscoveryNode, PingResponse> pings;

        public PingCollection() {
            pings = new HashMap<>();
        }

        /**
         * adds a ping if newer than previous pings from the same node
         *
         * @return true if added, false o.w.
         */
        public synchronized boolean addPing(PingResponse ping) {
            PingResponse existingResponse = pings.get(ping.node());
            // in case both existing and new ping have the same id (probably because they come
            // from nodes from version <1.4.0) we prefer to use the last added one.
            if (existingResponse == null || existingResponse.id() <= ping.id()) {
                pings.put(ping.node(), ping);
                return true;
            }
            return false;
        }

        /** serialize current pings to a list. It is guaranteed that the list contains one ping response per node */
        public synchronized List<PingResponse> toList() {
            return new ArrayList<>(pings.values());
        }

        /** the number of nodes for which there are known pings */
        public synchronized int size() {
            return pings.size();
        }
    }
}

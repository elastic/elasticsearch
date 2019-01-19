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

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 */
public final class ResponseCollectorService implements ClusterStateListener {

    private static final double ALPHA = 0.3;

    private final ConcurrentMap<String, NodeStatistics> nodeIdToStats = ConcurrentCollections.newConcurrentMap();

    public ResponseCollectorService(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) {
        nodeIdToStats.remove(nodeId);
    }

    public void addNodeStatistics(String nodeId, int queueSize, long responseTimeNanos, long avgServiceTimeNanos) {
        nodeIdToStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                ExponentiallyWeightedMovingAverage queueEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, queueSize);
                ExponentiallyWeightedMovingAverage responseEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, responseTimeNanos);
                return new NodeStatistics(nodeId, queueEWMA, responseEWMA, avgServiceTimeNanos);
            } else {
                ns.queueSize.addValue((double) queueSize);
                ns.responseTime.addValue((double) responseTimeNanos);
                ns.serviceTime = avgServiceTimeNanos;
                return ns;
            }
        });
    }

    public Map<String, ComputedNodeStats> getAllNodeStatistics() {
        final int clientNum = nodeIdToStats.size();
        // Transform the mutable object internally used for accounting into the computed version
        Map<String, ComputedNodeStats> nodeStats = new HashMap<>(nodeIdToStats.size());
        nodeIdToStats.forEach((k, v) -> {
            nodeStats.put(k, new ComputedNodeStats(clientNum, v));
        });
        return nodeStats;
    }

    public AdaptiveSelectionStats getAdaptiveStats(Map<String, Long> clientSearchConnections) {
        return new AdaptiveSelectionStats(clientSearchConnections, getAllNodeStatistics());
    }

    /**
     * Optionally return a {@code NodeStatistics} for the given nodeid, if
     * response information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<ComputedNodeStats> getNodeStatistics(final String nodeId) {
        final int clientNum = nodeIdToStats.size();
        return Optional.ofNullable(nodeIdToStats.get(nodeId)).map(ns -> new ComputedNodeStats(clientNum, ns));
    }

    /**
     * Struct-like class encapsulating a point-in-time snapshot of a particular
     * node's statistics. This includes the EWMA of queue size, response time,
     * and service time.
     */
    public static class ComputedNodeStats implements Writeable {
        // We store timestamps with nanosecond precision, however, the
        // formula specifies milliseconds, therefore we need to convert
        // the values so the times don't unduely weight the formula
        private final double FACTOR = 1000000.0;
        private final int clientNum;

        private double cachedRank = 0;

        public final String nodeId;
        public final int queueSize;
        public final double responseTime;
        public final double serviceTime;

        public ComputedNodeStats(String nodeId, int clientNum, int queueSize, double responseTime, double serviceTime) {
            this.nodeId = nodeId;
            this.clientNum = clientNum;
            this.queueSize = queueSize;
            this.responseTime = responseTime;
            this.serviceTime = serviceTime;
        }

        ComputedNodeStats(int clientNum, NodeStatistics nodeStats) {
            this(nodeStats.nodeId, clientNum,
                    (int) nodeStats.queueSize.getAverage(), nodeStats.responseTime.getAverage(), nodeStats.serviceTime);
        }

        ComputedNodeStats(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.clientNum = in.readInt();
            this.queueSize = in.readInt();
            this.responseTime = in.readDouble();
            this.serviceTime = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
            out.writeInt(this.clientNum);
            out.writeInt(this.queueSize);
            out.writeDouble(this.responseTime);
            out.writeDouble(this.serviceTime);
        }

        /**
         * Rank this copy of the data, according to the adaptive replica selection formula from the C3 paper
         * https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf
         */
        private double innerRank(long outstandingRequests) {
            // the concurrency compensation is defined as the number of
            // outstanding requests from the client to the node times the number
            // of clients in the system
            double concurrencyCompensation = outstandingRequests * clientNum;

            // Cubic queue adjustment factor. The paper chose 3 though we could
            // potentially make this configurable if desired.
            int queueAdjustmentFactor = 3;

            // EWMA of queue size
            double qBar = queueSize;
            double qHatS = 1 + concurrencyCompensation + qBar;

            // EWMA of response time
            double rS = responseTime / FACTOR;
            // EWMA of service time
            double muBarS = serviceTime / FACTOR;

            // The final formula
            double rank = rS - (1.0 / muBarS) + (Math.pow(qHatS, queueAdjustmentFactor) / muBarS);
            return rank;
        }

        public double rank(long outstandingRequests) {
            if (cachedRank == 0) {
                cachedRank = innerRank(outstandingRequests);
            }
            return cachedRank;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ComputedNodeStats[");
            sb.append(nodeId).append("](");
            sb.append("nodes: ").append(clientNum);
            sb.append(", queue: ").append(queueSize);
            sb.append(", response time: ").append(String.format(Locale.ROOT, "%.1f", responseTime));
            sb.append(", service time: ").append(String.format(Locale.ROOT, "%.1f", serviceTime));
            sb.append(", rank: ").append(String.format(Locale.ROOT, "%.1f", rank(1)));
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Class encapsulating a node's exponentially weighted queue size, response
     * time, and service time, however, this class is private and intended only
     * to be used for the internal accounting of {@code ResponseCollectorService}.
     */
    private static class NodeStatistics {
        final String nodeId;
        final ExponentiallyWeightedMovingAverage queueSize;
        final ExponentiallyWeightedMovingAverage responseTime;
        double serviceTime;

        NodeStatistics(String nodeId,
                       ExponentiallyWeightedMovingAverage queueSizeEWMA,
                       ExponentiallyWeightedMovingAverage responseTimeEWMA,
                       double serviceTimeEWMA) {
            this.nodeId = nodeId;
            this.queueSize = queueSizeEWMA;
            this.responseTime = responseTimeEWMA;
            this.serviceTime = serviceTimeEWMA;
        }
    }
}

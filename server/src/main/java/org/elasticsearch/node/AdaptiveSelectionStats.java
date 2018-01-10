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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class representing statistics about adaptive replica selection. This includes
 * EWMA of queue size, service time, and response time, as well as outgoing
 * searches to each node and the "rank" based on the ARS formula.
 */
public class AdaptiveSelectionStats implements Writeable, ToXContentFragment {

    private final Map<String, Long> clientOutgoingConnections;
    private final Map<String, ResponseCollectorService.ComputedNodeStats> nodeComputedStats;

    public AdaptiveSelectionStats(Map<String, Long> clientConnections,
                                  Map<String, ResponseCollectorService.ComputedNodeStats> nodeComputedStats) {
        this.clientOutgoingConnections = clientConnections;
        this.nodeComputedStats = nodeComputedStats;
    }

    public AdaptiveSelectionStats(StreamInput in) throws IOException {
        this.clientOutgoingConnections = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.nodeComputedStats = in.readMap(StreamInput::readString, ResponseCollectorService.ComputedNodeStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.clientOutgoingConnections, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeMap(this.nodeComputedStats, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("adaptive_selection");
        Set<String> allNodeIds = Sets.union(clientOutgoingConnections.keySet(), nodeComputedStats.keySet());
        for (String nodeId : allNodeIds) {
            builder.startObject(nodeId);
            ResponseCollectorService.ComputedNodeStats stats = nodeComputedStats.get(nodeId);
            if (stats != null) {
                long outgoingSearches = clientOutgoingConnections.getOrDefault(nodeId, 0L);
                builder.field("outgoing_searches", outgoingSearches);
                builder.field("avg_queue_size", stats.queueSize);
                builder.timeValueField("avg_service_time_ns", "avg_service_time", (long) stats.serviceTime, TimeUnit.NANOSECONDS);
                builder.timeValueField("avg_response_time_ns", "avg_response_time", (long) stats.responseTime, TimeUnit.NANOSECONDS);
                builder.field("rank", String.format(Locale.ROOT, "%.1f", stats.rank(outgoingSearches)));
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns a map of node id to the outgoing search requests to that node
     */
    public Map<String, Long> getOutgoingConnections() {
        return clientOutgoingConnections;
    }

    /**
     * Returns a map of node id to the computed stats
     */
    public Map<String, ResponseCollectorService.ComputedNodeStats> getComputedStats() {
        return nodeComputedStats;
    }

    /**
     * Returns a map of node id to the ranking of the nodes based on the adaptive replica formula
     */
    public Map<String, Double> getRanks() {
        return nodeComputedStats.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> e.getValue().rank(clientOutgoingConnections.getOrDefault(e.getKey(), 0L))));
    }
}

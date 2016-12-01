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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class that holds utility methods for serializing and working with allocation decisions.
 */
public class DecisionUtils {

    private static final Comparator<? super NodeAllocationResult> nodeDecisionComparator =
        Comparator.comparing(NodeAllocationResult::getNodeDecisionType)
            .thenComparingInt(NodeAllocationResult::getWeightRanking)
            .thenComparing(r -> r.getNode().getId());

    private DecisionUtils() {
        // no instance creation allowed
    }

    /**
     * Generates X-Content for a {@link DiscoveryNode} that leaves off some of the non-critical fields,
     * and assumes the outer object is created outside of this method call.
     */
    public static XContentBuilder discoveryNodeToXContent(XContentBuilder builder, ToXContent.Params params, DiscoveryNode node)
        throws IOException {

        builder.field("id", node.getId());
        builder.field("name", node.getName());
        builder.field("transport_address", node.getAddress().toString());
        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    /**
     * Sorts a map of node level decisions by the decision type, then by weight ranking, and finally by node id.
     */
    public static <T extends NodeAllocationResult> Map<String, T> sortNodeDecisions(Map<String, T> nodeDecisions) {
        return Collections.unmodifiableMap(
            nodeDecisions.values().stream()
                .sorted(nodeDecisionComparator)
                .collect(Collectors.toMap(r -> r.getNode().getId(),
                    Function.identity(),
                    (r1, r2) -> { throw new IllegalArgumentException(String.format(Locale.ROOT, "Duplicate key %s", r1)); },
                    LinkedHashMap::new))
        );
    }

    /**
     * Generates X-Content for the node-level decisions, creating the outer "node_decisions" object
     * in which they are serialized.
     */
    public static XContentBuilder nodeDecisionsToXContent(XContentBuilder builder, ToXContent.Params params,
                                                          Map<String, ? extends NodeAllocationResult> nodeDecisions) throws IOException {
        if (nodeDecisions != null) {
            builder.startObject("node_decisions");
            {
                for (String nodeId : nodeDecisions.keySet()) {
                    NodeAllocationResult explanation = nodeDecisions.get(nodeId);
                    explanation.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        return builder;
    }
}

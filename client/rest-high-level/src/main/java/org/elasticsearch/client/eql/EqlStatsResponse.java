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
package org.elasticsearch.client.eql;

import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EqlStatsResponse {
    private final NodesResponseHeader header;
    private final String clusterName;
    private final List<Node> nodes;

    public EqlStatsResponse(NodesResponseHeader header, String clusterName, List<Node> nodes) {
        this.header = header;
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EqlStatsResponse, Void>
        PARSER = new ConstructingObjectParser<>("eql/stats_response", true, args -> {
            int i = 0;
            NodesResponseHeader header = (NodesResponseHeader) args[i++];
            String clusterName = (String) args[i++];
            List<Node> nodes = (List<Node>) args[i];
            return new EqlStatsResponse(header, clusterName, nodes);
        });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), NodesResponseHeader::fromXContent, new ParseField("_nodes"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster_name"));
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> EqlStatsResponse.Node.PARSER.apply(p, null),
            new ParseField("stats"));
    }

    public static EqlStatsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public NodesResponseHeader getHeader() {
        return header;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EqlStatsResponse that = (EqlStatsResponse) o;
        return Objects.equals(nodes, that.nodes) && Objects.equals(header, that.header) && Objects.equals(clusterName, that.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes, header, clusterName);
    }

    public static class Node {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Node, Void>
            PARSER = new ConstructingObjectParser<>("eql/stats_response_node", true, (args, c) -> new Node((Map<String, Object>) args[0]));

        static {
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), new ParseField("stats"));
        }

        private Map<String, Object> stats;

        public Node(Map<String, Object> stats) {
            this.stats = stats;
        }

        public Map<String, Object> getStats() {
            return stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(stats, node.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stats);
        }
    }
}

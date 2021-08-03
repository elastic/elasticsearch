/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.NodesResponse;
import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.List;

/**
 * Base class for security responses that are node responses. Security uses a common pattern in the
 * response so this class is present to avoid duplication.
 */
public abstract class SecurityNodesResponse extends NodesResponse {

    private final List<Node> nodes;

    SecurityNodesResponse(List<Node> nodes, NodesResponseHeader header, String clusterName) {
        super(header, clusterName);
        this.nodes = nodes;
    }

    /** returns a list of nodes in which the cache was cleared */
    public List<Node> getNodes() {
        return nodes;
    }

    public static class Node {

        private static final ConstructingObjectParser<ClearRolesCacheResponse.Node, String> PARSER =
            new ConstructingObjectParser<>("clear_roles_cache_response_node", false,
                (args, id) -> new ClearRolesCacheResponse.Node(id, (String) args[0]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("name"));
        }

        private final String id;
        private final String name;

        public Node(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    public static <T extends NodesResponse> void declareCommonNodesResponseParsing(ConstructingObjectParser<T, Void> parser) {
        parser.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> Node.PARSER.apply(p, n),
            new ParseField("nodes"));
        NodesResponse.declareCommonNodesResponseParsing(parser);
    }
}

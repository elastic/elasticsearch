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

package org.elasticsearch.client.security;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * The response object that will be returned when clearing the cache of native roles
 */
public final class ClearRolesCacheResponse {

    private final List<Node> nodes;

    public ClearRolesCacheResponse(List<Node> nodes) {
        this.nodes = nodes;
    }

    /** returns a list of nodes in which the cache was cleared */
    public List<Node> getNodes() {
        return nodes;
    }

    public static class Node {

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

    public static ClearRolesCacheResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static Node nodeFromXContent(String id, XContentParser parser) throws IOException {
        ConstructingObjectParser<Node, Void> objectParser = new ConstructingObjectParser<>("clear_roles_cache_response_node",
            false, args -> new Node(id, (String) args[0]));
        objectParser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("name"));
        return objectParser.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ClearRolesCacheResponse, Void> PARSER =
        new ConstructingObjectParser<>("clear_roles_cache_response", false, args -> new ClearRolesCacheResponse((List<Node>)args[0]));

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> nodeFromXContent(n, p), new ParseField("nodes"));
    }

}

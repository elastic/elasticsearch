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

import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * The response object that will be returned when clearing the roles cache
 */
public final class ClearRolesCacheResponse extends SecurityNodesResponse {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ClearRolesCacheResponse, Void> PARSER =
        new ConstructingObjectParser<>("clear_roles_cache_response", false,
            args -> new ClearRolesCacheResponse((List<Node>)args[0], (NodesResponseHeader) args[1], (String) args[2]));

    static {
        SecurityNodesResponse.declareCommonNodesResponseParsing(PARSER);
    }

    public ClearRolesCacheResponse(List<Node> nodes, NodesResponseHeader header, String clusterName) {
        super(nodes, header, clusterName);
    }

    public static ClearRolesCacheResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

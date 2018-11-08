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
 * Response for a clear realm cache request. The response includes a header that contains the
 * number of successful and failed nodes.
 */
public final class ClearRealmCacheResponse extends SecurityNodesResponse {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ClearRealmCacheResponse, Void> PARSER =
        new ConstructingObjectParser<>("clear_realm_cache_response_parser",
            args -> new ClearRealmCacheResponse((List<Node>) args[0], (NodesResponseHeader) args[1], (String) args[2]));

    static {
        SecurityNodesResponse.declareCommonNodesResponseParsing(PARSER);
    }

    public ClearRealmCacheResponse(List<Node> nodes, NodesResponseHeader header, String clusterName) {
        super(nodes, header, clusterName);
    }

    public static ClearRealmCacheResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

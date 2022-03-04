/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * Response for a clear realm cache request. The response includes a header that contains the
 * number of successful and failed nodes.
 */
public final class ClearRealmCacheResponse extends SecurityNodesResponse {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ClearRealmCacheResponse, Void> PARSER = new ConstructingObjectParser<>(
        "clear_realm_cache_response_parser",
        args -> new ClearRealmCacheResponse((List<Node>) args[0], (NodesResponseHeader) args[1], (String) args[2])
    );

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

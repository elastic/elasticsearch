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
 * The response object that will be returned when clearing the privileges cache
 */
public final class ClearPrivilegesCacheResponse extends SecurityNodesResponse {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ClearPrivilegesCacheResponse, Void> PARSER =
        new ConstructingObjectParser<>("clear_privileges_cache_response", false,
            args -> new ClearPrivilegesCacheResponse((List<Node>)args[0], (NodesResponseHeader) args[1], (String) args[2]));

    static {
        SecurityNodesResponse.declareCommonNodesResponseParsing(PARSER);
    }

    public ClearPrivilegesCacheResponse(List<Node> nodes, NodesResponseHeader header, String clusterName) {
        super(nodes, header, clusterName);
    }

    public static ClearPrivilegesCacheResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

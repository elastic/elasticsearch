/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;

/**
 * Base class for responses that are node responses. These responses always contain the cluster
 * name and the {@link NodesResponseHeader}.
 */
public abstract class NodesResponse {

    private final NodesResponseHeader header;
    private final String clusterName;

    protected NodesResponse(NodesResponseHeader header, String clusterName) {
        this.header = header;
        this.clusterName = clusterName;
    }

    /**
     * Get the cluster name associated with all of the nodes.
     *
     * @return Never {@code null}.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Gets information about the number of total, successful and failed nodes the request was run on.
     * Also includes exceptions if relevant.
     */
    public NodesResponseHeader getHeader() {
        return header;
    }

    public static <T extends NodesResponse> void declareCommonNodesResponseParsing(ConstructingObjectParser<T, Void> parser) {
        parser.declareObject(ConstructingObjectParser.constructorArg(), NodesResponseHeader::fromXContent, new ParseField("_nodes"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster_name"));
    }
}

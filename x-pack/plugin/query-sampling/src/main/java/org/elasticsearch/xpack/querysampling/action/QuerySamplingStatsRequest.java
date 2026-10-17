/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;

public final class QuerySamplingStatsRequest extends BaseNodesRequest {

    public QuerySamplingStatsRequest(String... nodesIds) {
        super(nodesIds);
    }
}

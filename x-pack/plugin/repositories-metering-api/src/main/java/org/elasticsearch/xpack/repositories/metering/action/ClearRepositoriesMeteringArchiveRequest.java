/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;

public final class ClearRepositoriesMeteringArchiveRequest extends BaseNodesRequest {
    private final long maxVersionToClear;

    public ClearRepositoriesMeteringArchiveRequest(long maxVersionToClear, String... nodesIds) {
        super(nodesIds);
        this.maxVersionToClear = maxVersionToClear;
    }

    public long getMaxVersionToClear() {
        return maxVersionToClear;
    }
}

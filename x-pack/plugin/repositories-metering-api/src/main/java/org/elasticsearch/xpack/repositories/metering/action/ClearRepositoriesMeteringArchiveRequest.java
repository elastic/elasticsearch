/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamOutput;

public final class ClearRepositoriesMeteringArchiveRequest extends BaseNodesRequest<ClearRepositoriesMeteringArchiveRequest> {
    private final long maxVersionToClear;

    public ClearRepositoriesMeteringArchiveRequest(long maxVersionToClear, String... nodesIds) {
        super(nodesIds);
        this.maxVersionToClear = maxVersionToClear;
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }

    public long getMaxVersionToClear() {
        return maxVersionToClear;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public final class ClearRepositoriesStatsArchiveRequest extends BaseNodesRequest<ClearRepositoriesStatsArchiveRequest> {
    private final long maxVersionToClear;

    public ClearRepositoriesStatsArchiveRequest(StreamInput in) throws IOException {
        super(in);
        this.maxVersionToClear = in.readLong();
    }

    public ClearRepositoriesStatsArchiveRequest(long maxVersionToClear, String... nodesIds) {
        super(nodesIds);
        this.maxVersionToClear = maxVersionToClear;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(maxVersionToClear);
    }

    public long getMaxVersionToClear() {
        return maxVersionToClear;
    }
}

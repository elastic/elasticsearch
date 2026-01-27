/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

/**
 * Explanation response for a shard in the cluster
 */
public class ClusterAllocationExplainResponse extends ActionResponse implements ChunkedToXContentObject {

    private final ClusterAllocationExplanation cae;

    public ClusterAllocationExplainResponse(StreamInput in) throws IOException {
        this.cae = new ClusterAllocationExplanation(in);
    }

    public ClusterAllocationExplainResponse(ClusterAllocationExplanation cae) {
        this.cae = cae;
    }

    /**
     * Return the explanation for shard allocation in the cluster
     */
    public ClusterAllocationExplanation getExplanation() {
        return this.cae;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        cae.writeTo(out);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return cae.toXContentChunked(params);
    }
}

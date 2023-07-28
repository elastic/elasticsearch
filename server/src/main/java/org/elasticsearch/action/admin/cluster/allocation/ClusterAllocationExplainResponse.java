/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Explanation response for a shard in the cluster
 */
public class ClusterAllocationExplainResponse extends ActionResponse {

    private final ClusterAllocationExplanation cae;

    public ClusterAllocationExplainResponse(StreamInput in) throws IOException {
        super(in);
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
}

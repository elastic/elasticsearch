/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;

/**
 * Asks the selected nodes to compute the ground truth of some of their sampled queries.
 */
public final class QuerySamplingGroundTruthRequest extends BaseNodesRequest {

    private final int max;

    /**
     * @param max the most queries each node computes the ground truth of, so that how much work one call
     *            starts is bounded
     */
    public QuerySamplingGroundTruthRequest(int max, String... nodesIds) {
        super(nodesIds);
        this.max = max;
    }

    public int max() {
        return max;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (max < 1) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("[max] must be at least 1 but was [" + max + "]");
            return e;
        }
        return null;
    }
}

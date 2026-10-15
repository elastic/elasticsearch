/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/** Run only on the receiving node; never sent over the wire. */
public final class QuerySamplingGroundTruthRequest extends ActionRequest {

    private final int max;

    public QuerySamplingGroundTruthRequest(int max) {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}

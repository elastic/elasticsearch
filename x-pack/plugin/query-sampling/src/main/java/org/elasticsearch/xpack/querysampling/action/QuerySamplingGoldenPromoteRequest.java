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

/**
 * Asks for stored sampled queries to be promoted to a new version of the golden dataset. It is only run by the node that
 * receives it, which reads the queries from the index of the sample, so it is never sent anywhere.
 */
public final class QuerySamplingGoldenPromoteRequest extends ActionRequest {

    /**
     * The most that can be promoted at once, which is what one search of the sample can return.
     */
    public static final int MAX_QUERIES = 10_000;

    private final int max;

    /**
     * @param max the most queries to promote, so that how much one call writes is bounded
     */
    public QuerySamplingGoldenPromoteRequest(int max) {
        this.max = max;
    }

    public int max() {
        return max;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (max < 1 || max > MAX_QUERIES) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("[max] must be between 1 and " + MAX_QUERIES + " but was [" + max + "]");
            return e;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}

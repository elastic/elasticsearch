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
 * Like the request for ground truth it is only run by the node that receives it, which reads the stored sample from
 * the index, so it is never sent anywhere.
 */
public final class QuerySamplingRecallRequest extends ActionRequest {

    /**
     * The most queries that are read, which is also what an index returns in one search.
     */
    public static final int MAX_SAMPLES = 10_000;

    private final int max;
    private final boolean includeSamples;

    /**
     * @param max            the most queries to estimate from, the most recently picked ones if there are more
     * @param includeSamples whether to also return what each of the queries contributed
     */
    public QuerySamplingRecallRequest(int max, boolean includeSamples) {
        this.max = max;
        this.includeSamples = includeSamples;
    }

    public int max() {
        return max;
    }

    public boolean includeSamples() {
        return includeSamples;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (max < 1 || max > MAX_SAMPLES) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("[max] must be between 1 and " + MAX_SAMPLES + " but was [" + max + "]");
            return e;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}

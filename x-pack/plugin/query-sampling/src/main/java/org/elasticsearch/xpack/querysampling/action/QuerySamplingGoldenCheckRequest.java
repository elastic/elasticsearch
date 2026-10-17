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
public final class QuerySamplingGoldenCheckRequest extends ActionRequest {

    // what one search of the stored index can return
    public static final int MAX_QUERIES = 10_000;

    private final long version;
    private final int max;

    public QuerySamplingGoldenCheckRequest(long version, int max) {
        this.version = version;
        this.max = max;
    }

    public long version() {
        return version;
    }

    public int max() {
        return max;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (version < 0) {
            e = new ActionRequestValidationException();
            e.addValidationError("[version] must not be negative but was [" + version + "]");
        }
        if (max < 1 || max > MAX_QUERIES) {
            e = e == null ? new ActionRequestValidationException() : e;
            e.addValidationError("[max] must be between 1 and " + MAX_QUERIES + " but was [" + max + "]");
        }
        return e;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}

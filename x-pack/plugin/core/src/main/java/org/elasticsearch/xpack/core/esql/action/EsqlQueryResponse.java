/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Releasable;

/**
 * Response to an ES|QL query request.
 *
 * This query response must be closed when the consumer of its response
 * object is finished. Closing the query response closes and invalidates
 * the response object. Calling {@link #response()} on a closed query
 * response results in an IllegalStateException.
 */
public abstract class EsqlQueryResponse extends ActionResponse implements Releasable {

    private boolean closed;

    /** Returns the response object. */
    public EsqlResponse response() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        return responseInternal();
    }

    protected abstract EsqlResponse responseInternal();

    @Override
    public void close() {
        closed = true;
    }
}

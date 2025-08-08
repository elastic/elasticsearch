/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlResponseListener;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

/**
 * Custom response stream that sends everything at once.
 * <p>
 *     Used for backwards compatibility until the other classes are fully backwards-compatible.
 *     That means returning the correct error on an exception, and the expected body.
 * </p>
 * <p>
 *     This class currently delegates its functionality to {@link EsqlResponseListener}.
 * </p>
 */
class NonStreamingEsqlQueryResponseStream implements EsqlQueryResponseStream {

    private final ActionListener<EsqlQueryResponse> listener;

    NonStreamingEsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest) {
        this.listener = new EsqlResponseListener(restChannel, restRequest, esqlRequest).wrapWithLogging();
    }

    @Override
    public void startResponse(List<Attribute> schema) {
        // No-op, all done in the listener
    }

    @Override
    public void sendPages(Iterable<Page> pages) {
        // No-op, all done in the listener
    }

    @Override
    public void finishResponse(EsqlQueryResponse response) {
        // No-op, all done in the listener
    }

    @Override
    public void handleException(Exception e) {
        // No-op, all done in the listener
    }

    @Override
    public ActionListener<EsqlQueryResponse> completionListener() {
        return listener;
    }

    @Override
    public void close() {}
}

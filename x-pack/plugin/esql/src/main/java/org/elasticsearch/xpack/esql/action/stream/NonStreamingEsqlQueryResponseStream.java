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
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlResponseListener;

import java.io.IOException;
import java.util.List;

/**
 * Custom response stream that sends everything at once.
 * <p>
 *     Used for backwards compatibility, as errors would lead to sending an actual response.
 * </p>
 * <p>
 *     This class currently delegates its functionality to {@link EsqlResponseListener}.
 * </p>
 */
public class NonStreamingEsqlQueryResponseStream extends EsqlQueryResponseStream {

    private final ActionListener<EsqlQueryResponse> listener;

    NonStreamingEsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest) throws IOException {
        super(restChannel, restRequest, esqlRequest);

        this.listener = new EsqlResponseListener(restChannel, restRequest, esqlRequest).wrapWithLogging();
    }

    @Override
    protected boolean canBeStreamed() {
        return false;
    }

    @Override
    protected void doStartResponse(List<ColumnInfoImpl> columns) {
        throw new UnsupportedOperationException("This class does not support streaming");
    }

    @Override
    protected void doSendPages(Iterable<Page> pages) {
        throw new UnsupportedOperationException("This class does not support streaming");
    }

    @Override
    protected void doFinishResponse(EsqlQueryResponse response) {
        throw new UnsupportedOperationException("This class does not support streaming");
    }

    @Override
    protected void doHandleException(Exception e) {
        listener.onFailure(e);
    }

    @Override
    protected void doSendEverything(EsqlQueryResponse response) {
        // The base class will close the response, and the listener will do so too.
        // So we need to increment the reference count here
        // TODO: Check this
        response.incRef();
        listener.onResponse(response);
    }

    @Override
    public void close() {
        // TODO: Temporary to avoid closing the stream and sending an empty response. Remove this class later
    }
}

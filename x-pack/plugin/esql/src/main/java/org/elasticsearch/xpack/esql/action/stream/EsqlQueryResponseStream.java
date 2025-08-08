/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.arrow.ArrowFormat;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.io.IOException;
import java.util.List;

/**
 * Streamed {@link EsqlQueryResponse} response.
 */
public interface EsqlQueryResponseStream extends Releasable {
    /**
     * @param shouldStream false if streaming should be disabled
     */
    static EsqlQueryResponseStream forMediaType(
        RestChannel restChannel,
        RestRequest restRequest,
        EsqlQueryRequest esqlRequest,
        boolean shouldStream
    ) throws IOException {
        if (shouldStream == false) {
            // TODO: Make this override the canBeStreamed() instead? To avoid duplicating code and keeping the old classes
            return new NonStreamingEsqlQueryResponseStream(restChannel, restRequest, esqlRequest);
        }

        MediaType mediaType = EsqlMediaTypeParser.getResponseMediaType(restRequest, esqlRequest);

        if (mediaType instanceof TextFormat format) {
            return new TextEsqlQueryResponseStream(restChannel, restRequest, esqlRequest, format);
        } else if (mediaType == ArrowFormat.INSTANCE) {
            // TODO: Add support
            throw new UnsupportedOperationException("Arrow format is not yet supported for streaming");
        }

        return new XContentEsqlQueryResponseStream(restChannel, restRequest, esqlRequest);
    }

    /**
     * Starts the response stream.
     * <p>
     *     This is the first method to be called, with the initial data of the request, before beginning the computation
     * </p>
     */
    void startResponse(List<Attribute> schema);

    /**
     * Called after {@link #startResponse}, for each page of results.
     */
    void sendPages(Iterable<Page> pages);

    /**
     * Last method to be called, when the computation is finished.
     */
    void finishResponse(EsqlQueryResponse response);

    /**
     * Called when an exception is thrown at any point, and the request can't be completed.
     */
    void handleException(Exception e);

    /**
     * Returns a listener to be called when the response is completed.
     * <p>
     *     This listener takes care of calling both {@link #finishResponse} and {@link #handleException}.
     * </p>
     */
    ActionListener<EsqlQueryResponse> completionListener();
}

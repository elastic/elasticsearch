/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.formatter.TextFormat;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;

/**
 * Default, XContent response stream.
 */
class TextEsqlQueryResponseStream extends AbstractEsqlQueryResponseStream<CheckedConsumer<Writer, IOException>> {
    /**
     * Columns, stored on {@link #doStartResponse}, and used later when sending pages.
     */
    @Nullable
    private List<ColumnInfoImpl> columns;

    private final TextFormat format;
    private final boolean dropNullColumns;
    private final String contentType;

    TextEsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest, TextFormat format) {
        super(restChannel, restRequest, esqlRequest);

        this.format = format;
        this.dropNullColumns = restRequest.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        this.contentType = format.contentType(restRequest);
    }

    @Override
    protected boolean canBeStreamed() {
        return false; // TODO: Implement the full streaming
        // return dropNullColumns == false;
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doStartResponse(List<ColumnInfoImpl> columns) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doSendPage(Page page) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doFinishResponse(EsqlQueryResponse response) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doSendEverything(EsqlQueryResponse response) {
        return format.format(restRequest, response);
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doHandleException(Exception e) {
        // TODO: Implement
        return Collections.emptyIterator();
    }

    @Override
    protected void doSendChunks(Iterator<CheckedConsumer<Writer, IOException>> chunks, Releasable releasable) {
        if (chunks.hasNext()) {
            restChannel.sendResponse(
                RestResponse.chunked(RestStatus.OK, ChunkedRestResponseBodyPart.fromTextChunks(contentType, chunks), releasable)
            );
        }
    }

    @Override
    protected void doClose() {
        // TODO: Send the last part to close the chunked response here!
        /*restChannel.sendResponse(
            RestResponse.chunked(
                RestStatus.OK,
                ChunkedRestResponseBodyPart.fromTextChunks(contentType, chunks),
                null
            )
        );*/
    }
}

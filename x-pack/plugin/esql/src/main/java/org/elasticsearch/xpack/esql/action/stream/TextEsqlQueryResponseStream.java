/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.StreamingResponse;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.formatter.TextFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;

/**
 * Default, XContent response stream.
 */
class TextEsqlQueryResponseStream extends AbstractEsqlQueryResponseStream<CheckedConsumer<Writer, IOException>> {

    // TODO: Maybe create this on startResponse()? Does creating this do something with the response? Can we still safely set headers?
    private final StreamingResponse<CheckedConsumer<Writer, IOException>> streamingResponse;

    /**
     * Stored on {@link #doStartResponse}, and used later when sending pages.
     */
    @Nullable
    private List<ColumnInfoImpl> columns;
    /**
     * Stored on {@link #doStartResponse}, and used later when sending pages.
     */
    @Nullable
    private List<DataType> dataTypes;

    private final TextFormat format;
    private final boolean dropNullColumns;

    private RecyclerBytesStreamOutput currentOutput = null;
    /**
     * Writer that uses currentOutput as a sink.
     * <p>
     *     Used to avoid creating new writers for each chunk to write.
     * </p>
     * <p>
     *     Similar to what ChunkedRestResponseBodyPart.fromTextChunks() does
     * </p>
     */
    private final Writer writer = new OutputStreamWriter(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            assert currentOutput != null;
            currentOutput.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            assert currentOutput != null;
            currentOutput.write(b, off, len);
        }

        @Override
        public void flush() {
            assert currentOutput != null;
            currentOutput.flush();
        }

        @Override
        public void close() {
            assert currentOutput != null;
            currentOutput.flush();
        }
    }, StandardCharsets.UTF_8);

    TextEsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest, TextFormat format) {
        super(restChannel, restRequest, esqlRequest);

        this.format = format;
        this.dropNullColumns = restRequest.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        this.streamingResponse = new StreamingResponse<>(restChannel, format.contentType(restRequest), () -> {}, (chunk, streamOutput) -> {
            currentOutput = streamOutput;
            chunk.accept(writer);
            writer.flush();
            currentOutput = null;
        });
    }

    @Override
    protected boolean canBeStreamed() {
        return dropNullColumns == false;
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doStartResponse(List<ColumnInfoImpl> columns) {
        this.columns = columns;
        this.dataTypes = columns.stream().map(ColumnInfoImpl::type).toList();
        return format.formatHeader(restRequest, columns, new boolean[columns.size()]);
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doSendPages(Iterable<Page> pages) {
        return format.formatRows(
            restRequest,
            columns,
            () -> ResponseValueUtils.pagesToValues(dataTypes, pages),
            new boolean[dataTypes.size()]
        );
    }

    @Override
    protected Iterator<CheckedConsumer<Writer, IOException>> doFinishResponse(EsqlQueryResponse response) {
        // Nothing to do here
        return Collections.emptyIterator();
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
            streamingResponse.writeFragment(chunks, releasable);
        }
    }

    @Override
    protected void doClose() {
        streamingResponse.close();
    }
}

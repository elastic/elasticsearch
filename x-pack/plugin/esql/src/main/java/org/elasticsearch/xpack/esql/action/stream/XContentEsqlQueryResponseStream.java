/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.StreamingXContentResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.ResponseXContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;

/**
 * XContent response stream.
 */
class XContentEsqlQueryResponseStream extends AbstractEsqlQueryResponseStream<ToXContent> {

    // TODO: Maybe create this on startResponse()? Does creating this do something with the response? Can we still safely set headers?
    private final StreamingXContentResponse streamingXContentResponse;

    /**
     * Columns, stored on {@link #doStartResponse}, and used later when sending pages.
     */
    @Nullable
    private List<ColumnInfoImpl> columns;

    private final boolean dropNullColumns;

    XContentEsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest) throws IOException {
        super(restChannel, restRequest, esqlRequest);

        this.dropNullColumns = restRequest.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        this.streamingXContentResponse = new StreamingXContentResponse(restChannel, restChannel.request(), () -> {});
    }

    @Override
    protected boolean canBeStreamed() {
        return dropNullColumns == false && esqlRequest.columnar() == false;
    }

    @Override
    protected Iterator<ToXContent> doStartResponse(List<ColumnInfoImpl> columns) {
        assert dropNullColumns == false : "this method doesn't support dropping null columns";

        this.columns = columns;

        var content = new ArrayList<Iterator<ToXContent>>(3);

        content.add(ChunkedToXContentHelper.startObject());
        content.add(ResponseXContentUtils.allColumns(columns, "columns"));

        // Start the values array, to be filled in
        content.add(ChunkedToXContentHelper.startArray("values"));

        return asIterator(content);
    }

    @Override
    protected Iterator<ToXContent> doSendPages(Iterable<Page> pages) {
        assert columns != null : "columns must be set before sending pages";

        return ResponseXContentUtils.rowValues(columns, pages, null);
    }

    @Override
    protected Iterator<ToXContent> doFinishResponse(EsqlQueryResponse response) {
        var content = new ArrayList<Iterator<ToXContent>>(10);

        // End the values array
        content.add(ChunkedToXContentHelper.endArray());

        var executionInfo = response.getExecutionInfo();
        if (executionInfo != null) {
            if (executionInfo.overallTook() != null) {
                content.add(
                    ChunkedToXContentHelper.chunk(
                        (builder, p) -> builder //
                            .field("took", executionInfo.overallTook().millis())
                            .field(EsqlExecutionInfo.IS_PARTIAL_FIELD.getPreferredName(), executionInfo.isPartial())
                    )
                );
            }
            if (executionInfo.hasMetadataToReport()) {
                content.add(ChunkedToXContentHelper.field("_clusters", executionInfo, restRequest));
            }
        }
        content.add(
            ChunkedToXContentHelper.chunk(
                (builder, p) -> builder //
                    .field("documents_found", response.documentsFound())
                    .field("values_loaded", response.valuesLoaded())
            )
        );

        var profile = response.profile();
        if (profile != null) {
            content.add(ChunkedToXContentHelper.startObject("profile"));
            content.add(ChunkedToXContentHelper.chunk((b, p) -> {
                if (executionInfo != null) {
                    b.field("query", executionInfo.overallTimeSpan());
                    b.field("planning", executionInfo.planningTimeSpan());
                }
                return b;
            }));
            content.add(ChunkedToXContentHelper.array("drivers", profile.drivers().iterator(), restRequest));
            content.add(ChunkedToXContentHelper.array("plans", profile.plans().iterator()));
            content.add(ChunkedToXContentHelper.endObject());
        }

        content.add(ChunkedToXContentHelper.endObject());

        return asIterator(content);
    }

    @Override
    protected Iterator<ToXContent> doSendEverything(EsqlQueryResponse response) {
        // final Releasable releasable = releasableFromResponse(esqlResponse);

        // TODO: Instead of sendChunks, implement a flush() to attach the response to it? Or pass the response to the methods
        // TODO: Or make "doX" methods return an Iterator<? extends ToXContent> and then concat them all together

        var content = new ArrayList<Iterator<ToXContent>>(3);
        boolean[] nullColumns = null;
        if (dropNullColumns) {
            nullColumns = nullColumns(response.columns(), response.pages());
            content.add(sendStartResponseDroppingNullColumns(response.columns(), nullColumns));
        } else {
            content.add(doStartResponse(response.columns()));
        }
        // doSendPages doesn't work with nullColumns or columnar, so we generate them here directly
        content.add(ResponseXContentUtils.columnValues(response.columns(), response.pages(), esqlRequest.columnar(), nullColumns));
        content.add(doFinishResponse(response));
        return asIterator(content);
    }

    @Override
    protected Iterator<ToXContent> doHandleException(Exception e) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private Iterator<ToXContent> sendStartResponseDroppingNullColumns(List<ColumnInfoImpl> columns, boolean[] nullColumns) {
        assert dropNullColumns : "this method should only be called when dropping null columns";

        var content = new ArrayList<Iterator<ToXContent>>(3);

        content.add(ChunkedToXContentHelper.startObject());
        content.add(ResponseXContentUtils.allColumns(columns, "all_columns"));
        content.add(ResponseXContentUtils.nonNullColumns(columns, nullColumns, "columns"));

        return asIterator(content);
    }

    private boolean[] nullColumns(List<ColumnInfoImpl> columns, List<Page> pages) {
        boolean[] nullColumns = new boolean[columns.size()];
        for (int c = 0; c < nullColumns.length; c++) {
            nullColumns[c] = allColumnsAreNull(pages, c);
        }
        return nullColumns;
    }

    private boolean allColumnsAreNull(List<Page> pages, int c) {
        for (Page page : pages) {
            if (page.getBlock(c).areAllValuesNull() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void doSendChunks(Iterator<ToXContent> chunks, Releasable releasable) {
        if (chunks.hasNext()) {
            streamingXContentResponse.writeFragment(p0 -> chunks, releasable);
        }
    }

    @Override
    protected void doClose() {
        streamingXContentResponse.close();
    }
}

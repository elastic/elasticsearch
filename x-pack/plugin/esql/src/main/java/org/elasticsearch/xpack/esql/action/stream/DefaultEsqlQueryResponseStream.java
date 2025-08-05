/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.ResponseXContentUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;

/**
 * Default, XContent response stream.
 */
public class DefaultEsqlQueryResponseStream extends EsqlQueryResponseStream {
    DefaultEsqlQueryResponseStream(RestChannel restChannel, ToXContent.Params params) {
        super(restChannel, params);
    }

    @Override
    protected boolean canBeStreamed() {
        boolean dropNullColumns = params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        return dropNullColumns == false;
    }

    @Override
    protected void doStartResponse(List<ColumnInfoImpl> columns) {
        assert params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false) == false : "this method doesn't support dropping null columns";

        var content = new ArrayList<Iterator<? extends ToXContent>>(1);

        content.add(ResponseXContentUtils.allColumns(columns, "columns"));

        sendChunks(content);
    }

    @Override
    protected void doSendPages(Iterable<Page> pages) {
        // TODO: Implement
    }

    @Override
    protected void doFinishResponse(EsqlQueryResponse response) {
        // TODO: Implement
    }

    @Override
    protected void doSendEverything(EsqlQueryResponse response) {
        boolean dropNullColumns = params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);

        // TODO: Close the response
        // final Releasable releasable = releasableFromResponse(esqlResponse);

        // TODO: Instead of sendChunks, implement a flush() to attach the response to it? Or pass the response to the methods

        if (dropNullColumns) {
            sendStartResponseDroppingNullColumns(response.columns(), response.pages());
        } else {
            doStartResponse(response.columns());
        }
        doSendPages(response.pages());
        doFinishResponse(response);
    }

    private void sendStartResponseDroppingNullColumns(List<ColumnInfoImpl> columns, List<Page> pages) {
        assert params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false) : "this method should only be called when dropping null columns";

        var content = new ArrayList<Iterator<? extends ToXContent>>(2);

        boolean[] nullColumns = nullColumns(columns, pages);
        content.add(ResponseXContentUtils.allColumns(columns, "all_columns"));
        content.add(ResponseXContentUtils.nonNullColumns(columns, nullColumns, "columns"));

        sendChunks(content);
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

    // TODO:
    /*
    On start, check if it's columnar or something else that disable streaming.
    If streaming is ok, create the StreamingXContentResponse and write the opening fragment.
    If not, leave it null.

    On dispose, check if the StreamingXContentResponse is null or not.
    If it's null, send the full response directly.
    If it's not null, write the final fragment and close the StreamingXContentResponse.
    */

    /*
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        boolean dropNullColumns = params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        boolean[] nullColumns = dropNullColumns ? nullColumns() : null;

        var content = new ArrayList<Iterator<? extends ToXContent>>(25);
        content.add(ChunkedToXContentHelper.startObject());
        if (isAsync) {
            content.add(ChunkedToXContentHelper.chunk((builder, p) -> {
                if (asyncExecutionId != null) {
                    builder.field("id", asyncExecutionId);
                }
                builder.field("is_running", isRunning);
                return builder;
            }));
        }
        if (executionInfo != null && executionInfo.overallTook() != null) {
            content.add(
                ChunkedToXContentHelper.chunk(
                    (builder, p) -> builder //
                        .field("took", executionInfo.overallTook().millis())
                        .field(EsqlExecutionInfo.IS_PARTIAL_FIELD.getPreferredName(), executionInfo.isPartial())
                )
            );
        }
        content.add(
            ChunkedToXContentHelper.chunk(
                (builder, p) -> builder //
                    .field("documents_found", documentsFound)
                    .field("values_loaded", valuesLoaded)
            )
        );
        if (dropNullColumns) {
            content.add(ResponseXContentUtils.allColumns(columns, "all_columns"));
            content.add(ResponseXContentUtils.nonNullColumns(columns, nullColumns, "columns"));
        } else {
            content.add(ResponseXContentUtils.allColumns(columns, "columns"));
        }
        content.add(
            ChunkedToXContentHelper.array("values", ResponseXContentUtils.columnValues(this.columns, this.pages, columnar, nullColumns))
        );
        if (executionInfo != null && executionInfo.hasMetadataToReport()) {
            content.add(ChunkedToXContentHelper.field("_clusters", executionInfo, params));
        }
        if (profile != null) {
            content.add(ChunkedToXContentHelper.startObject("profile"));
            content.add(ChunkedToXContentHelper.chunk((b, p) -> {
                if (executionInfo != null) {
                    b.field("query", executionInfo.overallTimeSpan());
                    b.field("planning", executionInfo.planningTimeSpan());
                }
                return b;
            }));
            content.add(ChunkedToXContentHelper.array("drivers", profile.drivers.iterator(), params));
            content.add(ChunkedToXContentHelper.array("plans", profile.plans.iterator()));
            content.add(ChunkedToXContentHelper.endObject());
        }
        content.add(ChunkedToXContentHelper.endObject());

        return Iterators.concat(content.toArray(Iterator[]::new));
    }*/

    /*void startResponse(Releasable releasable) throws IOException {
        assert hasReferences();
        assert streamingXContentResponse == null;
        streamingXContentResponse = new StreamingXContentResponse(restChannel, restChannel.request(), () -> {});
        streamingXContentResponse.writeFragment(
            p0 -> ChunkedToXContentHelper.chunk((b, p) -> b.startObject().startArray("log")),
            releasable
        );
    }

    void writePage(RepositoryVerifyIntegrityResponseChunk chunk, Releasable releasable) {
        assert hasReferences();
        assert streamingXContentResponse != null;

        if (chunk.type() == RepositoryVerifyIntegrityResponseChunk.Type.ANOMALY) {
            anomalyCount.incrementAndGet();
        }
        streamingXContentResponse.writeFragment(
            p0 -> ChunkedToXContentHelper.chunk((b, p) -> b.startObject().value(chunk, p).endObject()),
            releasable
        );
    }

    void writeChunk(RepositoryVerifyIntegrityResponseChunk chunk, Releasable releasable) {
        assert hasReferences();
        assert streamingXContentResponse != null;

        if (chunk.type() == RepositoryVerifyIntegrityResponseChunk.Type.ANOMALY) {
            anomalyCount.incrementAndGet();
        }
        streamingXContentResponse.writeFragment(
            p0 -> ChunkedToXContentHelper.chunk((b, p) -> b.startObject().value(chunk, p).endObject()),
            releasable
        );
    }

    @Override
    protected void closeInternal() {
        try {
            assert finalResultListener.isDone();
            finalResultListener.addListener(new ActionListener<>() {
                @Override
                public void onResponse(RepositoryVerifyIntegrityResponse repositoryVerifyIntegrityResponse) {
                    // success - finish the response with the final results
                    assert streamingXContentResponse != null;
                    streamingXContentResponse.writeFragment(
                        p0 -> ChunkedToXContentHelper.chunk(
                            (b, p) -> b.endArray()
                                .startObject("results")
                                .field("status", repositoryVerifyIntegrityResponse.finalTaskStatus())
                                .field("final_repository_generation", repositoryVerifyIntegrityResponse.finalRepositoryGeneration())
                                .field("total_anomalies", anomalyCount.get())
                                .field(
                                    "result",
                                    anomalyCount.get() == 0
                                        ? repositoryVerifyIntegrityResponse
                                            .originalRepositoryGeneration() == repositoryVerifyIntegrityResponse.finalRepositoryGeneration()
                                                ? "pass"
                                                : "inconclusive due to concurrent writes"
                                        : "fail"
                                )
                                .endObject()
                                .endObject()
                        ),
                        () -> {}
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    if (streamingXContentResponse != null) {
                        // failure after starting the response - finish the response with a rendering of the final exception
                        streamingXContentResponse.writeFragment(
                            p0 -> ChunkedToXContentHelper.chunk(
                                (b, p) -> b.endArray()
                                    .startObject("exception")
                                    .value((bb, pp) -> ElasticsearchException.generateFailureXContent(bb, pp, e, true))
                                    .field("status", ExceptionsHelper.status(e))
                                    .endObject()
                                    .endObject()
                            ),
                            () -> {}
                        );
                    } else {
                        // didn't even get as far as starting to stream the response, must have hit an early exception (e.g. repo not found)
                        // so we can return this exception directly.
                        try {
                            restChannel.sendResponse(new RestResponse(restChannel, e));
                        } catch (IOException e2) {
                            e.addSuppressed(e2);
                            logger.error("error building error response", e);
                            assert false : e; // shouldn't actually throw anything here
                            restChannel.request().getHttpChannel().close();
                        }
                    }
                }
            });
        } finally {
            Releasables.closeExpectNoException(streamingXContentResponse);
        }
    }

    public ActionListener<RepositoryVerifyIntegrityResponse> getCompletionListener() {
        return completionListener;
    }*/
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.stream;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.StreamingXContentResponse;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.arrow.ArrowFormat;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Base class for streamed {@link EsqlQueryResponse} responses.
 *
 * TODO: Should this be thread-safe?
 * TODO: Add something to complete with an error on error. Is this BWC?
 * TODO: Took header wouldn't be available on streaming
 */
public abstract class EsqlQueryResponseStream implements Releasable {
    private static final Logger LOGGER = LogManager.getLogger(EsqlQueryResponseStream.class);

    public static EsqlQueryResponseStream forMediaType(RestChannel restChannel, RestRequest request) {
        MediaType mediaType = EsqlMediaTypeParser.getResponseMediaType(request, XContentType.JSON);

        if (mediaType instanceof TextFormat) {
            // TODO: Add support
            throw new UnsupportedOperationException("Text formats is not yet supported for streaming");
        } else if (mediaType == ArrowFormat.INSTANCE) {
            // TODO: Add support
            throw new UnsupportedOperationException("Arrow format is not yet supported for streaming");
        }

        return new DefaultEsqlQueryResponseStream(restChannel, request);
    }

    private final RestChannel restChannel;
    protected final ToXContent.Params params;
    /**
     * Initialized on the first call to {@link #startResponse} and used to write the response chunks.
     */
    @Nullable
    private StreamingXContentResponse streamingXContentResponse;
    /**
     * Flag to check if we sent the starting chunk of the response.
     * <p>
     *     Used to know if we should just send everything at once on {@link #finishResponse}.
     * </p>
     */
    private boolean initialStreamChunkSent = false;
    /**
     * Flag to check we don't call {@link #finishResponse} more than once.
     */
    private boolean finished = false;

    protected EsqlQueryResponseStream(RestChannel restChannel, ToXContent.Params params) {
        this.restChannel = restChannel;
        this.params = params;
    }

    /**
     * Starts the response stream. This is the first method to be called
     */
    public final void startResponse(List<ColumnInfoImpl> columns) throws IOException {
        assert streamingXContentResponse == null : "startResponse() called more than once";
        assert finished == false : "sendPages() called on a finished stream";

        streamingXContentResponse = new StreamingXContentResponse(restChannel, restChannel.request(), () -> {});

        if (canBeStreamed() == false) {
            return;
        }

        doStartResponse(columns);
        initialStreamChunkSent = true;
    }

    public final void sendPages(Iterable<Page> pages) {
        assert streamingXContentResponse != null : "sendPages() called before startResponse()";
        assert finished == false : "sendPages() called on a finished stream";

        if (initialStreamChunkSent) {
            doSendPages(pages);
        }
    }

    public final void finishResponse(EsqlQueryResponse response) {
        assert finished == false : "finishResponse() called more than once";

        if (initialStreamChunkSent) {
            doFinishResponse(response);
        } else {
            doSendEverything(response);
        }
        finished = true;
    }

    public final void handleException(Exception e) {
        assert finished == false : "handleException() called on a finished stream";

        // TODO: To be overridden by subclasses. This should append the error to the stream, if possible
        LOGGER.error("Error while streaming response", e);

        finished = true;
    }

    // TODO: For error handling, check RestActionListener error listener
    // TODO: Also ensure that we check if the channel is closed at some points (Also see RestActionListener)

    public final ActionListener<EsqlQueryResponse> completionListener() {
        return new ActionListener<>() {
            @Override
            public void onResponse(EsqlQueryResponse esqlResponse) {
                assert finished == false : "completionListener() called on a finished stream";

                finishResponse(esqlResponse);
            }

            @Override
            public void onFailure(Exception e) {
                assert finished == false : "onFailure() called on a finished stream";

                handleException(e);
            }
        };
    }

    /**
     * Returns true if the response can be streamed, false otherwise.
     * <p>
     *     Some parameters make the response not streamable, such as `columnar` or `drop_null_columns`,
     *     as the initial chunk can't be calculated until all pages are received.
     * </p>
     */
    protected abstract boolean canBeStreamed();

    protected abstract void doStartResponse(List<ColumnInfoImpl> columns);

    protected abstract void doSendPages(Iterable<Page> pages);

    protected abstract void doFinishResponse(EsqlQueryResponse response);

    protected abstract void doHandleException(Exception e);

    protected void doSendEverything(EsqlQueryResponse response) {
        // TODO: Is this safe? Should this be abstract to ensure proper implementation? Add tests for both cases
        doStartResponse(response.columns());
        doSendPages(response.pages());
        doFinishResponse(response);
    }

    @SuppressWarnings("unchecked")
    protected final void sendChunks(List<Iterator<? extends ToXContent>> chunkedContent) {
        assert streamingXContentResponse != null : "startResponse() not called yet";

        // TODO: Maybe accept a single chunk here, and do a flush() inside of each method?
        streamingXContentResponse.writeFragment(p0 -> Iterators.concat(chunkedContent.toArray(Iterator[]::new)), () -> {});
    }

    @Override
    public void close() {
        // TODO: Implement and check closing everywhere
        if (streamingXContentResponse != null) {
            streamingXContentResponse.close();
            streamingXContentResponse = null;
        }
        finished = true;
    }
}

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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.StreamingXContentResponse;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.arrow.ArrowFormat;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

    /**
     * @param shouldStream false if streaming should be disabled
     */
    public static EsqlQueryResponseStream forMediaType(
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

        if (mediaType instanceof TextFormat) {
            // TODO: Add support
            throw new UnsupportedOperationException("Text formats are not yet supported for streaming");
        } else if (mediaType == ArrowFormat.INSTANCE) {
            // TODO: Add support
            throw new UnsupportedOperationException("Arrow format is not yet supported for streaming");
        }

        return new DefaultEsqlQueryResponseStream(restChannel, restRequest, esqlRequest);
    }

    private final RestChannel restChannel;
    protected final RestRequest restRequest;
    protected final EsqlQueryRequest esqlRequest;

    // TODO: Maybe create this on startResponse()? Does creating this do something with the response? Can we still safely set headers?
    private final StreamingXContentResponse streamingXContentResponse;

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

    protected EsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest) throws IOException {
        this.restChannel = restChannel;
        this.restRequest = restRequest;
        this.esqlRequest = esqlRequest;
        this.streamingXContentResponse = new StreamingXContentResponse(restChannel, restChannel.request(), () -> {});
    }

    /**
     * Starts the response stream. This is the first method to be called
     */
    public final void startResponse(List<Attribute> schema) {
        assert initialStreamChunkSent == false : "startResponse() called more than once";
        assert finished == false : "sendPages() called on a finished stream";

        if (canBeStreamed() == false) {
            return;
        }

        // TODO: Copied from TransportEsqlQueryAction#toResponse. Deduplicate this code
        List<ColumnInfoImpl> columns = schema.stream().map(c -> {
            List<String> originalTypes;
            if (c instanceof UnsupportedAttribute ua) {
                // Sort the original types so they are easier to test against and prettier.
                originalTypes = new ArrayList<>(ua.originalTypes());
                Collections.sort(originalTypes);
            } else {
                originalTypes = null;
            }
            return new ColumnInfoImpl(c.name(), c.dataType().outputType(), originalTypes);
        }).toList();

        sendChunks(doStartResponse(columns));

        initialStreamChunkSent = true;
    }

    public final void sendPages(Iterable<Page> pages) {
        assert finished == false : "sendPages() called on a finished stream";

        if (initialStreamChunkSent) {
            sendChunks(doSendPages(pages));
        }
    }

    public final void finishResponse(EsqlQueryResponse response) {
        assert finished == false : "finishResponse() called more than once";

        // TODO: Also, is this closing right? EsqlResponseListener uses releasableFromResponse(), which increments the ref first
        response.mustIncRef();
        Releasable releasable = Releasables.assertOnce(response::decRef);
        boolean success = false;
        try {
            if (initialStreamChunkSent) {
                sendChunks(doFinishResponse(response), releasable);
            } else {
                sendChunks(doSendEverything(response), releasable);
            }
            success = true;
        } finally {
            if (success == false) {
                releasable.close();
            }
            finished = true;
        }
    }

    public final void handleException(Exception e) {
        assert finished == false : "handleException() called on a finished stream";

        // TODO: To be overridden by subclasses. This should append the error to the stream, if possible
        LOGGER.error("Error while streaming response", e);

        sendChunks(doHandleException(e));

        finished = true;
    }

    // TODO: For error handling, check RestActionListener error listener
    // TODO: Also ensure that we check if the channel is closed at some points (Also see RestActionListener)

    public final ActionListener<EsqlQueryResponse> completionListener() {
        return ActionListener.releaseAfter(new ActionListener<>() {
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
        }, this);
    }

    /**
     * Returns true if the response can be streamed, false otherwise.
     * <p>
     *     Some parameters make the response not streamable, such as `columnar` or `drop_null_columns`,
     *     as the initial chunk can't be calculated until all pages are received.
     * </p>
     */
    protected abstract boolean canBeStreamed();

    /**
     * Returns the chunks to be sent at the beginning of the response. Called once, at the start.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code true}.
     * </p>
     */
    protected abstract Iterator<? extends ToXContent> doStartResponse(List<ColumnInfoImpl> columns);

    /**
     * Returns the chunks for the given page. Called 0 to N times, after {@link #doStartResponse} and before {@link #doFinishResponse}.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code true}.
     * </p>
     */
    protected abstract Iterator<? extends ToXContent> doSendPages(Iterable<Page> pages);

    /**
     * Returns the remaining chunks of the response. Called once, at the end of the response.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code true}.
     * </p>
     */
    protected abstract Iterator<? extends ToXContent> doFinishResponse(EsqlQueryResponse response);

    /**
     * Returns the chunks to be sent for the given exception.
     * <p>
     *     This may be called at any time, so the code should track what was sent already
     *     and how to send a meaningful response given the chunks sent in previous calls.
     * </p>
     */
    protected abstract Iterator<? extends ToXContent> doHandleException(Exception e);

    /**
     * Returns the chunks of the full response. Called once for the full response.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code false}.
     * </p>
     */
    protected Iterator<? extends ToXContent> doSendEverything(EsqlQueryResponse response) {
        // TODO: Is this safe? Should this be abstract to ensure proper implementation? Add tests for both streamed and "everything" cases
        return Iterators.concat(doStartResponse(response.columns()), doSendPages(response.pages()), doFinishResponse(response));
    }

    @SuppressWarnings("unchecked")
    protected static Iterator<? extends ToXContent> asIterator(List<Iterator<? extends ToXContent>> chunks) {
        return Iterators.concat(chunks.toArray(Iterator[]::new));
    }

    private void sendChunks(Iterator<? extends ToXContent> chunks) {
        sendChunks(chunks, () -> {});
    }

    private void sendChunks(Iterator<? extends ToXContent> chunks, Releasable releasable) {
        if (chunks.hasNext()) {
            streamingXContentResponse.writeFragment(p0 -> chunks, releasable);
        }
    }

    @Override
    public void close() {
        // TODO: Implement and check closing everywhere
        streamingXContentResponse.close();
        finished = true;
    }
}

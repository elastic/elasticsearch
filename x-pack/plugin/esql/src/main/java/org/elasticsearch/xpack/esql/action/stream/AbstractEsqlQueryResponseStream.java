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
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;

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
abstract class AbstractEsqlQueryResponseStream<T> implements EsqlQueryResponseStream {
    private static final Logger LOGGER = LogManager.getLogger(AbstractEsqlQueryResponseStream.class);

    protected final RestChannel restChannel;
    protected final RestRequest restRequest;
    protected final EsqlQueryRequest esqlRequest;

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

    protected AbstractEsqlQueryResponseStream(RestChannel restChannel, RestRequest restRequest, EsqlQueryRequest esqlRequest) {
        this.restChannel = restChannel;
        this.restRequest = restRequest;
        this.esqlRequest = esqlRequest;
    }

    /**
     * Starts the response stream. This is the first method to be called
     */
    @Override
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

    @Override
    public final void sendPage(Page pages) {
        assert finished == false : "sendPage() called on a finished stream";

        if (initialStreamChunkSent) {
            sendChunks(doSendPage(page));
        }
    }

    @Override
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

    @Override
    public final void handleException(Exception e) {
        assert finished == false : "handleException() called on a finished stream";

        // TODO: To be overridden by subclasses. This should append the error to the stream, if possible
        LOGGER.error("Error while streaming response", e);

        sendChunks(doHandleException(e));

        finished = true;
    }

    // TODO: For error handling, check RestActionListener error listener
    // TODO: Also ensure that we check if the channel is closed at some points (Also see RestActionListener)

    @Override
    public ActionListener<EsqlQueryResponse> completionListener() {
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
    protected abstract Iterator<T> doStartResponse(List<ColumnInfoImpl> columns);

    /**
     * Returns the chunks for the given page. Called 0 to N times, after {@link #doStartResponse} and before {@link #doFinishResponse}.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code true}.
     * </p>
     */
    protected abstract Iterator<T> doSendPage(Page page);

    /**
     * Returns the remaining chunks of the response. Called once, at the end of the response.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code true}.
     * </p>
     */
    protected abstract Iterator<T> doFinishResponse(EsqlQueryResponse response);

    /**
     * Returns the chunks to be sent for the given exception.
     * <p>
     *     This may be called at any time, so the code should track what was sent already
     *     and how to send a meaningful response given the chunks sent in previous calls.
     * </p>
     */
    protected abstract Iterator<T> doHandleException(Exception e);

    /**
     * Returns the chunks of the full response. Called once for the full response.
     * <p>
     *     Only called if {@link #canBeStreamed()} returns {@code false}.
     * </p>
     */
    protected Iterator<T> doSendEverything(EsqlQueryResponse response) {
        // TODO: Is this safe? Should this be abstract to ensure proper implementation? Add tests for both streamed and "everything" cases
        return Iterators.concat(doStartResponse(response.columns()), doSendPage(response.pages()), doFinishResponse(response));
    }

    protected abstract void doSendChunks(Iterator<T> chunks, Releasable releasable);

    protected void doClose() {}

    @SuppressWarnings("unchecked")
    protected static <T> Iterator<T> asIterator(List<Iterator<T>> chunks) {
        return Iterators.concat(chunks.toArray(Iterator[]::new));
    }

    private void sendChunks(Iterator<T> chunks) {
        sendChunks(chunks, () -> {});
    }

    protected void sendChunks(Iterator<T> chunks, Releasable releasable) {
        doSendChunks(chunks, releasable);
    }

    @Override
    public void close() {
        doClose();
        finished = true;
    }
}

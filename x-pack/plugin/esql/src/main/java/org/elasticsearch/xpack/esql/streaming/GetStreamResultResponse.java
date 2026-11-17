/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

/**
 * The result of a stream read: the pages read from the storage and the metadata read with them. It lives on one node and is never
 * serialized.
 * <p>
 * The pages are owned by this response until {@link #takePages()} hands them to the response body. Closing the response releases them
 * if they were not taken.
 */
public class GetStreamResultResponse extends ActionResponse implements Releasable {

    private final GetStreamResultRequest request;
    private final StreamMetadata metadata;
    private StreamPages pages;

    /**
     * @param request the request this answers, for its offset and limits
     * @param pages   the pages that were read. For a status-only request there are none.
     */
    public GetStreamResultResponse(GetStreamResultRequest request, StreamPages pages) {
        this.request = request;
        this.metadata = pages.metadata();
        this.pages = pages;
    }

    public GetStreamResultRequest request() {
        return request;
    }

    /** The metadata read together with the pages. */
    public StreamMetadata metadata() {
        return metadata;
    }

    /** Hands the pages, and their ownership, to the caller. It can be taken once. */
    public synchronized StreamPages takePages() {
        StreamPages taken = pages;
        pages = null;
        if (taken == null) {
            throw new IllegalStateException("the pages were already taken");
        }
        return taken;
    }

    @Override
    public synchronized void close() {
        if (pages != null) {
            pages.close();
            pages = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}

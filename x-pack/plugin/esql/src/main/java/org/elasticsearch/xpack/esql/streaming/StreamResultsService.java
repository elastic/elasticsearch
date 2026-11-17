/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Reads the stored result of a stream, the counterpart of {@code AsyncResultsService}.
 * <p>
 * The flow is: look up the metadata, check that the stream exists, has not expired and belongs to the caller (all three give the same
 * 404), then read the pages once, waiting for rows if the request asked for it, and return them. It only uses the {@link StreamStorage}.
 * There is no task or node memory to look at.
 * <p>
 * A GET makes a single {@code getPages} call, for {@code max_rows} rows or, when it is not set, for every row from the offset. The pages
 * of that read are held until they are written, so the memory a GET uses is the size of what it returns.
 */
public class StreamResultsService {

    private final StreamStorage storage;
    private final StreamSearchSecurity security;
    private final ThreadPool threadPool;

    public StreamResultsService(StreamStorage storage, StreamSearchSecurity security, ThreadPool threadPool) {
        this.storage = storage;
        this.security = security;
        this.threadPool = threadPool;
    }

    public void retrieveResult(CancellableTask task, GetStreamResultRequest request, ActionListener<GetStreamResultResponse> listener) {
        StreamStorage store = storage;
        String id = request.id();
        // A "not found" of the storage itself must look like the one of the checks below. Only the storage call is wrapped: a failure
        // produced further down, such as the stored failure of a query that failed with a "not found" of its own, is passed on as is.
        store.getMetadata(id, StreamTaskManagementService.uniformNotFound(id, listener.delegateFailureAndWrap((l, metadata) -> {
            // Security and expiry are checked once, up front, before any wait or page read.
            // TODO: this compares the caller with the security headers stored in the metadata, but nothing stores them yet: the stream
            // POST does not call StreamStorage#create. When it does, it must pass the security headers of the request that started the
            // query. Empty headers mean "created with security off" and let every caller read the stream. See StreamSearchSecurity.
            StreamMetadata checked = security.getStreamAndCheckAuthentication(id, metadata, System.currentTimeMillis());
            if (request.statusOnly()) {
                respond(request, new StreamPages(checked, List.of()), l);
                return;
            }
            StreamTaskManagementService.addCompletionListener(
                threadPool,
                task,
                store,
                id,
                // the metadata that was just checked serves the first read, so it is not read twice in a row
                checked,
                request.offset(),
                rowsToRead(request),
                request.waitTimeout(),
                l.delegateFailureAndWrap((l2, pages) -> respond(request, pages, l2))
            );
        })));
    }

    /**
     * The number of rows to ask the storage for: {@code max_rows} when it is set, otherwise every row from the offset. It is clamped so
     * that {@code offset + rows} cannot overflow inside the storage.
     */
    static long rowsToRead(GetStreamResultRequest request) {
        long rest = Long.MAX_VALUE - request.offset();
        return request.hasRowLimit() ? Math.min(request.maxRows(), rest) : rest;
    }

    private static void respond(GetStreamResultRequest request, StreamPages pages, ActionListener<GetStreamResultResponse> listener) {
        Exception failure = validate(request, pages);
        if (failure != null) {
            pages.close();
            listener.onFailure(failure);
            return;
        }
        listener.onResponse(new GetStreamResultResponse(request, pages));
    }

    /** Returns the exception that makes this read an HTTP error instead of a response, or {@code null}. */
    private static Exception validate(GetStreamResultRequest request, StreamPages pages) {
        StreamMetadata metadata = pages.metadata();
        if (metadata.status() == StreamMetadata.Status.COMPLETE && metadata.completionInfo() != null) {
            long totalRows = metadata.completionInfo().rows() == null ? Long.MAX_VALUE : metadata.completionInfo().rows();
            if (request.offset() > totalRows) {
                return new IllegalArgumentException(
                    "offset [" + request.offset() + "] is beyond the end of the result [" + totalRows + "]"
                );
            }
        }
        // A failure before the first row is a normal HTTP error. After rows were written the status is already 200, and the error
        // goes in the last line instead.
        if (request.statusOnly() == false
            && metadata.status() == StreamMetadata.Status.FAILED
            && pages.pages().isEmpty()
            && metadata.completionInfo() != null
            && metadata.completionInfo().failure() != null) {
            StreamMetadata.StreamFailure failure = metadata.completionInfo().failure();
            return new ElasticsearchStatusException(failure.reason(), failure.status());
        }
        return null;
    }
}

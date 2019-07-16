/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * File chunks are sent/requested sequentially by at most one thread at any time. However, the sender/requestor won't wait for the response
 * before processing the next file chunk request to reduce the recovery time especially on secure/compressed or high latency communication.
 * <p>
 * The sender/requestor can send up to {@code maxConcurrentFileChunks} file chunk requests without waiting for responses. Since the recovery
 * target can receive file chunks out of order, it has to buffer those file chunks in memory and only flush to disk when there's no gap.
 * To ensure the recover target never buffers more than {@code maxConcurrentFileChunks} file chunks, we allow the sender/requestor to send
 * only up to {@code maxConcurrentFileChunks} file chunk requests from the last flushed (and acknowledged) file chunk. We leverage the local
 * checkpoint tracker for this purpose. We generate a new sequence number and assign it to each file chunk request before sending; then mark
 * that sequence number as processed when we receive a response for the corresponding file chunk request. With the local checkpoint tracker,
 * we know the last acknowledged-flushed file-chunk is a file chunk whose {@code requestSeqId} equals to the local checkpoint because the
 * recover target can flush all file chunks up to the local checkpoint.
 * <p>
 * When the number of un-replied file chunk requests reaches the limit (i.e. the gap between the max_seq_no and the local checkpoint is
 * greater than {@code maxConcurrentFileChunks}), the sending/requesting thread will abort its execution. That process will be resumed by
 * one of the networking threads which receive/handle the responses of the current pending file chunk requests. This process will continue
 * until all chunk requests are sent/responded.
 */
abstract class MultiFileTransfer<Request extends MultiFileTransfer.ChunkRequest> implements Closeable {
    private Status status = Status.PROCESSING;
    private final Logger logger;
    private final ActionListener<Void> listener;
    private final LocalCheckpointTracker requestSeqIdTracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final AsyncIOProcessor<FileChunkResponseItem> processor;
    private final int maxConcurrentFileChunks;
    private StoreFileMetaData currentFile = null;
    private final Iterator<StoreFileMetaData> remainingFiles;
    private Tuple<StoreFileMetaData, Request> readAheadRequest = null;

    protected MultiFileTransfer(Logger logger, ThreadContext threadContext, ActionListener<Void> listener,
                                int maxConcurrentFileChunks, List<StoreFileMetaData> files) {
        this.logger = logger;
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
        this.listener = listener;
        this.processor = new AsyncIOProcessor<>(logger, maxConcurrentFileChunks, threadContext) {
            @Override
            protected void write(List<Tuple<FileChunkResponseItem, Consumer<Exception>>> items) {
                handleItems(items);
            }
        };
        this.remainingFiles = files.iterator();
    }

    public final void start() {
        addItem(UNASSIGNED_SEQ_NO, null, null); // put a dummy item to start the processor
    }

    private void addItem(long requestSeqId, StoreFileMetaData md, Exception failure) {
        processor.put(new FileChunkResponseItem(requestSeqId, md, failure), e -> { assert e == null : e; });
    }

    private void handleItems(List<Tuple<FileChunkResponseItem, Consumer<Exception>>> items) {
        if (status != Status.PROCESSING) {
            assert status == Status.FAILED : "must not receive any response after the transfer was completed";
            // These exceptions will be ignored as we record only the first failure, log them for debugging purpose.
            items.stream().filter(item -> item.v1().failure != null).forEach(item ->
                logger.debug(new ParameterizedMessage("failed to transfer a file chunk request {}", item.v1().md), item.v1().failure));
            return;
        }
        try {
            for (Tuple<FileChunkResponseItem, Consumer<Exception>> item : items) {
                final FileChunkResponseItem resp = item.v1();
                if (resp.requestSeqId == UNASSIGNED_SEQ_NO) {
                    continue; // not an actual item
                }
                requestSeqIdTracker.markSeqNoAsProcessed(resp.requestSeqId);
                if (resp.failure != null) {
                    handleError(resp.md, resp.failure);
                    throw resp.failure;
                }
            }
            while (requestSeqIdTracker.getMaxSeqNo() - requestSeqIdTracker.getProcessedCheckpoint() < maxConcurrentFileChunks) {
                final Tuple<StoreFileMetaData, Request> request = readAheadRequest != null ? readAheadRequest : getNextRequest();
                readAheadRequest = null;
                if (request == null) {
                    assert currentFile == null && remainingFiles.hasNext() == false;
                    if (requestSeqIdTracker.getMaxSeqNo() == requestSeqIdTracker.getProcessedCheckpoint()) {
                        onCompleted(null);
                    }
                    return;
                }
                final long requestSeqId = requestSeqIdTracker.generateSeqNo();
                sendChunkRequest(request.v2(), ActionListener.wrap(
                    r -> addItem(requestSeqId, request.v1(), null),
                    e -> addItem(requestSeqId, request.v1(), e)));
            }
            // While we are waiting for the responses, we can prepare the next request in advance
            // so we can send it immediately when the responses arrive to reduce the transfer time.
            if (readAheadRequest == null) {
                readAheadRequest = getNextRequest();
            }
        } catch (Exception e) {
            onCompleted(e);
        }
    }

    private void onCompleted(Exception failure) {
        if (Assertions.ENABLED && status != Status.PROCESSING) {
            throw new AssertionError("invalid status: expected [" + Status.PROCESSING + "] actual [" + status + "]", failure);
        }
        status = failure == null ? Status.SUCCESS : Status.FAILED;
        try {
            IOUtils.close(failure, this);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(null);
    }

    private Tuple<StoreFileMetaData, Request> getNextRequest() throws Exception {
        try {
            if (currentFile == null) {
                if (remainingFiles.hasNext()) {
                    currentFile = remainingFiles.next();
                    onNewFile(currentFile);
                } else {
                    return null;
                }
            }
            final StoreFileMetaData md = currentFile;
            final Request request = nextChunkRequest(md);
            if (request.lastChunk()) {
                currentFile = null;
            }
            return Tuple.tuple(md, request);
        } catch (Exception e) {
            handleError(currentFile, e);
            throw e;
        }
    }

    /**
     * This method is called when starting sending/requesting a new file. Subclasses should override
     * this method to reset the file offset or close the previous file and open a new file if needed.
     */
    protected abstract void onNewFile(StoreFileMetaData md) throws IOException;

    protected abstract Request nextChunkRequest(StoreFileMetaData md) throws IOException;

    protected abstract void sendChunkRequest(Request request, ActionListener<Void> listener);

    protected abstract void handleError(StoreFileMetaData md, Exception e) throws Exception;

    private static class FileChunkResponseItem {
        final long requestSeqId;
        final StoreFileMetaData md;
        final Exception failure;

        FileChunkResponseItem(long requestSeqId, StoreFileMetaData md, Exception failure) {
            this.requestSeqId = requestSeqId;
            this.md = md;
            this.failure = failure;
        }
    }

    protected interface ChunkRequest {
        /**
         * @return {@code true} if this chunk request is the last chunk of the current file
         */
        boolean lastChunk();
    }

    private enum Status {
        PROCESSING,
        SUCCESS,
        FAILED
    }
}

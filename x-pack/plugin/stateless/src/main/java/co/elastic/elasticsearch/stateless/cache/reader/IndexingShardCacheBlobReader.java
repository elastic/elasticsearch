/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.cache.reader;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * A {@link CacheBlobReader} that fetches page-aligned data from the indexing node. May throw
 * {@link org.elasticsearch.ResourceNotFoundException} exception if the commit has been uploaded to the object store in the meantime.
 *
 * The chunk size argument is only used to round down the beginning of a range. The end of the range is always rounded up to the next page.
 */
public class IndexingShardCacheBlobReader implements CacheBlobReader {

    private static final Logger logger = LogManager.getLogger(IndexingShardCacheBlobReader.class);

    private final ShardId shardId;
    private final PrimaryTermAndGeneration bccTermAndGen;
    private final String preferredNodeId;
    private final Client client;
    private final long chunkSizeBytes;
    private final ExecutorService fillVBCCExecutor;

    public IndexingShardCacheBlobReader(
        ShardId shardId,
        PrimaryTermAndGeneration bccTermAndGen,
        String preferredNodeId,
        Client client,
        ByteSizeValue chunkSize,
        ThreadPool threadPool
    ) {
        this.shardId = shardId;
        this.bccTermAndGen = bccTermAndGen;
        this.preferredNodeId = preferredNodeId;
        this.client = client;
        this.chunkSizeBytes = chunkSize.getBytes();
        this.fillVBCCExecutor = threadPool.executor(Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL);
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        assert length <= remainingFileLength : length + " > " + remainingFileLength;
        long start = BlobCacheUtils.roundDownToAlignedSize(position, chunkSizeBytes);
        // It is important that the end is only rounded up to a position that is not past the last CC.
        // This is an important property for the ability to append blobs to a VBCC.
        //
        // Hence we use the minimum of the next chunk position or the current file's end position
        // (rounded to page aligned, which is ok due to padding).
        long chunkEnd = BlobCacheUtils.roundUpToAlignedSize(position + length, chunkSizeBytes);
        long fileEnd = BlobCacheUtils.toPageAlignedSize(position + remainingFileLength);
        long end = Math.min(chunkEnd, fileEnd);
        return ByteRange.of(start, end);
    }

    @Override
    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
        assert Objects.equals(EsExecutors.executorName(Thread.currentThread()), Stateless.SHARD_READ_THREAD_POOL) == false
            : Thread.currentThread().getName() + " is a shard read thread";
        getVirtualBatchedCompoundCommitChunk(bccTermAndGen, position, length, preferredNodeId, ActionListener.wrap(rbr -> {
            // The InboundHandler decrements the GetVirtualBatchedCompoundCommitChunkResponse (and thus the data). So we need to retain the
            // data, which is later decrementing in the close function of the getRangeInputStream()'s InputStream.
            ReleasableBytesReference reference = rbr.retain();
            final var streamInput = new FilterStreamInput(reference.streamInput()) {

                private volatile boolean closed;

                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        reference.decRef();
                        closed = true;
                    }
                }
            };
            fillVBCCExecutor.execute(
                ActionRunnable.supply(ActionListener.runAfter(listener, () -> { assert streamInput.closed; }), () -> streamInput)
            );
        }, originalException -> {
            // It is possible that the executor for the failure path is the same as the one waiting for the future. This can happen
            // when the action fails locally without the request being sent out.
            // Complete exceptionally also with the dedicate executor to (1) avoid completing a future on the same thread pool and
            // (2) potentially allow more future processing on the failure path so it does not hog a transport thread
            fillVBCCExecutor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    listener.onFailure(originalException);
                }

                @Override
                public void onFailure(Exception e) {
                    originalException.addSuppressed(e);
                    listener.onFailure(originalException);
                }
            });
        }));
    }

    /**
     * Fetches a chunk of data from the indexing node. The returned {@link ReleasableBytesReference} is incref'ed and must be released
     * by the consumer.
     */
    // protected for testing override
    protected void getVirtualBatchedCompoundCommitChunk(
        final PrimaryTermAndGeneration virtualBccTermAndGen,
        final long offset,
        final int length,
        final String preferredNodeId,
        final ActionListener<ReleasableBytesReference> listener
    ) {
        GetVirtualBatchedCompoundCommitChunkRequest request = new GetVirtualBatchedCompoundCommitChunkRequest(
            shardId,
            virtualBccTermAndGen.primaryTerm(),
            virtualBccTermAndGen.generation(),
            offset,
            length,
            preferredNodeId
        );
        client.execute(TransportGetVirtualBatchedCompoundCommitChunkAction.TYPE, request, listener.map(r -> r.getData()));
    }

    @Override
    public String toString() {
        return "IndexingShardCacheBlobReader{"
            + "shardId="
            + shardId
            + ", bccTermAndGen="
            + bccTermAndGen
            + ", preferredNodeId='"
            + preferredNodeId
            + "'}";
    }
}

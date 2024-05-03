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

import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link CacheBlobReader} that fetches page-aligned data from the indexing node. May throw
 * {@link org.elasticsearch.ResourceNotFoundException} exception if the
 * fetch needs to be tried from the blob store.
 *
 * The chunk size argument is only used to round down the beginning of a range. The end of the range is always rounded up to the next page.
 */
public class IndexingShardCacheBlobReader implements CacheBlobReader {

    private final ShardId shardId;
    private final PrimaryTermAndGeneration bccTermAndGen;
    private final Client client;
    private final long chunkSizeBytes;

    public IndexingShardCacheBlobReader(ShardId shardId, PrimaryTermAndGeneration bccTermAndGen, Client client, ByteSizeValue chunkSize) {
        this.shardId = shardId;
        this.bccTermAndGen = bccTermAndGen;
        this.client = client;
        this.chunkSizeBytes = chunkSize.getBytes();
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
    public InputStream getRangeInputStream(long position, int length) throws IOException {
        // TODO ideally do not use ShardReadThread pool here, do it in-thread. (ES-8155)
        PlainActionFuture<ReleasableBytesReference> bytesFuture = new PlainActionFuture<>();
        getVirtualBatchedCompoundCommitChunk(bccTermAndGen, position, length, bytesFuture.map(r -> r.retain()));
        ReleasableBytesReference reference = FutureUtils.get(bytesFuture);
        return new FilterStreamInput(reference.streamInput()) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    reference.decRef();
                }
            }
        };
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
        final ActionListener<ReleasableBytesReference> listener
    ) {
        GetVirtualBatchedCompoundCommitChunkRequest request = new GetVirtualBatchedCompoundCommitChunkRequest(
            shardId,
            virtualBccTermAndGen.primaryTerm(),
            virtualBccTermAndGen.generation(),
            offset,
            length
        );
        // The InboundHandler decrements the GetVirtualBatchedCompoundCommitChunkResponse (and thus the data). So we need to retain the
        // data, which is later decrementing in the close function of the getRangeInputStream()'s InputStream.
        client.execute(TransportGetVirtualBatchedCompoundCommitChunkAction.TYPE, request, listener.map(r -> r.getData()));
    }
}

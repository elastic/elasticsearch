/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.OptionalLong;

/**
 * A {@link MergePolicy.OneMerge} that records the size of the merged segment as soon as the merge is committed.
 * <p>
 * {@link SegmentCommitInfo#sizeInBytes()} is cached without synchronization and the cache is reset whenever the segment's generation
 * advances (e.g. deletes or doc-values updates being written). Once a merge has been committed its segment is visible to the
 * {@link org.apache.lucene.index.IndexWriter}, which mutates it under its own lock, so reading the size afterwards from the merge
 * thread can observe {@code -1}, a stale value or a partially updated file set. {@link #mergeFinished} is invoked while the
 * {@code IndexWriter} lock is held and before any other thread can touch the new segment, making it the one safe point to read the size.
 * <p>
 * The recorded value is a snapshot of the size at commit time, which is what merge metrics want (the bytes this merge produced); it does
 * not reflect later deletes or doc-values updates to the segment.
 * <p>
 * {@link ThreadPoolMergeScheduler} requires the merges it runs to be of this type, so any {@link InternalEngine#wrapMergePolicy} override
 * must produce merges that extend it.
 */
public class SizeTrackingOneMerge extends MergePolicy.OneMerge {
    private static final Logger logger = LogManager.getLogger(SizeTrackingOneMerge.class);

    private final MergePolicy.OneMerge wrapped;
    private volatile OptionalLong mergedSegmentSizeInBytes = OptionalLong.empty();

    public SizeTrackingOneMerge(MergePolicy.OneMerge wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
        return wrapped.wrapForMerge(reader);
    }

    @Override
    public void mergeFinished(boolean success, boolean segmentDropped) throws IOException {
        super.mergeFinished(success, segmentDropped);
        SegmentCommitInfo mergeInfo = getMergeInfo();
        if (success && segmentDropped) {
            // The merged segment was fully deleted and dropped, so the merge produced no bytes.
            mergedSegmentSizeInBytes = OptionalLong.of(0L);
        } else if (success && mergeInfo != null) {
            try {
                mergedSegmentSizeInBytes = OptionalLong.of(mergeInfo.sizeInBytes());
            } catch (IOException e) {
                // The size is only used for metrics, so don't fail the merge; callers fall back to the estimate.
                logger.debug(() -> Strings.format("failed to read size of merged segment [%s], falling back to estimate", mergeInfo), e);
            }
        }
    }

    /**
     * Failed and aborted merges deliberately report empty rather than {@code 0}, so that {@code 0} unambiguously means "the merge produced
     * no bytes". Callers are not expected to query the size of a merge that did not complete successfully.
     *
     * @return the size of the merged segment ({@code 0} if it was dropped because all its documents were deleted), or empty if the merge
     * did not succeed or the size of the committed segment could not be read
     */
    public OptionalLong mergedSegmentSizeInBytes() {
        return mergedSegmentSizeInBytes;
    }
}

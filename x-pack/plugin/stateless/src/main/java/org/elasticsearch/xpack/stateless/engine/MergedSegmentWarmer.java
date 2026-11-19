/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.core.Strings.format;

/// The warmer that Lucene runs on the merge thread for each merged segment, before the merge is committed to the index writer, which tells
/// a [MergedSegmentListener] about the segment.
///
/// Lucene's warmer is synchronous: the merge is not committed until [#warm] returns, so it waits for the listener. This holds the merge
/// thread, and the slot of the merge in the merge thread pool, until the listener is completed. The listener is asynchronous to let an
/// implementation that does not need the merge thread to wait stop doing so without changing its callers.
///
/// What the listener does is best effort: if it fails, the failure is logged and the merge continues, as a failure of the warmer would fail
/// the merge, and with it the engine. The segment is then committed like any other. If the merge thread is interrupted while waiting,
/// the merge is aborted instead, since the listener may still be pending.
///
/// The index writer is created while the engine is being constructed, before the fields of the engine are set, so the warmer cannot refer
/// to the engine: it is created with the engine configuration.
public class MergedSegmentWarmer implements IndexWriter.IndexReaderWarmer {

    private static final Logger logger = LogManager.getLogger(MergedSegmentWarmer.class);

    private final ShardId shardId;
    private final MergedSegmentListener listener;
    // the running merges and the waits of their merge threads, by id of the merged segment
    private final Map<String, MergePolicy.OneMerge> merges = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PlainActionFuture<Void>> waiting = ConcurrentCollections.newConcurrentMap();

    public MergedSegmentWarmer(ShardId shardId, MergedSegmentListener listener) {
        this.shardId = Objects.requireNonNull(shardId);
        this.listener = Objects.requireNonNull(listener);
    }

    /// Registers the merge that produces the segment, before the segment is warmed.
    public void onMergeStarted(SegmentCommitInfo mergedSegment, MergePolicy.OneMerge merge) {
        final var previous = merges.put(segmentId(mergedSegment.info), merge);
        // the index writer sets the merged segment of a merge more than once
        assert previous == null || previous == merge : "segment [" + mergedSegment.info.name + "] is already merged by " + previous;
    }

    public void onMergeFinished(SegmentCommitInfo mergedSegment, MergePolicy.OneMerge merge) {
        final var removed = merges.remove(segmentId(mergedSegment.info));
        assert removed == merge : "segment [" + mergedSegment.info.name + "] was tracked for " + removed + " instead of " + merge;
    }

    /// Whether the warmer still tracks any merge, or any merge thread waiting for the listener.
    boolean hasTrackedMerges() {
        return merges.isEmpty() == false || waiting.isEmpty() == false;
    }

    /// Called when the merge is aborted, e.g. when the index writer is rolled back on engine close. The merge thread stops waiting
    /// for the listener, which may still be pending, and the merge is aborted.
    public void onMergeAborted(SegmentCommitInfo mergedSegment) {
        final var future = waiting.get(segmentId(mergedSegment.info));
        if (future != null) {
            future.onFailure(mergeAborted(mergedSegment.info.name));
        }
    }

    // segment names can be reused across index writers, ids are unique
    private static String segmentId(SegmentInfo segmentInfo) {
        return StringHelper.idToString(segmentInfo.getId());
    }

    private MergePolicy.MergeAbortedException mergeAborted(String segmentName) {
        return new MergePolicy.MergeAbortedException(format("%s merge of segment [%s] aborted", shardId, segmentName));
    }

    @Override
    public void warm(LeafReader reader) throws IOException {
        final var segment = Lucene.segmentReader(reader);
        final var segmentName = segment.getSegmentName();
        final var segmentId = segmentId(segment.getSegmentInfo().info);
        final var merge = merges.get(segmentId);
        final BooleanSupplier isAborted = merge != null ? merge::isAborted : () -> false;
        final var future = new PlainActionFuture<Void>();
        final var previous = waiting.put(segmentId, future);
        assert previous == null : "segment [" + segmentName + "] is already being warmed";
        try {
            // checked once waiting is registered, so an abort either is seen here or fails the future
            if (isAborted.getAsBoolean()) {
                throw mergeAborted(segmentName);
            }
            listener.onNewSegmentMerged(segment, isAborted, future);
            // TODO: bound the wait with a timeout
            future.get();
        } catch (MergePolicy.MergeAbortedException e) {
            throw e;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MergePolicy.MergeAbortedException mergeAborted) {
                throw mergeAborted;
            }
            logger.warn(
                () -> format("%s failed to process the merged segment [%s], continuing with the merge", shardId, segmentName),
                e.getCause()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // the listener may still be pending, so the segment must not be committed as if it was completed
            final var aborted = new MergePolicy.MergeAbortedException(
                format("%s interrupted while waiting for the listener of the merged segment [%s]", shardId, segment.getSegmentName())
            );
            aborted.addSuppressed(e);
            throw aborted;
        } catch (Exception e) {
            logger.warn(() -> format("%s failed to process the merged segment [%s], continuing with the merge", shardId, segmentName), e);
        } finally {
            final var removed = waiting.remove(segmentId);
            assert removed == future : "segment [" + segmentName + "] was not being warmed";
        }
    }
}

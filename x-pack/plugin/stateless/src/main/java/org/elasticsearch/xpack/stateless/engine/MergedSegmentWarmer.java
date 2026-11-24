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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.core.Strings.format;

/// The warmer that Lucene runs on the merge thread for each merged segment, before the merge is committed to the index writer, which tells
/// a [MergedSegmentListener] about the segment.
///
/// Lucene's warmer is synchronous: the merge is not committed until [#warm] returns, so it waits for the listener. This holds the merge
/// thread, and the slot of the merge in the merge thread pool, until the listener is completed. The listener is asynchronous to let an
/// implementation that does not need the merge thread to wait stop doing so without changing its callers.
///
/// A failure of the listener is passed on as is, so that the engine handles it, and a failure that is not a
/// [MergePolicy.MergeAbortedException] fails the merge, and with it the engine, as the segment was not processed as expected. A
/// [MergePolicy.MergeAbortedException] aborts the merge instead, and so does an interrupt of the merge thread while it waits, since the
/// listener may still be pending. An aborted merge is discarded, and does not fail the engine.
///
/// Aborting the merge, e.g. when the engine is closed, does not stop the wait: the merge thread keeps waiting until the listener is
/// completed, and closing the engine waits for it. The listener is told whether the merge is aborted, to stop early if it can. A merge
/// that is already aborted when its segment is warmed is not passed to the listener.
public class MergedSegmentWarmer implements IndexWriter.IndexReaderWarmer {

    private final ShardId shardId;
    private final MergedSegmentListener listener;
    // the running merges by id of the merged segment
    private final Map<String, MergePolicy.OneMerge> merges = ConcurrentCollections.newConcurrentMap();

    public MergedSegmentWarmer(ShardId shardId, MergedSegmentListener listener) {
        this.shardId = Objects.requireNonNull(shardId);
        this.listener = Objects.requireNonNull(listener);
    }

    /// Registers the merge that produces the segment, before the segment is warmed.
    public void onMergeStarted(SegmentCommitInfo mergedSegment, MergePolicy.OneMerge merge) {
        assert assertOnMergeThread();
        final var previous = merges.put(segmentId(mergedSegment.info), merge);
        // the index writer sets the merged segment of a merge more than once
        assert previous == null || previous == merge : "segment [" + mergedSegment.info.name + "] is already merged by " + previous;
    }

    /// Stops tracking the merge that produced the segment, whether the merge succeeded, failed or was aborted.
    public void onMergeFinished(SegmentCommitInfo mergedSegment, MergePolicy.OneMerge merge) {
        assert assertOnMergeThread();
        final var removed = merges.remove(segmentId(mergedSegment.info));
        assert removed == merge : "segment [" + mergedSegment.info.name + "] was tracked for " + removed + " instead of " + merge;
    }

    /// Whether the warmer still tracks any merge.
    boolean hasTrackedMerges() {
        return merges.isEmpty() == false;
    }

    // merges run on the merge thread pool, or on the threads of Lucene's own scheduler if the thread pool scheduler is disabled
    private static boolean assertOnMergeThread() {
        return Thread.currentThread().getName().contains("Lucene Merge Thread")
            || ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MERGE);
    }

    // segment names can be reused across index writers, ids are unique
    private static String segmentId(SegmentInfo segmentInfo) {
        return StringHelper.idToString(segmentInfo.getId());
    }

    @Override
    public void warm(LeafReader reader) throws IOException {
        final var segment = Lucene.segmentReader(reader);
        final var segmentName = segment.getSegmentName();
        final var segmentId = segmentId(segment.getSegmentInfo().info);
        final var merge = merges.get(segmentId);
        assert merge != null : "no merge tracked for segment [" + segmentName + "]";
        // an aborted merge is discarded by the index writer, so there is nothing to tell the listener about
        if (merge.isAborted()) {
            return;
        }
        try {
            final var future = new PlainActionFuture<Void>();
            listener.onNewSegmentMerged(segment, merge::isAborted, future);
            // TODO: bound the wait with a timeout
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // the listener may still be pending, so the segment must not be committed as if it was completed
            // and the listener must see that the merge is aborted
            merge.setAborted();
            final var aborted = new MergePolicy.MergeAbortedException(
                format("%s interrupted while waiting for the listener of the merged segment [%s]", shardId, segmentName)
            );
            aborted.addSuppressed(e);
            throw aborted;
        } catch (ExecutionException e) {
            final var failure = e.getCause();
            // a MergeAbortedException, which is an IOException, aborts the merge, any other failure fails it
            if (failure instanceof IOException ioException) {
                throw ioException;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException(failure);
        }
    }
}

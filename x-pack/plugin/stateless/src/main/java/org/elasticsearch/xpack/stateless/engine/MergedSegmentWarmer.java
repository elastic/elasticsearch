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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
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

    public MergedSegmentWarmer(ShardId shardId, MergedSegmentListener listener) {
        this.shardId = Objects.requireNonNull(shardId);
        this.listener = Objects.requireNonNull(listener);
    }

    @Override
    public void warm(LeafReader reader) throws IOException {
        final var segment = Lucene.segmentReader(reader);
        try {
            final var future = new PlainActionFuture<Void>();
            listener.onNewSegmentMerged(segment, future);
            // TODO: bound the wait with a timeout
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // the listener may still be pending, so the segment must not be committed as if it was completed
            final var aborted = new MergePolicy.MergeAbortedException(
                format("%s interrupted while waiting for the listener of the merged segment [%s]", shardId, segment.getSegmentName())
            );
            aborted.addSuppressed(e);
            throw aborted;
        } catch (Exception e) {
            final Throwable failure = e instanceof ExecutionException ? e.getCause() : e;
            logger.warn(
                () -> format("%s failed to process the merged segment [%s], continuing with the merge", shardId, segment.getSegmentName()),
                failure
            );
        }
    }
}

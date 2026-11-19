/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.action.ActionListener;

import java.util.function.BooleanSupplier;

/// Notified of each segment that a merge of an index shard produced, before the merge is committed to the index writer.
///
/// The notification happens on the merge thread, which waits for the listener to be completed. Until then the merged segment cannot be
/// part of any commit, so an implementation completes the listener only once whatever must happen before the segment can be published has
/// happened. This is a notification, not a guarantee that the segment is published: the merge can still be aborted or fail after the
/// listener is completed, and its segment is then discarded.
///
/// What a listener does is best effort: a failure does not fail the merge, it is logged and the segment is published like any other. An
/// implementation must therefore treat what it was to do for a segment as not done if it fails, and must not rely on the merge failing.
/// It reports a failure by failing the listener, never by throwing: a throw is treated as a failure, so any work it started would then
/// run after the segment is released.
///
/// If the merge thread is interrupted while it waits, the merge is aborted and the segment is released although the listener is still
/// pending: the listener must still be completed, and the segment must not be used after it is released.
///
/// The merge thread, and with it the slot of the merge in the merge thread pool, is held until the listener is completed. An
/// implementation must complete it promptly, on an executor other than the one of the merge thread, and without needing locks that are
/// held by a thread that is closing the engine or the shard.
///
/// The listener must be completed eventually, in particular when the engine or the shard is closed while it is pending: the index writer
/// waits for the running merges before it closes, so a listener that is never completed blocks the closing of the engine and of the shard.
@FunctionalInterface
public interface MergedSegmentListener {

    /// A listener that does nothing.
    MergedSegmentListener NOOP = (segment, isAborted, listener) -> listener.onResponse(null);

    /// @param segment   the reader of the segment produced by the merge, whose files are complete. It must not be retained.
    /// @param isAborted whether the merge is aborted, e.g. because the engine is closing. The listener must still be completed.
    /// @param listener  completed once the segment can be committed, or with the failure if what had to happen first could not
    void onNewSegmentMerged(SegmentReader segment, BooleanSupplier isAborted, ActionListener<Void> listener);
}

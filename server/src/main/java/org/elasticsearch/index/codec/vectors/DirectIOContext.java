/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;

import java.util.Set;

/**
 * An {@link IOContext} carrying the {@link DirectIOHint}, so that {@code HybridDirectory} opens or creates the raw
 * vector file with direct I/O: searches reading the raw vectors of a field with {@code on_disk_rescore}
 * ({@link #searchRead}), merges reading the raw vectors of a segment written with {@code on_disk_merge}
 * ({@link #mergeRead}), and merges writing them ({@link #mergeWrite}).
 * <p>
 * The read-side contexts are built from the search-time state, so there is no merge to describe: their
 * {@link #mergeInfo()} is null, and the MERGE type of {@link #mergeRead} is there to select the merge-sized direct I/O
 * delegate in {@code HybridDirectory}, which serves raw vector files alone. Lucene's own
 * {@code DirectIODirectory#useDirectIO} would dereference the merge info, but {@code AlwaysDirectIODirectory} overrides
 * that method and never reads it. The write-side context preserves the {@code MERGE} type, which merge I/O rate limiting
 * requires: {@code ConcurrentMergeScheduler#wrapForMerge} asserts that every write during a merge carries a merge context.
 */
public final class DirectIOContext implements IOContext {

    private final Context context;
    private final MergeInfo mergeInfo;
    private final FlushInfo flushInfo;
    private final Set<FileOpenHint> hints;

    private DirectIOContext(Context context, MergeInfo mergeInfo, FlushInfo flushInfo, Set<FileOpenHint> hints) {
        this.context = context;
        this.mergeInfo = mergeInfo;
        this.flushInfo = flushInfo;
        // always add DirectIOHint to the hints given
        this.hints = Set.copyOf(Sets.union(hints, Set.of(DirectIOHint.INSTANCE)));
    }

    public static DirectIOContext searchRead(Set<FileOpenHint> hints) {
        return new DirectIOContext(Context.DEFAULT, null, null, hints);
    }

    public static DirectIOContext mergeRead(Set<FileOpenHint> hints) {
        return new DirectIOContext(Context.MERGE, null, null, hints);
    }

    /** The merge's own context with the direct I/O hint added. Copied, not wrapped: Lucene's merge context ignores {@code withHints}. */
    public static DirectIOContext mergeWrite(IOContext mergeContext) {
        assert mergeContext.context() == Context.MERGE : "expected a merge context, got " + mergeContext.context();
        return new DirectIOContext(mergeContext.context(), mergeContext.mergeInfo(), mergeContext.flushInfo(), mergeContext.hints());
    }

    @Override
    public Context context() {
        return context;
    }

    @Override
    public MergeInfo mergeInfo() {
        return mergeInfo;
    }

    @Override
    public FlushInfo flushInfo() {
        return flushInfo;
    }

    @Override
    public Set<FileOpenHint> hints() {
        return hints;
    }

    /** Keeps the direct I/O hint whatever hints replace the others. */
    @Override
    public IOContext withHints(FileOpenHint... hints) {
        return new DirectIOContext(context, mergeInfo, flushInfo, Set.of(hints));
    }
}

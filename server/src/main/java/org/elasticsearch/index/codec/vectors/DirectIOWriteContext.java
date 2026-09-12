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
 * An {@link IOContext} for creating raw vector files with direct I/O during a merge, so that
 * streaming the merged raw vectors to disk does not evict hotter data from the page cache.
 * <p>
 * Unlike {@link DirectIOCapableFlatVectorsFormat.DirectIOContext}, which represents a
 * {@link Context#DEFAULT} read context, this context delegates {@link #context()},
 * {@link #mergeInfo()} and {@link #flushInfo()} to the merge context it wraps. Preserving the
 * {@link Context#MERGE} context type is required for merge I/O rate limiting
 * ({@code ConcurrentMergeScheduler#wrapForMerge} asserts that every write during a merge has a
 * merge context) and for anything else keyed on the context type. The {@link DirectIOHint} is
 * sticky: it survives {@link #withHints} replacing the other hints. The
 * merge-ness of the context is carried by {@link #context()} delegating to the wrapped merge
 * context, which is how the read side routes too, so no separate merge marker is needed.
 */
public final class DirectIOWriteContext implements IOContext {

    private static final Set<FileOpenHint> STICKY_HINTS = Set.of(DirectIOHint.INSTANCE);

    private final IOContext delegate;
    private final Set<FileOpenHint> hints;

    public DirectIOWriteContext(IOContext delegate) {
        this(delegate, Sets.union(delegate.hints(), STICKY_HINTS));
    }

    private DirectIOWriteContext(IOContext delegate, Set<FileOpenHint> hints) {
        assert delegate.context() == Context.MERGE : "expected a merge context, got " + delegate.context();
        this.delegate = delegate;
        this.hints = hints;
    }

    @Override
    public Context context() {
        return delegate.context();
    }

    @Override
    public MergeInfo mergeInfo() {
        return delegate.mergeInfo();
    }

    @Override
    public FlushInfo flushInfo() {
        return delegate.flushInfo();
    }

    @Override
    public Set<FileOpenHint> hints() {
        return hints;
    }

    @Override
    public IOContext withHints(FileOpenHint... hints) {
        // do not call delegate.withHints(): Lucene's built-in merge contexts may not support it,
        // and the delegate is only carried for context()/mergeInfo(). The sticky hints are
        // always re-added.
        return new DirectIOWriteContext(delegate, Sets.union(Set.of(hints), STICKY_HINTS));
    }
}

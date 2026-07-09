/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

/**
 * {@link Releasable} {@link AbstractRefCounted} base for {@link Block}s and {@link Vector}s. Calls to
 * {@link #decRef()} and {@link #close()} are equivalent.
 * <p>
 * {@link AbstractRefCounted}'s reference count is already thread safe. That matters here because
 * a block's underlying data can be shared -- via {@code incRef} -- between sibling pages. These
 * sibling pages can be released concurrently by different threads once dispatched to background
 * workers (e.g. by {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}).
 */
public abstract class AbstractRefCountedReleasable extends AbstractRefCounted implements Releasable {

    @Override
    public final void close() {
        decRef();
    }

    public final boolean isReleased() {
        return hasReferences() == false;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.adjustScale;

/**
 * An iterator that wraps another bucket iterator and adjusts its scale.
 * When scaling down, multiple buckets can collapse into a single one. This iterator ensures they are merged correctly.
 */
final class ScaleAdjustingBucketIterator implements ExponentialHistogram.BucketIterator {

    private final ExponentialHistogram.BucketIterator delegate;
    private final int scaleAdjustment;

    private long currentIndex;
    private long currentCount;
    boolean hasNextValue;

    /**
     * Creates a new scale-adjusting iterator.
     *
     * @param delegate    the iterator to wrap
     * @param targetScale the target scale for the new iterator
     */
    ScaleAdjustingBucketIterator(ExponentialHistogram.BucketIterator delegate, int targetScale) {
        this.delegate = delegate;
        scaleAdjustment = targetScale - delegate.scale();
        hasNextValue = true;
        advance();
    }

    @Override
    public boolean hasNext() {
        return hasNextValue;
    }

    @Override
    public long peekCount() {
        assertEndNotReached();
        return currentCount;
    }

    @Override
    public long peekIndex() {
        assertEndNotReached();
        return currentIndex;
    }

    @Override
    public void advance() {
        assertEndNotReached();
        hasNextValue = delegate.hasNext();
        if (hasNextValue == false) {
            return;
        }
        currentIndex = adjustScale(delegate.peekIndex(), delegate.scale(), scaleAdjustment);
        currentCount = delegate.peekCount();
        delegate.advance();
        while (delegate.hasNext() && adjustScale(delegate.peekIndex(), delegate.scale(), scaleAdjustment) == currentIndex) {
            currentCount += delegate.peekCount();
            delegate.advance();
        }
    }

    private void assertEndNotReached() {
        if (hasNextValue == false) {
            throw new IllegalStateException("Iterator has no more buckets");
        }
    }

    @Override
    public int scale() {
        return delegate.scale() + scaleAdjustment;
    }
}

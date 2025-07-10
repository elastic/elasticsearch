/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.adjustScale;

public final class ScaleAdjustingBucketIterator implements ExponentialHistogram.BucketIterator {

    private final ExponentialHistogram.BucketIterator delegate;
    private final int scaleAdjustment;

    private long currentIndex;
    private long currentCount;
    boolean hasNextValue;

    public ScaleAdjustingBucketIterator(ExponentialHistogram.BucketIterator delegate, int targetScale) {
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
        currentIndex = adjustScale(delegate.peekIndex(), scaleAdjustment);
        currentCount = delegate.peekCount();
        delegate.advance();
        while (delegate.hasNext() && adjustScale(delegate.peekIndex(), scaleAdjustment) == currentIndex) {
            currentCount += delegate.peekCount();
            delegate.advance();
        }
    }

    private void assertEndNotReached() {
        if (hasNextValue == false) {
            throw new IllegalStateException("no more buckets available");
        }
    }

    @Override
    public int scale() {
        return delegate.scale() + scaleAdjustment;
    }

    @Override
    public ExponentialHistogram.BucketIterator copy() {
        throw new UnsupportedOperationException();
    }
}

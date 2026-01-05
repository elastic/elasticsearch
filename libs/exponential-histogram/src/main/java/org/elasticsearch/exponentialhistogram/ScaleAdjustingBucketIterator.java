/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.adjustScale;

/**
 * An iterator that wraps another bucket iterator and adjusts its scale.
 * When scaling down, multiple buckets can collapse into a single one. This iterator ensures they are merged correctly.
 */
final class ScaleAdjustingBucketIterator implements BucketIterator {

    private final BucketIterator delegate;
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
    ScaleAdjustingBucketIterator(BucketIterator delegate, int targetScale) {
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

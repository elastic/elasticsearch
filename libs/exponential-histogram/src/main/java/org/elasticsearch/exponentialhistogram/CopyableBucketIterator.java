/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

/**
 * A {@link BucketIterator} that can be copied.
 */
public interface CopyableBucketIterator extends BucketIterator {

    /**
     * Creates a copy of this bucket iterator, pointing at the same bucket of the same range of buckets.
     * Calling {@link #advance()} on the copied iterator does not affect this instance and vice-versa.
     *
     * @return a copy of this iterator
     */
    CopyableBucketIterator copy();
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

/**
 * Base implementation for {@link LongObjectPagedHashMap} and {@link ObjectObjectPagedHashMap}.
 */
abstract class AbstractObjectHash extends AbstractPagedHashMap {

    AbstractObjectHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
    }

    /** Is the current buckets used */
    protected abstract boolean used(long bucket);

    /** Remove the entry at the given index and add it back */
    protected abstract void removeAndAdd(long index);

    /** Resize to the given capacity. */
    protected abstract void resize(long capacity);

    @Override
    protected final void rehash(long buckets, long newBuckets) {
        // Resize arrays
        resize(newBuckets);
        for (long i = 0; i < buckets; ++i) {
            if (used(i)) {
                removeAndAdd(i);
            }
        }
        // The only entries which have not been put in their final position in the previous loop are those that were stored in a slot that
        // is < slot(key, mask). This only happens when slot(key, mask) returned a slot that was close to the end of the array and collision
        // resolution has put it back in the first slots. This time, collision resolution will have put them at the beginning of the newly
        // allocated slots. Let's re-add them to make sure they are in the right slot. This 2nd loop will typically exit very early.
        for (long i = buckets; i < newBuckets; ++i) {
            if (used(i)) {
                removeAndAdd(i); // add it back
            } else {
                break;
            }
        }
    }
}

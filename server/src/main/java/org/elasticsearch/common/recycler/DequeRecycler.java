/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.recycler;


import java.util.Deque;

/**
 * A {@link Recycler} implementation based on a {@link Deque}. This implementation is NOT thread-safe.
 */
public class DequeRecycler<T> extends AbstractRecycler<T> {

    final Deque<T> deque;
    final int maxSize;

    public DequeRecycler(C<T> c, Deque<T> queue, int maxSize) {
        super(c);
        this.deque = queue;
        this.maxSize = maxSize;
    }

    @Override
    public V<T> obtain() {
        final T v = deque.pollFirst();
        if (v == null) {
            return new DV(c.newInstance(), false);
        }
        return new DV(v, true);
    }

    /** Called before releasing an object, returns true if the object should be recycled and false otherwise. */
    protected boolean beforeRelease() {
        return deque.size() < maxSize;
    }

    /** Called after a release. */
    protected void afterRelease(boolean recycled) {
        // nothing to do
    }

    private class DV implements Recycler.V<T> {

        T value;
        final boolean recycled;

        DV(T value, boolean recycled) {
            this.value = value;
            this.recycled = recycled;
        }

        @Override
        public T v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return recycled;
        }

        @Override
        public void close() {
            if (value == null) {
                throw new IllegalStateException("recycler entry already released...");
            }
            final boolean recycle = beforeRelease();
            if (recycle) {
                c.recycle(value);
                deque.addFirst(value);
            }
            else {
                c.destroy(value);
            }
            value = null;
            afterRelease(recycle);
        }
    }
}

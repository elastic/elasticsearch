/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class QueueRecycler<T> extends Recycler<T> {

    final Queue<T> queue;
    final AtomicInteger size;
    final int maxSize;

    public QueueRecycler(C<T> c, int maxSize) {
        this(c, ConcurrentCollections.<T>newQueue(), maxSize);
    }

    public QueueRecycler(C<T> c, Queue<T> queue, int maxSize) {
        super(c);
        this.queue = queue;
        this.maxSize = maxSize;
        // we maintain size separately because concurrent queue implementations typically have linear-time size() impls
        this.size = new AtomicInteger();
    }

    @Override
    public void close() {
        assert queue.size() == size.get();
        queue.clear();
        size.set(0);
    }

    @Override
    public V<T> obtain(int sizing) {
        final T v = queue.poll();
        if (v == null) {
            return new QV(c.newInstance(sizing), false);
        }
        size.decrementAndGet();
        return new QV(v, true);
    }

    class QV implements Recycler.V<T> {

        T value;
        final boolean recycled;

        QV(T value, boolean recycled) {
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
        public boolean release() {
            if (value == null) {
                throw new ElasticSearchIllegalStateException("recycler entry already released...");
            }
            if (size.incrementAndGet() <= maxSize) {
                c.clear(value);
                queue.offer(value);
            } else {
                size.decrementAndGet();
            }
            value = null;
            return true;
        }
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
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
 */

package org.elasticsearch.gradle.release;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.function.Supplier;

class Spool<T> implements Iterable<T> {
    private final Supplier<Iterable<T>> supplier;
    private final Queue<T> queue;

    public Spool(Supplier<Iterable<T>> supplier) {
        this.supplier = supplier;
        queue = new ArrayDeque<>();
    }

    private void refill() {
        assert this.queue.isEmpty();
        this.supplier.get().forEach(this.queue::add);
    }

    @Override
    public Iterator<T> iterator() {
        return new SpoolIterator();
    }

    public class SpoolIterator implements Iterator<T> {

        @Override
        public boolean hasNext() {
            if (queue.isEmpty()) {
                refill();
            }

            return queue.isEmpty() == false;
        }

        @Override
        public T next() {
            return queue.poll();
        }
    }
}

/* @notice
 * Copyright (C) 2012 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */

package org.elasticsearch.common.collect;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

/**
 * An {@code EvictingQueue} is a non-blocking queue which is limited to a maximum size; when new elements are added to a
 * full queue, elements are evicted from the head of the queue to accommodate the new elements.
 *
 * @param <T> The type of elements in the queue.
 */
public class EvictingQueue<T> implements Queue<T> {
    private final int maximumSize;
    private final ArrayDeque<T> queue;

    /**
     * Construct a new {@code EvictingQueue} that holds {@code maximumSize} elements.
     *
     * @param maximumSize The maximum number of elements that the queue can hold
     * @throws IllegalArgumentException if {@code maximumSize} is less than zero
     */
    public EvictingQueue(int maximumSize) {
        if (maximumSize < 0) {
            throw new IllegalArgumentException("maximumSize < 0");
        }
        this.maximumSize = maximumSize;
        this.queue = new ArrayDeque<>(maximumSize);
    }

    /**
     * @return the number of additional elements that the queue can accommodate before evictions occur
     */
    public int remainingCapacity() {
        return this.maximumSize - this.size();
    }

    /**
     * Add the given element to the queue, possibly forcing an eviction from the head if {@link #remainingCapacity()} is
     * zero.
     *
     * @param t the element to add
     * @return true if the element was added (always the case for {@code EvictingQueue}
     */
    @Override
    public boolean add(T t) {
        if (maximumSize == 0) {
            return true;
        }
        if (queue.size() == maximumSize) {
            queue.remove();
        }
        queue.add(t);
        return true;
    }

    /**
     * @see #add(Object)
     */
    @Override
    public boolean offer(T t) {
        return add(t);
    }

    @Override
    public T remove() {
        return queue.remove();
    }


    @Override
    public T poll() {
        return queue.poll();
    }

    @Override
    public T element() {
        return queue.element();
    }

    @Override
    public T peek() {
        return queue.peek();
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return queue.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return queue.iterator();
    }

    @Override
    public Object[] toArray() {
        return queue.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return queue.toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        return queue.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return queue.containsAll(c);
    }

    /**
     * Add the given elements to the queue, possibly forcing evictions from the head if {@link #remainingCapacity()} is
     * zero or becomes zero during the execution of this method.
     *
     * @param c the collection of elements to add
     * @return true if any elements were added to the queue
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        boolean modified = false;
        for (T e : c)
            if (add(e))
                modified = true;
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return queue.retainAll(c);
    }

    @Override
    public void clear() {
        queue.clear();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A priority queue maintains a partial ordering of its elements such that the least element can
 * always be found in constant time. Put()'s and pop()'s require log(size) but the remove()
 * cost implemented here is linear.
 *
 * <p><b>NOTE</b>: Iteration order is not specified.
 *
 * Based in lucene's {@link org.apache.lucene.util.PriorityQueue} but it uses a {@link ObjectArray} instead of plain {code Object[]}.
 * This class only track the {@link ObjectArray} and not the memory usage of the elements. Furthermore,
 * the elements are not closed even if they implement {@link Releasable}.
 */
public abstract class ObjectArrayPriorityQueue<T> implements Iterable<T>, Releasable {
    private long size = 0;
    private final long maxSize;
    // package private for testing
    final ObjectArray<T> heap;

    /**
     * Create a priority queue.
     */
    public ObjectArrayPriorityQueue(long maxSize, BigArrays bigArrays) {
        final long heapSize;
        if (0 == maxSize) {
            // We allocate 1 extra to avoid if statement in top()
            heapSize = 2;
        } else {
            if ((maxSize < 0) || (maxSize >= Long.MAX_VALUE)) {
                // Throw exception to prevent confusing OOME:
                throw new IllegalArgumentException("maxSize must be >= 0 and < " + (Long.MAX_VALUE) + "; got: " + maxSize);
            }
            // NOTE: we add +1 because all access to heap is
            // 1-based not 0-based. heap[0] is unused.
            heapSize = maxSize + 1;
        }

        this.heap = bigArrays.newObjectArray(heapSize);
        this.maxSize = maxSize;
    }

    /**
     * Determines the ordering of objects in this priority queue. Subclasses must define this one
     * method.
     *
     * @return <code>true</code> iff parameter <code>a</code> is less than parameter <code>b</code>.
     */
    protected abstract boolean lessThan(T a, T b);

    /**
     * Adds an Object to a PriorityQueue in log(size) time. If one tries to add more objects than
     * maxSize from initialize an {@link ArrayIndexOutOfBoundsException} is thrown.
     *
     * @return the new 'top' element in the queue.
     */
    public final T add(T element) {
        // don't modify size until we know heap access didn't throw AIOOB.
        long index = size + 1;
        heap.set(index, element);
        size = index;
        upHeap(index);
        return heap.get(1);
    }

    /**
     * Adds all elements of the collection into the queue. This method should be preferred over
     * calling {@link #add(Object)} in loop if all elements are known in advance as it builds queue
     * faster.
     *
     * <p>If one tries to add more objects than the maxSize passed in the constructor, an {@link
     * ArrayIndexOutOfBoundsException} is thrown.
     */
    public void addAll(Collection<T> elements) {
        if (this.size + elements.size() > this.maxSize) {
            throw new ArrayIndexOutOfBoundsException(
                "Cannot add " + elements.size() + " elements to a queue with remaining capacity: " + (maxSize - size)
            );
        }

        // Heap with size S always takes first S elements of the array,
        // and thus it's safe to fill array further - no actual non-sentinel value will be overwritten.
        Iterator<T> iterator = elements.iterator();
        while (iterator.hasNext()) {
            this.heap.set(size + 1, iterator.next());
            this.size++;
        }

        // The loop goes down to 1 as heap is 1-based not 0-based.
        for (long i = (size >>> 1); i >= 1; i--) {
            downHeap(i);
        }
    }

    /**
     * Adds an Object to a PriorityQueue in log(size) time. It returns the object (if any) that was
     * dropped off the heap because it was full. This can be the given parameter (in case it is
     * smaller than the full heap's minimum, and couldn't be added), or another object that was
     * previously the smallest value in the heap and now has been replaced by a larger one, or null if
     * the queue wasn't yet full with maxSize elements.
     */
    public T insertWithOverflow(T element) {
        if (size < maxSize) {
            add(element);
            return null;
        } else if (size > 0 && lessThan(heap.get(1), element)) {
            T ret = heap.get(1);
            heap.set(1, element);
            updateTop();
            return ret;
        } else {
            return element;
        }
    }

    /** Returns the least element of the PriorityQueue in constant time. */
    public final T top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap.get(1);
    }

    /** Removes and returns the least element of the PriorityQueue in log(size) time. */
    public final T pop() {
        if (size > 0) {
            T result = heap.get(1); // save first value
            heap.set(1, heap.get(size)); // move last to first
            heap.set(size, null); // permit GC of objects
            size--;
            downHeap(1); // adjust heap
            return result;
        } else {
            return null;
        }
    }

    /**
     * Should be called when the Object at top changes values. Still log(n) worst case, but it's at
     * least twice as fast to
     *
     * <pre class="prettyprint">
     * pq.top().change();
     * pq.updateTop();
     * </pre>
     *
     * instead of
     *
     * <pre class="prettyprint">
     * o = pq.pop();
     * o.change();
     * pq.push(o);
     * </pre>
     *
     * @return the new 'top' element.
     */
    public final T updateTop() {
        downHeap(1);
        return heap.get(1);
    }

    /** Replace the top of the pq with {@code newTop} and run {@link #updateTop()}. */
    public final T updateTop(T newTop) {
        heap.set(1, newTop);
        return updateTop();
    }

    /** Returns the number of elements currently stored in the PriorityQueue. */
    public final long size() {
        return size;
    }

    /** Removes all entries from the PriorityQueue. */
    public final void clear() {
        for (int i = 0; i <= size; i++) {
            heap.set(i, null);
        }
        size = 0;
    }

    /**
     * Removes an existing element currently stored in the PriorityQueue. Cost is linear with the size
     * of the queue. (A specialization of PriorityQueue which tracks element positions would provide a
     * constant remove time but the trade-off would be extra cost to all additions/insertions)
     */
    public final boolean remove(T element) {
        for (int i = 1; i <= size; i++) {
            if (heap.get(i) == element) {
                heap.set(i, heap.get(size));
                heap.set(size, null); // permit GC of objects
                size--;
                if (i <= size) {
                    if (upHeap(i) == false) {
                        downHeap(i);
                    }
                }
                return true;
            }
        }
        return false;
    }

    private boolean upHeap(long origPos) {
        long i = origPos;
        T node = heap.get(i); // save bottom node
        long j = i >>> 1;
        while (j > 0 && lessThan(node, heap.get(j))) {
            heap.set(i, heap.get(j)); // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap.set(i, node); // install saved node
        return i != origPos;
    }

    private void downHeap(long i) {
        T node = heap.get(i); // save top node
        long j = i << 1; // find smaller child
        long k = j + 1;
        if (k <= size && lessThan(heap.get(k), heap.get(j))) {
            j = k;
        }
        while (j <= size && lessThan(heap.get(j), node)) {
            heap.set(i, heap.get(j)); // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap.get(k), heap.get(j))) {
                j = k;
            }
        }
        heap.set(i, node); // install saved node
    }

    @Override
    public final Iterator<T> iterator() {
        return new Iterator<>() {

            long i = 1;

            @Override
            public boolean hasNext() {
                return i <= size;
            }

            @Override
            public T next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return heap.get(i++);
            }
        };
    }

    @Override
    public final void close() {
        Releasables.close(heap);
        doClose();
    }

    protected void doClose() {}
}

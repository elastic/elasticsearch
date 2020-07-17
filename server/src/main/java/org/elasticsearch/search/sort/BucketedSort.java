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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;

/**
 * Type specialized sort implementations designed for use in aggregations.
 * Aggregations have a couple of super interesting characteristics:
 * <ul>
 * <li>They can have many, many buckets so this implementation backs to
 * {@link BigArrays} so it doesn't need to allocate any objects per bucket
 * and the circuit breaker in {@linkplain BigArrays} will automatically
 * track memory usage and abort execution if it grows too large.</li>
 * <li>Its fairly common for a bucket to be collected but not returned so
 * these implementations delay as much work as possible until collection</li>
 * </ul>
 * <p>
 * Every bucket is in one of two states: "gathering" or min/max "heap". While
 * "gathering" the next empty slot is stored in the "root" offset of the
 * bucket and collecting a value is just adding it in the next slot bumping
 * the tracking value at the root. So collecting values is {@code O(1)}.
 * Extracting the results in sorted order is {@code O(n * log n)} because,
 * well, sorting is {@code O(n * log n)}. When a bucket has collected
 * {@link #bucketSize} entries it is converted into a min "heap" in
 * {@code O(n)} time. Or into max heap, if {@link #order} is ascending.
 * </p>
 * <p>
 * Once a "heap", collecting a document is the heap-standard {@code O(log n)}
 * worst case. Critically, it is a very fast {@code O(1)} to check if a value
 * is competitive at all which, so long as buckets aren't hit in reverse
 * order, they mostly won't be. Extracting results in sorted order is still
 * {@code O(n * log n)}. 
 * </p>
 * <p>
 * When we first collect a bucket we make sure that we've allocated enough
 * slots to hold all sort values for the entire bucket. In other words: the
 * storage is "dense" and we don't try to save space when storing partially
 * filled buckets.
 * </p>
 * <p>
 * We actually *oversize* the allocations
 * (like {@link BigArrays#overSize(long)}) to get amortized linear number
 * of allocations and to play well with our paged arrays.
 * </p>
 */
public abstract class BucketedSort implements Releasable {
    /**
     * Callbacks for storing extra data along with competitive sorts.
     */
    public interface ExtraData {
        /**
         * Swap the position of two bits of extra data.
         * <p>
         * Both parameters will have previously been loaded by
         * {@link Loader#loadFromDoc(long, int)} so the implementer shouldn't
         * need to grow the underlying storage to implement this. 
         * </p>
         */
        void swap(long lhs, long rhs);
        /**
         * Prepare to load extra data from a leaf.
         */
        Loader loader(LeafReaderContext ctx) throws IOException;
        @FunctionalInterface
        interface Loader {
            /**
             * Load extra data from a doc.
             * <p>
             * Implementers <strong>should</strong> grow their underlying
             * storage to fit the {@code index}.
             * </p>
             */
            void loadFromDoc(long index, int doc) throws IOException;
        }
    }

    /**
     * An implementation of {@linkplain ExtraData} that does nothing.
     */
    public static final ExtraData NOOP_EXTRA_DATA = new ExtraData() {
        @Override
        public void swap(long lhs, long rhs) {}

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            return (index, doc) -> {};
        }
    };

    protected final BigArrays bigArrays;
    private final SortOrder order;
    private final DocValueFormat format;
    private final int bucketSize;
    private final ExtraData extra;
    /**
     * {@code true} if the bucket is in heap mode, {@code false} if
     * it is still gathering.
     */
    private final BitArray heapMode;

    protected BucketedSort(BigArrays bigArrays, SortOrder order, DocValueFormat format, int bucketSize, ExtraData extra) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.format = format;
        this.bucketSize = bucketSize;
        this.extra = extra;
        heapMode = new BitArray(1, bigArrays);
    }

    /**
     * The order of the sort.
     */
    public final SortOrder getOrder() {
        return order;
    }

    /**
     * The format to use when presenting the values.
     */
    public final DocValueFormat getFormat() {
        return format;
    }

    /**
     * The number of values to store per bucket.
     */
    public int getBucketSize() {
        return bucketSize;
    }

    /**
     * Used with {@link BucketedSort#getValues(long, ResultBuilder)} to
     * build results from the sorting operation.
     */
    @FunctionalInterface
    public interface ResultBuilder<T> {
        T build(long index, SortValue sortValue);
    }

    /**
     * Get the values for a bucket if it has been collected. If it hasn't
     * then returns an empty list.
     * @param builder builds results. See {@link ExtraData} for how to store
     *                data along side the sort for this to extract.
     */
    public final <T extends Comparable<T>> List<T> getValues(long bucket, ResultBuilder<T> builder) {
        long rootIndex = bucket * bucketSize;
        if (rootIndex >= values().size()) {
            // We've never seen this bucket.
            return emptyList();
        }
        long start = inHeapMode(bucket) ? rootIndex : (rootIndex + getNextGatherOffset(rootIndex) + 1);
        long end = rootIndex + bucketSize;
        List<T> result = new ArrayList<>(bucketSize);
        for (long index = start; index < end; index++) {
            result.add(builder.build(index, getValue(index)));
        }
        // TODO we usually have a heap here so we could use that to build the results sorted.
        result.sort(order.wrap(Comparator.<T>naturalOrder()));
        return result;
    }

    /**
     * Get the values for a bucket if it has been collected. If it hasn't
     * then returns an empty array.
     */
    public final List<SortValue> getValues(long bucket) {
        return getValues(bucket, (i, sv) -> sv);
    }

    /**
     * Is this bucket a min heap {@code true} or in gathering mode {@code false}? 
     */
    private boolean inHeapMode(long bucket) {
        return heapMode.get((int) bucket);
    }

    /**
     * Get the {@linkplain Leaf} implementation that'll do that actual collecting.
     * @throws IOException most implementations need to perform IO to prepare for each leaf
     */
    public abstract Leaf forLeaf(LeafReaderContext ctx) throws IOException;

    /**
     * Does this sort need scores? Most don't, but sorting on {@code _score} does.
     */
    public abstract boolean needsScores();

    /**
     * The {@linkplain BigArray} backing this sort.
     */
    protected abstract BigArray values();

    /**
     * Grow the {@linkplain BigArray} backing this sort to account for new buckets.
     * This will only be called if the array is too small.
     */
    protected abstract void growValues(long minSize);

    /**
     * Get the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     */
    protected abstract int getNextGatherOffset(long rootIndex);

    /**
     * Set the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     */
    protected abstract void setNextGatherOffset(long rootIndex, int offset);

    /**
     * Get the value at an index.
     */
    protected abstract SortValue getValue(long index);

    /**
     * {@code true} if the entry at index {@code lhs} is "better" than
     * the entry at {@code rhs}. "Better" in this means "lower" for
     * {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}. 
     */
    protected abstract boolean betterThan(long lhs, long rhs);

    /**
     * Swap the data at two indices.
     */
    protected abstract void swap(long lhs, long rhs);

    /**
     * Return a fairly human readable representation of the array backing the sort.
     * <p>
     * This is intentionally not a {@link #toString()} implementation because it'll
     * be quite slow.
     * </p>
     */
    protected final String debugFormat() {
        StringBuilder b = new StringBuilder();
        for (long index = 0; index < values().size(); index++) {
            if (index % bucketSize == 0) {
                b.append('\n').append(String.format(Locale.ROOT, "%20d", index / bucketSize)).append(":  ");
            }
            b.append(String.format(Locale.ROOT, "%20s", getValue(index))).append(' ');
        }
        return b.toString();
    }

    /**
     * Initialize the gather offsets after setting up values. Subclasses
     * should call this once, after setting up their {@link #values()}.  
     */
    protected final void initGatherOffsets() {
        setNextGatherOffsets(0);
    }

    /**
     * Allocate storage for more buckets and store the "next gather offset"
     * for those new buckets.
     */
    private void grow(long minSize) {
        long oldMax = values().size() - 1;
        growValues(minSize);
        // Set the next gather offsets for all newly allocated buckets.
        setNextGatherOffsets(oldMax - (oldMax % getBucketSize()) + getBucketSize());
    }

    /**
     * Maintain the "next gather offsets" for newly allocated buckets.
     */
    private void setNextGatherOffsets(long startingAt) {
        int nextOffset = getBucketSize() - 1;
        for (long bucketRoot = startingAt; bucketRoot < values().size(); bucketRoot += getBucketSize()) {
            setNextGatherOffset(bucketRoot, nextOffset);
        }
    }

    /**
     * Heapify a bucket who's entries are in random order.
     * <p>
     * This works by validating the heap property on each node, iterating
     * "upwards", pushing any out of order parents "down". Check out the
     * <a href="https://en.wikipedia.org/w/index.php?title=Binary_heap&oldid=940542991#Building_a_heap">wikipedia</a>
     * entry on binary heaps for more about this.
     * </p>
     * <p>
     * While this *looks* like it could easily be {@code O(n * log n)}, it is
     * a fairly well studied algorithm attributed to Floyd. There's
     * been a bunch of work that puts this at {@code O(n)}, close to 1.88n worst
     * case.
     * </p>
     * <ul>
     * <li>Hayward, Ryan; McDiarmid, Colin (1991).  
     * <a href="https://web.archive.org/web/20160205023201/http://www.stats.ox.ac.uk/__data/assets/pdf_file/0015/4173/heapbuildjalg.pdf">
     * Average Case Analysis of Heap Building byRepeated Insertion</a> J. Algorithms.
     * <li>D.E. Knuth, ”The Art of Computer Programming, Vol. 3, Sorting and Searching”</li>
     * </ul>
     * @param rootIndex the index the start of the bucket 
     */
    private void heapify(long rootIndex) {
        int maxParent = bucketSize / 2 - 1;
        for (int parent = maxParent; parent >= 0; parent--) {
            downHeap(rootIndex, parent);
        }
    }

    /**
     * Correct the heap invariant of a parent and its children. This
     * runs in {@code O(log n)} time.
     * @param rootIndex index of the start of the bucket
     * @param parent Index within the bucket of the parent to check.
     *               For example, 0 is the "root". 
     */
    private void downHeap(long rootIndex, int parent) {
        while (true) {
            long parentIndex = rootIndex + parent;
            int worst = parent;
            long worstIndex = parentIndex;
            int leftChild = parent * 2 + 1;
            long leftIndex = rootIndex + leftChild;
            if (leftChild < bucketSize) {
                if (betterThan(worstIndex, leftIndex)) {
                    worst = leftChild;
                    worstIndex = leftIndex;
                }
                int rightChild = leftChild + 1;
                long rightIndex = rootIndex + rightChild;
                if (rightChild < bucketSize && betterThan(worstIndex, rightIndex)) {
                    worst = rightChild;
                    worstIndex = rightIndex;
                }
            }
            if (worst == parent) {
                break;
            }
            swap(worstIndex, parentIndex);
            extra.swap(worstIndex, parentIndex);
            parent = worst;
        }
    }

    @Override
    public final void close() {
        Releasables.close(values(), heapMode);
    }

    /**
     * Performs the actual collection against a {@linkplain LeafReaderContext}.
     */
    public abstract class Leaf implements ScorerAware {
        private final LeafReaderContext ctx;
        private ExtraData.Loader loader = null;

        protected Leaf(LeafReaderContext ctx) {
            this.ctx = ctx;
        }

        /**
         * Collect this doc, returning {@code true} if it is competitive.
         */
        public final void collect(int doc, long bucket) throws IOException {
            if (false == advanceExact(doc)) {
                return;
            }
            long rootIndex = bucket * bucketSize;
            if (inHeapMode(bucket)) {
                if (docBetterThan(rootIndex)) {
                    // TODO a "bottom up" insert would save a couple of comparisons. Worth it?
                    setIndexToDocValue(rootIndex);
                    loader().loadFromDoc(rootIndex, doc);
                    downHeap(rootIndex, 0);
                }
                return;
            }
            // Gathering mode
            long requiredSize = rootIndex + bucketSize;
            if (values().size() < requiredSize) {
                grow(requiredSize);
            }
            int next = getNextGatherOffset(rootIndex);
            assert 0 <= next && next < bucketSize :
                "Expected next to be in the range of valid buckets [0 <= " + next + " < " + bucketSize + "]";
            long index = next + rootIndex;
            setIndexToDocValue(index);
            loader().loadFromDoc(index, doc);
            if (next == 0) {
                if (bucket > Integer.MAX_VALUE) {
                    throw new UnsupportedOperationException("Bucketed sort doesn't support more than [" + Integer.MAX_VALUE + "] buckets");
                    // BitArray needs int keys and this'd take a ton of memory to use that many buckets. So we just don't.
                }
                heapMode.set((int) bucket);
                heapify(rootIndex);
            } else {
                setNextGatherOffset(rootIndex, next - 1);
            }
            return;
        }

        /**
         * Read the sort value from {@code doc} and return {@code true}
         * if there is a value for that document. Otherwise return
         * {@code false} and the sort will skip that document.
         */
        protected abstract boolean advanceExact(int doc) throws IOException;

        /**
         * Set the value at the index to the value of the document to which
         * we just advanced.
         */
        protected abstract void setIndexToDocValue(long index);

        /**
         * {@code true} if the sort value for the doc is "better" than the
         * entry at {@code index}. "Better" in means is "lower" for
         * {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}. 
         */
        protected abstract boolean docBetterThan(long index);

        /**
         * Get the extra data loader, building it if we haven't yet built one for this leaf.
         */
        private ExtraData.Loader loader() throws IOException {
            if (loader == null) {
                loader = extra.loader(ctx);
            }
            return loader;
        }
    }

    /**
     * Superclass for implementations of {@linkplain BucketedSort} for {@code double} keys.
     */
    public abstract static class ForDoubles extends BucketedSort {
        private DoubleArray values = bigArrays.newDoubleArray(getBucketSize(), false);

        public ForDoubles(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format, int bucketSize, ExtraData extra) {
            super(bigArrays, sortOrder, format, bucketSize, extra);
            initGatherOffsets();
        }

        @Override
        public boolean needsScores() { return false; }

        @Override
        protected final BigArray values() { return values; }

        @Override
        protected final void growValues(long minSize) {
            values = bigArrays.grow(values, minSize);
        }

        @Override
        protected final int getNextGatherOffset(long rootIndex) {
            // This cast is safe because all ints fit accurately into a double.
            return (int) values.get(rootIndex);
        }

        @Override
        protected final void setNextGatherOffset(long rootIndex, int offset) {
            values.set(rootIndex, offset);
        }

        @Override
        protected final SortValue getValue(long index) {
            return SortValue.from(values.get(index));
        }

        @Override
        protected final boolean betterThan(long lhs, long rhs) {
            return getOrder().reverseMul() * Double.compare(values.get(lhs), values.get(rhs)) < 0;
        }

        @Override
        protected final void swap(long lhs, long rhs) {
            double tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected Leaf(LeafReaderContext ctx) {
                super(ctx);
            }

            /**
             * Return the value for of this sort for the document to which
             * we just {@link #advanceExact(int) moved}. This should be fast
             * because it is called twice per competitive hit when in heap
             * mode, once for {@link #docBetterThan(long)} and once
             * for {@link #setIndexToDocValue(long)}.
             */
            protected abstract double docValue();

            @Override
            public final void setScorer(Scorable scorer) {}

            @Override
            protected final void setIndexToDocValue(long index) {
                values.set(index, docValue());
            }

            @Override
            protected final boolean docBetterThan(long index) {
                return getOrder().reverseMul() * Double.compare(docValue(), values.get(index)) < 0;
            }
        }
    }

    /**
     * Superclass for implementations of {@linkplain BucketedSort} for {@code float} keys.
     */
    public abstract static class ForFloats extends BucketedSort {
        /**
         * The maximum size of buckets this can store. This is because we
         * store the next offset to write to in a float and floats only have
         * {@code 23} bits of mantissa so they can't accurate store values
         * higher than {@code 2 ^ 24}. 
         */
        public static final int MAX_BUCKET_SIZE = (int) Math.pow(2, 24);

        private FloatArray values = bigArrays.newFloatArray(1, false);

        public ForFloats(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format, int bucketSize, ExtraData extra) {
            super(bigArrays, sortOrder, format, bucketSize, extra);
            if (bucketSize > MAX_BUCKET_SIZE) {
                close();
                throw new IllegalArgumentException("bucket size must be less than [2^24] but was [" + bucketSize + "]");
            }
            initGatherOffsets();
        }

        @Override
        protected final BigArray values() { return values; }

        @Override
        protected final void growValues(long minSize) {
            values = bigArrays.grow(values, minSize);
        }

        @Override
        protected final int getNextGatherOffset(long rootIndex) {
            /*
             * This cast will not lose precision because we make sure never
             * to write values here that float can't store precisely.
             */
            return (int) values.get(rootIndex);
        }

        @Override
        protected final void setNextGatherOffset(long rootIndex, int offset) {
            values.set(rootIndex, offset);
        }

        @Override
        protected final SortValue getValue(long index) {
            return SortValue.from(values.get(index));
        }

        @Override
        protected final boolean betterThan(long lhs, long rhs) {
            return getOrder().reverseMul() * Float.compare(values.get(lhs), values.get(rhs)) < 0;
        }

        @Override
        protected final void swap(long lhs, long rhs) {
            float tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected Leaf(LeafReaderContext ctx) {
                super(ctx);
            }

            /**
             * Return the value for of this sort for the document to which
             * we just {@link #advanceExact(int) moved}. This should be fast
             * because it is called twice per competitive hit when in heap
             * mode, once for {@link #docBetterThan(long)} and once
             * for {@link #setIndexToDocValue(long)}.
             */
            protected abstract float docValue();

            @Override
            protected final void setIndexToDocValue(long index) {
                values.set(index, docValue());
            }

            @Override
            protected final boolean docBetterThan(long index) {
                return getOrder().reverseMul() * Float.compare(docValue(), values.get(index)) < 0;
            }
        }
    }

    /**
     * Superclass for implementations of {@linkplain BucketedSort} for {@code long} keys.
     */
    public abstract static class ForLongs extends BucketedSort {
        private LongArray values = bigArrays.newLongArray(1, false);

        public ForLongs(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format, int bucketSize, ExtraData extra) {
            super(bigArrays, sortOrder, format, bucketSize, extra);
            initGatherOffsets();
        }

        @Override
        public final boolean needsScores() { return false; }

        @Override
        protected final BigArray values() { return values; }

        @Override
        protected final void growValues(long minSize) {
            values = bigArrays.grow(values, minSize);
        }

        @Override
        protected final int getNextGatherOffset(long rootIndex) {
            return (int) values.get(rootIndex);
        }

        @Override
        protected final void setNextGatherOffset(long rootIndex, int offset) {
            values.set(rootIndex, offset);
        }

        @Override
        protected final SortValue getValue(long index) {
            return SortValue.from(values.get(index));
        }

        @Override
        protected final boolean betterThan(long lhs, long rhs) {
            return getOrder().reverseMul() * Long.compare(values.get(lhs), values.get(rhs)) < 0;
        }

        @Override
        protected final void swap(long lhs, long rhs) {
            long tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected Leaf(LeafReaderContext ctx) {
                super(ctx);
            }

            /**
             * Return the value for of this sort for the document to which
             * we just {@link #advanceExact(int) moved}. This should be fast
             * because it is called twice per competitive hit when in heap
             * mode, once for {@link #docBetterThan(long)} and once
             * for {@link #setIndexToDocValue(long)}.
             */
            protected abstract long docValue();

            @Override
            public final void setScorer(Scorable scorer) {}

            @Override
            protected final void setIndexToDocValue(long index) {
                values.set(index, docValue());
            }

            @Override
            protected final boolean docBetterThan(long index) {
                return getOrder().reverseMul() * Long.compare(docValue(), values.get(index)) < 0;
            }
        }
    }
}

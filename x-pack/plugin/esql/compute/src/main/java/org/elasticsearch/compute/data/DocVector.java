/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * {@link Vector} where each entry references a lucene document.
 */
public final class DocVector extends AbstractVector implements Vector {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocVector.class);

    /**
     * Per position memory cost to build the shard segment doc map required
     * to load fields out of order.
     */
    public static final int SHARD_SEGMENT_DOC_MAP_PER_ROW_OVERHEAD = Integer.BYTES * 2;

    private final IntVector shards;
    private final IntVector segments;
    private final IntVector docs;

    /**
     * Are the docs in this vector all in one segment and non-decreasing? If
     * so we can load doc values via a fast path.
     */
    private Boolean singleSegmentNonDecreasing;

    /**
     * Maps the vector positions to ascending docs per-shard and per-segment.
     */
    private int[] shardSegmentDocMapForwards;

    /**
     * Reverse of {@link #shardSegmentDocMapForwards}.
     */
    private int[] shardSegmentDocMapBackwards;

    final DocBlock block;

    public DocVector(IntVector shards, IntVector segments, IntVector docs, Boolean singleSegmentNonDecreasing) {
        super(shards.getPositionCount(), null);
        this.shards = shards;
        this.segments = segments;
        this.docs = docs;
        this.singleSegmentNonDecreasing = singleSegmentNonDecreasing;
        if (shards.getPositionCount() != segments.getPositionCount()) {
            throw new IllegalArgumentException(
                "invalid position count [" + shards.getPositionCount() + " != " + segments.getPositionCount() + "]"
            );
        }
        if (shards.getPositionCount() != docs.getPositionCount()) {
            throw new IllegalArgumentException(
                "invalid position count [" + shards.getPositionCount() + " != " + docs.getPositionCount() + "]"
            );
        }
        block = new DocBlock(this);
    }

    public IntVector shards() {
        return shards;
    }

    public IntVector segments() {
        return segments;
    }

    public IntVector docs() {
        return docs;
    }

    public boolean singleSegmentNonDecreasing() {
        if (singleSegmentNonDecreasing == null) {
            singleSegmentNonDecreasing = checkIfSingleSegmentNonDecreasing();
        }
        return singleSegmentNonDecreasing;
    }

    private boolean checkIfSingleSegmentNonDecreasing() {
        if (getPositionCount() < 2) {
            return true;
        }
        if (shards.isConstant() == false || segments.isConstant() == false) {
            return false;
        }
        int prev = docs.getInt(0);
        int p = 1;
        while (p < getPositionCount()) {
            int v = docs.getInt(p++);
            if (prev > v) {
                return false;
            }
            prev = v;
        }
        return true;

    }

    /**
     * Map from the positions in this page to the positions in lucene's native order for
     * loading doc values.
     */
    public int[] shardSegmentDocMapForwards() {
        buildShardSegmentDocMapIfMissing();
        return shardSegmentDocMapForwards;
    }

    /**
     * Reverse of {@link #shardSegmentDocMapForwards}. If you load doc values in the "forward"
     * order then you can call {@link Block#filter} on the loaded values with this array to
     * put them in the same order as this {@link Page}.
     */
    public int[] shardSegmentDocMapBackwards() {
        buildShardSegmentDocMapIfMissing();
        return shardSegmentDocMapBackwards;
    }

    private void buildShardSegmentDocMapIfMissing() {
        if (shardSegmentDocMapForwards != null) {
            return;
        }

        int[] forwards = shardSegmentDocMapForwards = new int[shards.getPositionCount()];
        for (int p = 0; p < forwards.length; p++) {
            forwards[p] = p;
        }
        new IntroSorter() {
            int pivot;

            @Override
            protected void setPivot(int i) {
                pivot = forwards[i];
            }

            @Override
            protected int comparePivot(int j) {
                int cmp = Integer.compare(shards.getInt(pivot), shards.getInt(forwards[j]));
                if (cmp != 0) {
                    return cmp;
                }
                cmp = Integer.compare(segments.getInt(pivot), segments.getInt(forwards[j]));
                if (cmp != 0) {
                    return cmp;
                }
                return Integer.compare(docs.getInt(pivot), docs.getInt(forwards[j]));
            }

            @Override
            protected void swap(int i, int j) {
                int tmp = forwards[i];
                forwards[i] = forwards[j];
                forwards[j] = tmp;
            }
        }.sort(0, forwards.length);

        int[] backwards = shardSegmentDocMapBackwards = new int[forwards.length];
        for (int p = 0; p < forwards.length; p++) {
            backwards[forwards[p]] = p;
        }
    }

    @Override
    public DocBlock asBlock() {
        return block;
    }

    @Override
    public DocVector filter(int... positions) {
        return new DocVector(shards.filter(positions), segments.filter(positions), docs.filter(positions), null);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC;
    }

    @Override
    public boolean isConstant() {
        return shards.isConstant() && segments.isConstant() && docs.isConstant();
    }

    @Override
    public int hashCode() {
        return Objects.hash(shards, segments, docs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocVector == false) {
            return false;
        }
        DocVector other = (DocVector) obj;
        return shards.equals(other.shards) && segments.equals(other.segments) && docs.equals(other.docs);
    }

    private static long ramBytesOrZero(int[] array) {
        return array == null ? 0 : RamUsageEstimator.shallowSizeOf(array);
    }

    public static long ramBytesEstimated(
        IntVector shards,
        IntVector segments,
        IntVector docs,
        int[] shardSegmentDocMapForwards,
        int[] shardSegmentDocMapBackwards
    ) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(shards) + RamUsageEstimator.sizeOf(segments) + RamUsageEstimator.sizeOf(docs)
            + ramBytesOrZero(shardSegmentDocMapForwards) + ramBytesOrZero(shardSegmentDocMapBackwards);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(shards, segments, docs, shardSegmentDocMapForwards, shardSegmentDocMapBackwards);
    }

    @Override
    public void close() {
        released = true;
        Releasables.closeExpectNoException(shards.asBlock(), segments.asBlock(), docs.asBlock()); // Ugh! we always close blocks
    }
}

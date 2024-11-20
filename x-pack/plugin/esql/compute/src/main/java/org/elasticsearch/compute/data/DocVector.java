/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * {@link Vector} where each entry references a lucene document.
 */
public final class DocVector extends AbstractVector implements Vector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocVector.class);

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

    public DocVector(IntVector shards, IntVector segments, IntVector docs, Boolean singleSegmentNonDecreasing) {
        super(shards.getPositionCount(), shards.blockFactory());
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
        blockFactory().adjustBreaker(BASE_RAM_BYTES_USED);
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

    public boolean singleSegment() {
        return shards.isConstant() && segments.isConstant();
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

        boolean success = false;
        long estimatedSize = sizeOfSegmentDocMap();
        blockFactory().adjustBreaker(estimatedSize);
        int[] forwards = null;
        int[] backwards = null;
        try {
            int[] finalForwards = forwards = new int[shards.getPositionCount()];
            for (int p = 0; p < forwards.length; p++) {
                forwards[p] = p;
            }
            if (singleSegment()) {
                new IntroSorter() {
                    int pivot;

                    @Override
                    protected void setPivot(int i) {
                        pivot = finalForwards[i];
                    }

                    @Override
                    protected int comparePivot(int j) {
                        return Integer.compare(docs.getInt(pivot), docs.getInt(finalForwards[j]));
                    }

                    @Override
                    protected void swap(int i, int j) {
                        int tmp = finalForwards[i];
                        finalForwards[i] = finalForwards[j];
                        finalForwards[j] = tmp;
                    }
                }.sort(0, forwards.length);
            } else {
                new IntroSorter() {
                    int pivot;

                    @Override
                    protected void setPivot(int i) {
                        pivot = finalForwards[i];
                    }

                    @Override
                    protected int comparePivot(int j) {
                        int cmp = Integer.compare(shards.getInt(pivot), shards.getInt(finalForwards[j]));
                        if (cmp != 0) {
                            return cmp;
                        }
                        cmp = Integer.compare(segments.getInt(pivot), segments.getInt(finalForwards[j]));
                        if (cmp != 0) {
                            return cmp;
                        }
                        return Integer.compare(docs.getInt(pivot), docs.getInt(finalForwards[j]));
                    }

                    @Override
                    protected void swap(int i, int j) {
                        int tmp = finalForwards[i];
                        finalForwards[i] = finalForwards[j];
                        finalForwards[j] = tmp;
                    }
                }.sort(0, forwards.length);
            }
            backwards = new int[forwards.length];
            for (int p = 0; p < forwards.length; p++) {
                backwards[forwards[p]] = p;
            }
            success = true;
            shardSegmentDocMapForwards = forwards;
            shardSegmentDocMapBackwards = backwards;
        } finally {
            if (success == false) {
                blockFactory().adjustBreaker(-estimatedSize);
            }
        }
    }

    private long sizeOfSegmentDocMap() {
        return 2 * (((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) + ((long) Integer.BYTES) * shards.getPositionCount());
    }

    @Override
    public DocBlock asBlock() {
        return new DocBlock(this);
    }

    @Override
    public DocVector filter(int... positions) {
        IntVector filteredShards = null;
        IntVector filteredSegments = null;
        IntVector filteredDocs = null;
        DocVector result = null;
        try {
            filteredShards = shards.filter(positions);
            filteredSegments = segments.filter(positions);
            filteredDocs = docs.filter(positions);
            result = new DocVector(filteredShards, filteredSegments, filteredDocs, null);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(filteredShards, filteredSegments, filteredDocs);
            }
        }
    }

    @Override
    public DocBlock keepMask(BooleanVector mask) {
        throw new UnsupportedOperationException("can't mask DocVector because it can't contain nulls");
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DocVector");
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
    public void allowPassingToDifferentDriver() {
        super.allowPassingToDifferentDriver();
        shards.allowPassingToDifferentDriver();
        segments.allowPassingToDifferentDriver();
        docs.allowPassingToDifferentDriver();
    }

    @Override
    public void closeInternal() {
        Releasables.closeExpectNoException(
            () -> blockFactory().adjustBreaker(-BASE_RAM_BYTES_USED - (shardSegmentDocMapForwards == null ? 0 : sizeOfSegmentDocMap())),
            shards,
            segments,
            docs
        );
    }
}

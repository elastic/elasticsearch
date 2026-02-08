/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

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

    /**
     * The shard IDs for each position. Note that these shard IDs are shared between all doc vectors running in the same node, but a given
     * doc vector might only reference a subset of the shard IDs (Which is the subset is also the one exposed by {@link #refCounteds}).
     * These shard IDs are sliced up by DataNodeComputeHandler, and depend on the MAX_CONCURRENT_SHARDS_PER_NODE setting.
     */
    private final IntVector shards;
    private final IntVector segments;
    private final IntVector docs;

    /**
     * Reference counting for the {@link IndexReader}s that can do things with
     * these doc ids. We aggressively close the readers when we don't reference
     * them anymore.
     */
    private final IndexedByShardId<? extends RefCounted> refCounteds;

    /**
     * Can this vector reference duplicate documents? Some {@link BlockLoader}s will
     * run more slowly if this is {@code true}. These {@linkplain BlockLoader}s will
     * return incorrect results if there are duplicates and this is {@code false}.
     * This exists because of a hierarchy of speeds:
     * <ul>
     *     <li>
     *         We can better optimize some {@link BlockLoader}s when they receive
     *         {@linkplain DocVector}s that don't contain duplicates.
     *     </li>
     *     <li>
     *         It's rare that we want to load from duplicate doc ids. We don't need
     *         to spend that much time optimizing it.
     *     </li>
     *     <li>
     *         We sometimes really <strong>want</strong> to load from duplicate
     *         doc ids to minimize total amount of loading we have to do in fairly
     *         specific cases like resolving dimension values after time series
     *         aggregations.
     *     </li>
     * </ul>
     */
    private final boolean mayContainDuplicates;

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

    /**
     * Fancy wrapper around the ctor for {@link DocVector}.
     */
    public static Config config() {
        return new Config();
    }

    public DocVector(
        IndexedByShardId<? extends RefCounted> refCounteds,
        IntVector shards,
        IntVector segments,
        IntVector docs,
        Config config
    ) {
        super(shards.getPositionCount(), shards.blockFactory());
        this.refCounteds = refCounteds;
        this.shards = shards;
        this.segments = segments;
        this.docs = docs;
        this.singleSegmentNonDecreasing = config.singleSegmentNonDecreasing;
        this.mayContainDuplicates = config.mayContainDuplicates;

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
        if (Assertions.ENABLED && mayContainDuplicates == false) {
            assertNoDupes();
        }
        blockFactory().adjustBreaker(BASE_RAM_BYTES_USED);

        if (config.incrementShardRefCounts) {
            forEachShardRefCounter(RefCounted::mustIncRef);
        }
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

    /**
     * Does this vector contain only documents from a single segment.
     */
    public boolean singleSegment() {
        return shards.isConstant() && segments.isConstant();
    }

    /**
     * Can this vector reference duplicate documents? Some {@link BlockLoader}s will
     * run more slowly if this is {@code true}. These {@linkplain BlockLoader}s will
     * return incorrect results if there are duplicates and this is {@code false}.
     * This exists because of a hierarchy of speeds:
     * <ul>
     *     <li>
     *         We can better optimize some {@link BlockLoader}s when they receive
     *         {@linkplain DocVector}s that don't contain duplicates.
     *     </li>
     *     <li>
     *         It's rare that we want to load from duplicate doc ids. We don't need
     *         to spend that much time optimizing it.
     *     </li>
     *     <li>
     *         We sometimes really <strong>want</strong> to load from duplicate
     *         doc ids to minimize total amount of loading we have to do in fairly
     *         specific cases like resolving dimension values after time series
     *         aggregations.
     *     </li>
     * </ul>
     */
    public boolean mayContainDuplicates() {
        return mayContainDuplicates;
    }

    /**
     * Does this vector contain only documents from a single segment <strong>and</strong>
     * are the documents always non-decreasing? Non-decreasing here means almost monotonically
     * increasing.
     */
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
        if (singleSegment() == false) {
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
        int[] forwards;
        try {
            forwards = new int[shards.getPositionCount()];
            for (int p = 0; p < forwards.length; p++) {
                forwards[p] = p;
            }
            if (singleSegment()) {
                new IntroSorter() {
                    int pivot;

                    @Override
                    protected void setPivot(int i) {
                        pivot = forwards[i];
                    }

                    @Override
                    protected int comparePivot(int j) {
                        return Integer.compare(docs.getInt(pivot), docs.getInt(forwards[j]));
                    }

                    @Override
                    protected void swap(int i, int j) {
                        int tmp = forwards[i];
                        forwards[i] = forwards[j];
                        forwards[j] = tmp;
                    }
                }.sort(0, forwards.length);
            } else {
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
            }
            int[] backwards = new int[forwards.length];
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

    public static long sizeOfSegmentDocMap(int positionCount) {
        return 2 * (((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) + ((long) Integer.BYTES) * positionCount);
    }

    private long sizeOfSegmentDocMap() {
        return sizeOfSegmentDocMap(getPositionCount());
    }

    @Override
    public DocBlock asBlock() {
        return new DocBlock(this);
    }

    @Override
    public DocVector filter(boolean mayContainDuplicates, int... positions) {
        mayContainDuplicates |= this.mayContainDuplicates;
        IntVector filteredShards = null;
        IntVector filteredSegments = null;
        IntVector filteredDocs = null;
        DocVector result = null;
        try {
            filteredShards = shards.filter(mayContainDuplicates, positions);
            filteredSegments = segments.filter(mayContainDuplicates, positions);
            filteredDocs = docs.filter(mayContainDuplicates, positions);
            Config config = config();
            if (mayContainDuplicates) {
                config.mayContainDuplicates();
            }
            result = new DocVector(refCounteds, filteredShards, filteredSegments, filteredDocs, config);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(filteredShards, filteredSegments, filteredDocs);
            }
        }
    }

    @Override
    public DocVector deepCopy(BlockFactory blockFactory) {
        IntVector filteredShards = null;
        IntVector filteredSegments = null;
        IntVector filteredDocs = null;
        DocVector result = null;
        try {
            filteredShards = shards.deepCopy(blockFactory);
            filteredSegments = segments.deepCopy(blockFactory);
            filteredDocs = docs.deepCopy(blockFactory);
            Config config = config();
            if (mayContainDuplicates) {
                config.mayContainDuplicates();
            }
            result = new DocVector(refCounteds, filteredShards, filteredSegments, filteredDocs, config);
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
        return shards.equals(other.shards)
            && segments.equals(other.segments)
            && docs.equals(other.docs)
            && mayContainDuplicates == other.mayContainDuplicates;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DocVector[");
        sb.append("shards=").append(shards);
        sb.append(", segments=").append(segments);
        sb.append(", docs=").append(docs);
        sb.append(", mayContainDuplicates=").append(mayContainDuplicates);
        sb.append(']');
        return sb.toString();
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
        forEachShardRefCounter(RefCounted::decRef);
    }

    public RefCounted shardRefCounted(int position) {
        return refCounteds.get(shards.getInt(position));
    }

    private void forEachShardRefCounter(Consumer<RefCounted> consumer) {
        switch (shards) {
            case ConstantIntVector constantIntVector -> consumer.accept(refCounteds.get(constantIntVector.getInt(0)));
            case ConstantNullVector ignored -> {
                // Noop
            }
            default -> {
                for (int i = 0; i < shards.getPositionCount(); i++) {
                    consumer.accept(refCounteds.get(shards.getInt(i)));
                }
            }
        }
    }

    /**
     * Builds a doc vector with a fixed length {@link DocBlock}.
     */
    public static FixedBuilder newFixedBuilder(BlockFactory blockFactory, int estimatedSize) {
        return new FixedBuilder(blockFactory, estimatedSize);
    }

    private void assertNoDupes() {
        record Doc(int shard, int segment, int doc) {}
        Set<Doc> set = new HashSet<>(getPositionCount());
        for (int i = 0; i < getPositionCount(); i++) {
            Doc doc = new Doc(shards.getInt(i), segments.getInt(i), docs.getInt(i));
            boolean firstTime = set.add(doc);
            if (firstTime == false) {
                throw new IllegalStateException("configured not to contain duplicates but " + doc + " was duplicated");
            }
        }
    }

    public static class FixedBuilder implements Releasable {
        private final IntVector.FixedBuilder shards;
        private final IntVector.FixedBuilder segments;
        private final IntVector.FixedBuilder docs;
        private IndexedByShardId<? extends RefCounted> shardRefCounters = AlwaysReferencedIndexedByShardId.INSTANCE;

        private FixedBuilder(BlockFactory blockFactory, int size) {
            IntVector.FixedBuilder shards = null;
            IntVector.FixedBuilder segments = null;
            IntVector.FixedBuilder docs = null;
            try {
                shards = blockFactory.newIntVectorFixedBuilder(size);
                segments = blockFactory.newIntVectorFixedBuilder(size);
                docs = blockFactory.newIntVectorFixedBuilder(size);
            } finally {
                if (docs == null) {
                    Releasables.closeExpectNoException(shards, segments);
                }
            }
            // TODO it'd be nice to detect constant shards and segments.
            this.shards = shards;
            this.segments = segments;
            this.docs = docs;
        }

        public FixedBuilder append(int shard, int segment, int doc) {
            shards.appendInt(shard);
            segments.appendInt(segment);
            docs.appendInt(doc);
            return this;
        }

        public FixedBuilder shardRefCounters(IndexedByShardId<? extends RefCounted> shardRefCounters) {
            this.shardRefCounters = shardRefCounters;
            return this;
        }

        public DocVector build(Config config) {
            // Pass null for singleSegmentNonDecreasing so we calculate it when we first need it.
            IntVector shards = null;
            IntVector segments = null;
            IntVector docs = null;
            DocVector result = null;
            try {
                shards = this.shards.build();
                segments = this.segments.build();
                docs = this.docs.build();
                result = new DocVector(shardRefCounters, shards, segments, docs, config);
                return result;
            } finally {
                if (result == null) {
                    Releasables.closeExpectNoException(shards, segments, docs);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(shards, segments, docs);
        }
    }

    public static class Config {
        private Boolean singleSegmentNonDecreasing;
        private boolean incrementShardRefCounts = true;
        private boolean mayContainDuplicates = false;

        private Config() {}

        public Config singleSegmentNonDecreasing(boolean singleSegmentNonDecreasing) {
            this.singleSegmentNonDecreasing = singleSegmentNonDecreasing;
            return this;
        }

        public Config dontIncrementShardRefCounts() {
            this.incrementShardRefCounts = false;
            return this;
        }

        public Config mayContainDuplicates() {
            this.mayContainDuplicates = true;
            return this;
        }
    }
}

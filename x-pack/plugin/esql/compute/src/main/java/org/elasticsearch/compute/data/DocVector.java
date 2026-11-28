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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * {@link Vector} where each entry references a lucene document.
 * <p>
 * On the node that read the documents, the shard of each row is a slot of the node's shard contexts, and the vector holds
 * a reference on each shard it reads. Before rows leave that node, {@link #detach} replaces the slots with
 * {@link #origins()}: the shard and the reader context of each slot. Such a vector pins nothing, because the reader
 * context keeps the reader, so it can cross the network and come back to the node that loads the documents.
 */
public final class DocVector extends AbstractVector implements Vector {
    /**
     * The first version that can read a {@link DocBlock}. Code that sends one checks the version of the receiving node
     * first, so the check in {@link Block#writeTypedBlock} only catches bugs in that code.
     */
    public static final TransportVersion ESQL_DOC_ORIGINS = TransportVersion.fromName("esql_doc_origins");

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
     * The origin of each shard slot once the rows left the node that read them, {@code null} while they are on it. Shared
     * by the vectors sliced or filtered from this one, so it may hold origins no row uses.
     */
    @Nullable
    private final DocOrigins origins;

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
        this.origins = config.origins;
        this.singleSegmentNonDecreasing = config.singleSegmentNonDecreasing;
        this.mayContainDuplicates = config.mayContainDuplicates;

        if (origins != null && refCounteds != AlwaysReferencedIndexedByShardId.INSTANCE) {
            throw new IllegalArgumentException("DocVector with origins can't reference shards");
        }
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
        if (Assertions.ENABLED) {
            if (origins != null) {
                assertOrdinalsInRange();
            }
            if (mayContainDuplicates == false) {
                assertNoDupes();
            }
        }
        blockFactory().adjustBreaker(BASE_RAM_BYTES_USED + originsRamBytesUsed());

        // a vector with origins references no shard, so it has no refs to take or release
        if (origins == null && config.incrementShardRefCounts) {
            boolean success = false;
            try {
                incrementShardRefCounts();
                success = true;
            } finally {
                if (success == false) {
                    // Nothing will close a vector that failed to construct, so return its breaker charge here
                    blockFactory().adjustBreaker(-BASE_RAM_BYTES_USED - originsRamBytesUsed());
                }
            }
        }
    }

    /**
     * The origin of each shard slot, or {@code null} while the rows are on the node that read them.
     */
    @Nullable
    public DocOrigins origins() {
        return origins;
    }

    /**
     * The origin of the document at {@code position}.
     *
     * @throws IllegalStateException if the vector isn't detached
     */
    public DocOrigin origin(int position) {
        if (origins == null) {
            throw new IllegalStateException("DocVector has no origins");
        }
        return origins.get(shards.getInt(position));
    }

    /**
     * Fails if the vector is {@link #detach detached}, because its shard ordinals don't name shard contexts of this node.
     * Code that reads documents through the shard contexts of the node calls this first, so a vector that comes back from
     * another node fails instead of reading another shard.
     *
     * @param reader what reads the documents, for the message
     */
    public void ensureAttached(String reader) {
        if (origins != null) {
            throw new IllegalStateException("[" + reader + "] can't read a detached DocVector");
        }
    }

    /**
     * A copy whose rows can leave this node: the shard slots the rows reference become a dictionary of origins in the order
     * rows first reference them, and the segments and docs are shared. The copy holds no reference on any shard, so
     * releasing this vector releases the shards.
     *
     * @param originOfSlot the origin of a shard slot of this node. Called once for each slot a row references.
     */
    public DocVector detach(IntFunction<DocOrigin> originOfSlot) {
        if (origins != null) {
            throw new IllegalStateException("DocVector is already detached");
        }
        int positions = getPositionCount();
        // A TopN builds the shard vector as an array even when every row comes from one shard, so the check runs on the
        // rows and not on isConstant.
        List<DocOrigin> referenced = new ArrayList<>();
        int[] ordinalOfSlot = null;
        if (positions > 0) {
            if (shards.isConstant()) {
                referenced.add(originOfSlot.apply(shards.getInt(0)));
            } else {
                ordinalOfSlot = new int[shards.max() + 1];
                Arrays.fill(ordinalOfSlot, -1);
                for (int p = 0; p < positions; p++) {
                    int slot = shards.getInt(p);
                    if (ordinalOfSlot[slot] < 0) {
                        ordinalOfSlot[slot] = referenced.size();
                        referenced.add(originOfSlot.apply(slot));
                    }
                }
            }
        }
        IntVector ordinals = null;
        boolean shared = false;
        DocVector result = null;
        try {
            if (referenced.size() <= 1) {
                ordinals = blockFactory().newConstantIntVector(0, positions);
            } else {
                try (IntVector.FixedBuilder builder = blockFactory().newIntVectorFixedBuilder(positions)) {
                    for (int p = 0; p < positions; p++) {
                        builder.appendInt(ordinalOfSlot[shards.getInt(p)]);
                    }
                    ordinals = builder.build();
                }
            }
            segments.incRef();
            docs.incRef();
            shared = true;
            result = new DocVector(
                AlwaysReferencedIndexedByShardId.INSTANCE,
                ordinals,
                segments,
                docs,
                config().origins(DocOrigins.of(referenced)).mayContainDuplicates(mayContainDuplicates)
            );
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(ordinals, shared ? segments : null, shared ? docs : null);
            }
        }
    }

    /**
     * Writes a {@link #detach detached} vector. An empty vector names no shard, so it is written even if it isn't detached,
     * and reads back as a detached one. Wire format:
     * <ol>
     *     <li>{@code boolean} {@link #mayContainDuplicates()}</li>
     *     <li>the {@link DocOrigins}, including those no row references</li>
     *     <li>the shards, which are ordinals into the origins, the segments and the docs, as {@link IntVector}s</li>
     * </ol>
     */
    public void writeTo(StreamOutput out) throws IOException {
        if (origins == null && getPositionCount() > 0) {
            // the shard slots of a node mean nothing to another node
            throw new IllegalStateException("DocVector has no origins");
        }
        out.writeBoolean(mayContainDuplicates);
        (origins == null ? DocOrigins.EMPTY : origins).writeTo(out);
        shards.writeTo(out);
        segments.writeTo(out);
        docs.writeTo(out);
    }

    /**
     * Reads a vector written by {@link #writeTo}.
     */
    public static DocVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        boolean mayContainDuplicates = in.readBoolean();
        DocOrigins origins = DocOrigins.readFrom(in);
        IntVector shards = null;
        IntVector segments = null;
        IntVector docs = null;
        DocVector result = null;
        try {
            shards = IntVector.readFrom(blockFactory, in);
            segments = IntVector.readFrom(blockFactory, in);
            docs = IntVector.readFrom(blockFactory, in);
            Config config = config().origins(origins).mayContainDuplicates(mayContainDuplicates);
            result = new DocVector(AlwaysReferencedIndexedByShardId.INSTANCE, shards, segments, docs, config);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(shards, segments, docs);
            }
        }
    }

    private long originsRamBytesUsed() {
        return origins == null ? 0 : origins.ramBytesUsed();
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
    public DocVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        IntVector slicedShards = null;
        IntVector slicedSegments = null;
        IntVector slicedDocs = null;
        DocVector result = null;
        try {
            slicedShards = shards.slice(beginInclusive, endExclusive);
            slicedSegments = segments.slice(beginInclusive, endExclusive);
            slicedDocs = docs.slice(beginInclusive, endExclusive);
            Config config = config().origins(origins).mayContainDuplicates(mayContainDuplicates);
            result = new DocVector(refCounteds, slicedShards, slicedSegments, slicedDocs, config);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(slicedShards, slicedSegments, slicedDocs);
            }
        }
    }

    @Override
    public DocVector filter(boolean mayContainDuplicates, int[] positions, int offset, int length) {
        mayContainDuplicates |= this.mayContainDuplicates;
        IntVector filteredShards = null;
        IntVector filteredSegments = null;
        IntVector filteredDocs = null;
        DocVector result = null;
        try {
            filteredShards = shards.filter(mayContainDuplicates, positions, offset, length);
            filteredSegments = segments.filter(mayContainDuplicates, positions, offset, length);
            filteredDocs = docs.filter(mayContainDuplicates, positions, offset, length);
            Config config = config().origins(origins).mayContainDuplicates(mayContainDuplicates);
            result = new DocVector(refCounteds, filteredShards, filteredSegments, filteredDocs, config);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(filteredShards, filteredSegments, filteredDocs);
            }
        }
    }

    @Override
    public DocVector filter(boolean mayContainDuplicates, int... positions) {
        return filter(mayContainDuplicates, positions, 0, positions.length);
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
            Config config = config().origins(origins).mayContainDuplicates(mayContainDuplicates);
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
    public int valueMaxByteSize() {
        return 3 * Integer.BYTES;
    }

    @Override
    public boolean isConstant() {
        return shards.isConstant() && segments.isConstant() && docs.isConstant();
    }

    /**
     * With origins, rows hash by the origin they name rather than by their ordinal, like {@link #equals}.
     */
    @Override
    public int hashCode() {
        if (origins == null) {
            return Objects.hash(shards, segments, docs);
        }
        int result = 1;
        for (int p = 0; p < getPositionCount(); p++) {
            result = 31 * result + origin(p).hashCode();
            result = 31 * result + segments.getInt(p);
            result = 31 * result + docs.getInt(p);
        }
        return result;
    }

    /**
     * With origins, two vectors are equal when their rows name the same documents in the same order, however their
     * dictionaries are laid out.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocVector == false) {
            return false;
        }
        DocVector other = (DocVector) obj;
        if (mayContainDuplicates != other.mayContainDuplicates || (origins == null) != (other.origins == null)) {
            return false;
        }
        if (origins == null) {
            return shards.equals(other.shards) && segments.equals(other.segments) && docs.equals(other.docs);
        }
        if (getPositionCount() != other.getPositionCount()) {
            return false;
        }
        for (int p = 0; p < getPositionCount(); p++) {
            if (segments.getInt(p) != other.segments.getInt(p)
                || docs.getInt(p) != other.docs.getInt(p)
                || origin(p).equals(other.origin(p)) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DocVector[");
        if (origins != null) {
            sb.append("origins=").append(origins).append(", ");
        }
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
        return ramBytesEstimated(shards, segments, docs, shardSegmentDocMapForwards, shardSegmentDocMapBackwards) + originsRamBytesUsed();
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
            () -> blockFactory().adjustBreaker(
                -BASE_RAM_BYTES_USED - originsRamBytesUsed() - (shardSegmentDocMapForwards == null ? 0 : sizeOfSegmentDocMap())
            ),
            shards,
            segments,
            docs
        );
        if (origins == null) {
            decrementShardRefCounts();
        }
    }

    public RefCounted shardRefCounted(int position) {
        return refCounteds.get(shards.getInt(position));
    }

    /**
     * Increments the shard ref counts that {@link #decrementShardRefCounts} decrements. If a shard
     * is already closed this decrements the ref counts it already incremented before rethrowing,
     * so a failed constructor leaves every shard ref count as it was.
     */
    private void incrementShardRefCounts() {
        switch (shards) {
            case ConstantIntVector constantIntVector -> refCounteds.get(constantIntVector.getInt(0)).mustIncRef();
            case ConstantNullVector ignored -> {
                // Noop
            }
            default -> {
                int incremented = 0;
                boolean success = false;
                try {
                    while (incremented < shards.getPositionCount()) {
                        refCounteds.get(shards.getInt(incremented)).mustIncRef();
                        incremented++;
                    }
                    success = true;
                } finally {
                    if (success == false) {
                        for (int p = 0; p < incremented; p++) {
                            refCounteds.get(shards.getInt(p)).decRef();
                        }
                    }
                }
            }
        }
    }

    private void decrementShardRefCounts() {
        switch (shards) {
            case ConstantIntVector constantIntVector -> refCounteds.get(constantIntVector.getInt(0)).decRef();
            case ConstantNullVector ignored -> {
                // Noop
            }
            default -> {
                for (int i = 0; i < shards.getPositionCount(); i++) {
                    refCounteds.get(shards.getInt(i)).decRef();
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
        record Doc(Object shard, int segment, int doc) {}
        Set<Doc> set = new HashSet<>(getPositionCount());
        for (int i = 0; i < getPositionCount(); i++) {
            // with origins, two ordinals never name the same origin, but compare the origins to keep the check exact
            Object shard = origins == null ? shards.getInt(i) : origin(i);
            Doc doc = new Doc(shard, segments.getInt(i), docs.getInt(i));
            boolean firstTime = set.add(doc);
            if (firstTime == false) {
                throw new IllegalStateException("configured not to contain duplicates but " + doc + " was duplicated");
            }
        }
    }

    private void assertOrdinalsInRange() {
        int scan = shards.isConstant() ? Math.min(1, getPositionCount()) : getPositionCount();
        for (int p = 0; p < scan; p++) {
            int ordinal = shards.getInt(p);
            assert ordinal >= 0 && ordinal < origins.size() : "origin ordinal [" + ordinal + "] out of [0, " + origins.size() + ")";
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
        @Nullable
        private DocOrigins origins;

        private Config() {}

        /**
         * The shard vector holds ordinals into {@code origins}, because the rows left the node that read them. Such a vector
         * references no shard, so it takes {@link AlwaysReferencedIndexedByShardId} and takes and releases no shard refs.
         * {@code null} keeps the rows on their node.
         */
        public Config origins(@Nullable DocOrigins origins) {
            this.origins = origins;
            return this;
        }

        public Config mayContainDuplicates(boolean mayContainDuplicates) {
            this.mayContainDuplicates = mayContainDuplicates;
            return this;
        }

        public Config singleSegmentNonDecreasing(boolean singleSegmentNonDecreasing) {
            this.singleSegmentNonDecreasing = singleSegmentNonDecreasing;
            return this;
        }

        /**
         * Make the vector adopt shard refs that the caller already took rather than take its own.
         * The vector releases them when its own ref count drops to zero, so the caller must take
         * exactly one ref per position, or a single ref if {@code shards} is a
         * {@link ConstantIntVector}. The vector owns the refs only once it is built. If the
         * constructor, {@link DocBlock.Builder#build(Config)} or {@link FixedBuilder#build(Config)}
         * throws then the caller still owns the refs and must release them.
         */
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

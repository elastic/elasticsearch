/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.HashSet;
import java.util.Set;

/**
 * {@link Vector} where each entry references a lucene document by its {@link DocRefOrigin}, segment and doc id. Unlike
 * a {@link DocVector} it pins no reader and stays meaningful on other nodes, so it can cross the network and come back
 * to the node that loads the document.
 * <p>
 * Rows point at their origin through an ordinal into {@link #origins()}. The dictionary is shared by the vectors
 * filtered or sliced from this one, so it may hold origins no row uses.
 */
public final class DocRefVector extends AbstractVector implements Vector {
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocRefVector.class);

    private final DocRefOrigins origins;
    private final IntVector originOrdinals;
    private final IntVector segments;
    private final IntVector docs;
    private final boolean mayContainDuplicates;

    /**
     * Takes ownership of the three vectors and reserves memory for the dictionary.
     *
     * @param mayContainDuplicates {@code false} promises that no two rows reference the same document
     */
    public DocRefVector(DocRefOrigins origins, IntVector originOrdinals, IntVector segments, IntVector docs, boolean mayContainDuplicates) {
        super(originOrdinals.getPositionCount(), originOrdinals.blockFactory());
        if (originOrdinals.getPositionCount() != segments.getPositionCount()
            || originOrdinals.getPositionCount() != docs.getPositionCount()) {
            throw new IllegalArgumentException(
                "invalid position counts ["
                    + originOrdinals.getPositionCount()
                    + ", "
                    + segments.getPositionCount()
                    + ", "
                    + docs.getPositionCount()
                    + "]"
            );
        }
        this.origins = origins;
        this.originOrdinals = originOrdinals;
        this.segments = segments;
        this.docs = docs;
        this.mayContainDuplicates = mayContainDuplicates;
        if (Assertions.ENABLED) {
            assertValid();
        }
        blockFactory().adjustBreaker(BASE_RAM_BYTES_USED + origins.ramBytesUsed());
    }

    public DocRefOrigins origins() {
        return origins;
    }

    public IntVector originOrdinals() {
        return originOrdinals;
    }

    public IntVector segments() {
        return segments;
    }

    public IntVector docs() {
        return docs;
    }

    /**
     * The origin of the document at {@code position}.
     */
    public DocRefOrigin origin(int position) {
        return origins.get(originOrdinals.getInt(position));
    }

    /**
     * Do all rows come from one origin?
     */
    public boolean singleOrigin() {
        return originOrdinals.isConstant();
    }

    /**
     * Can two rows reference the same document?
     */
    public boolean mayContainDuplicates() {
        return mayContainDuplicates;
    }

    @Override
    public DocRefBlock asBlock() {
        return new DocRefBlock(this);
    }

    @Override
    public DocRefVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        IntVector slicedOrdinals = null;
        IntVector slicedSegments = null;
        IntVector slicedDocs = null;
        DocRefVector result = null;
        try {
            slicedOrdinals = originOrdinals.slice(beginInclusive, endExclusive);
            slicedSegments = segments.slice(beginInclusive, endExclusive);
            slicedDocs = docs.slice(beginInclusive, endExclusive);
            result = new DocRefVector(origins, slicedOrdinals, slicedSegments, slicedDocs, mayContainDuplicates);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(slicedOrdinals, slicedSegments, slicedDocs);
            }
        }
    }

    @Override
    public DocRefVector filter(boolean mayContainDuplicates, int[] positions, int offset, int length) {
        mayContainDuplicates |= this.mayContainDuplicates;
        IntVector filteredOrdinals = null;
        IntVector filteredSegments = null;
        IntVector filteredDocs = null;
        DocRefVector result = null;
        try {
            filteredOrdinals = originOrdinals.filter(mayContainDuplicates, positions, offset, length);
            filteredSegments = segments.filter(mayContainDuplicates, positions, offset, length);
            filteredDocs = docs.filter(mayContainDuplicates, positions, offset, length);
            result = new DocRefVector(origins, filteredOrdinals, filteredSegments, filteredDocs, mayContainDuplicates);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(filteredOrdinals, filteredSegments, filteredDocs);
            }
        }
    }

    @Override
    public DocRefVector filter(boolean mayContainDuplicates, int... positions) {
        return filter(mayContainDuplicates, positions, 0, positions.length);
    }

    @Override
    public DocRefVector deepCopy(BlockFactory blockFactory) {
        IntVector copiedOrdinals = null;
        IntVector copiedSegments = null;
        IntVector copiedDocs = null;
        DocRefVector result = null;
        try {
            copiedOrdinals = originOrdinals.deepCopy(blockFactory);
            copiedSegments = segments.deepCopy(blockFactory);
            copiedDocs = docs.deepCopy(blockFactory);
            result = new DocRefVector(origins, copiedOrdinals, copiedSegments, copiedDocs, mayContainDuplicates);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(copiedOrdinals, copiedSegments, copiedDocs);
            }
        }
    }

    @Override
    public DocRefBlock keepMask(BooleanVector mask) {
        throw new UnsupportedOperationException("can't mask DocRefVector because it can't contain nulls");
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DocRefVector");
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC_REF;
    }

    @Override
    public int valueMaxByteSize() {
        return 3 * Integer.BYTES;
    }

    @Override
    public boolean isConstant() {
        return originOrdinals.isConstant() && segments.isConstant() && docs.isConstant();
    }

    /**
     * Equal when the rows reference the same documents in the same order. How the dictionaries are laid out and
     * {@link #mayContainDuplicates()} don't matter.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocRefVector == false) {
            return false;
        }
        DocRefVector other = (DocRefVector) obj;
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
    public int hashCode() {
        int result = 1;
        for (int p = 0; p < getPositionCount(); p++) {
            result = 31 * result + origin(p).hashCode();
            result = 31 * result + segments.getInt(p);
            result = 31 * result + docs.getInt(p);
        }
        return result;
    }

    @Override
    public String toString() {
        return "DocRefVector[origins="
            + origins
            + ", originOrdinals="
            + originOrdinals
            + ", segments="
            + segments
            + ", docs="
            + docs
            + ", mayContainDuplicates="
            + mayContainDuplicates
            + ']';
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + origins.ramBytesUsed() + originOrdinals.ramBytesUsed() + segments.ramBytesUsed() + docs.ramBytesUsed();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        super.allowPassingToDifferentDriver();
        originOrdinals.allowPassingToDifferentDriver();
        segments.allowPassingToDifferentDriver();
        docs.allowPassingToDifferentDriver();
    }

    @Override
    public void closeInternal() {
        Releasables.closeExpectNoException(
            () -> blockFactory().adjustBreaker(-BASE_RAM_BYTES_USED - origins.ramBytesUsed()),
            originOrdinals,
            segments,
            docs
        );
    }

    private void assertValid() {
        record Doc(DocRefOrigin origin, int segment, int doc) {}
        Set<Doc> seen = mayContainDuplicates ? null : new HashSet<>(getPositionCount());
        for (int p = 0; p < getPositionCount(); p++) {
            int ordinal = originOrdinals.getInt(p);
            assert ordinal >= 0 && ordinal < origins.size() : "origin ordinal [" + ordinal + "] out of [0, " + origins.size() + ")";
            assert segments.getInt(p) >= 0 : "negative segment [" + segments.getInt(p) + "]";
            assert docs.getInt(p) >= 0 : "negative doc [" + docs.getInt(p) + "]";
            if (seen != null) {
                Doc doc = new Doc(origins.get(ordinal), segments.getInt(p), docs.getInt(p));
                assert seen.add(doc) : "configured not to contain duplicates but " + doc + " was duplicated";
            }
        }
    }
}

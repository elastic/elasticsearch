/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Block view of a {@link $Type$Vector}. Cannot represent multi-values or nulls.
 * This class is generated. Do not edit it.
 */
public final class $Type$VectorBlock extends AbstractVectorBlock implements $Type$Block {

    private final $Type$Vector vector;

    /**
     * @param vector considered owned by the current block; must not be used in any other {@code Block}
     */
    $Type$VectorBlock($Type$Vector vector) {
        this.vector = vector;
    }

    @Override
    public $Type$Vector asVector() {
        return vector;
    }

$if(BytesRef)$
    @Override
    public OrdinalBytesRefBlock asOrdinals() {
        var ordinals = vector.asOrdinals();
        if (ordinals != null) {
            return ordinals.asBlock();
        } else {
            return null;
        }
    }
$endif$

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return vector.getBytesRef(valueIndex, dest);
$else$
    public $type$ get$Type$(int valueIndex) {
        return vector.get$Type$(valueIndex);
$endif$
    }

    @Override
    public int getPositionCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public $Type$Block filter(int... positions) {
        return vector.filter(positions).asBlock();
    }

    @Override
    public $Type$Block keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends $Type$Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return vector.lookup(positions, targetBlockSize);
    }

    @Override
    public $Type$Block expand() {
        incRef();
        return this;
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Block that) {
            return $Type$Block.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Block.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }

    @Override
    public void closeInternal() {
        assert (vector.isReleased() == false) : "can't release block [" + this + "] containing already released vector";
        Releasables.closeExpectNoException(vector);
    }

    @Override
    public void allowPassingToDifferentDriver() {
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }
}

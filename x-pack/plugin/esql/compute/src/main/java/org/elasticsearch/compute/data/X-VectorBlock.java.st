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
import org.elasticsearch.core.Releasables;

/**
 * Block view of a $Type$Vector.
 * This class is generated. Do not edit it.
 */
public sealed class $Type$VectorBlock extends AbstractVectorBlock implements $Type$Block permits $Type$VectorBlock.ShallowCopy {

    final class ShallowCopy extends $Type$VectorBlock {
        /**
         * The vector must be a shallow copy of the original vector.
         */
        ShallowCopy($Type$Vector vector) {
            super(vector);
        }

        @Override
        public void close() {
            released = true;
            $Type$VectorBlock.this.close();
        }
    }

    private final $Type$Vector vector;

    $Type$VectorBlock($Type$Vector vector) {
        super(vector.getPositionCount(), vector.blockFactory());
        this.vector = vector;
    }

    @Override
    public $Type$Vector asVector() {
        return vector;
    }

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
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public $Type$Block filter(int... positions) {
        return new ShallowCopy(new Filter$Type$Vector(vector, positions));
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
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released block [" + this + "]");
        }
        released = true;
        Releasables.closeExpectNoException(vector);
    }
}

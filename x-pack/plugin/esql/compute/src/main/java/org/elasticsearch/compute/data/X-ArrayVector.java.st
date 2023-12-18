/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

$else$
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
$endif$

/**
 * Vector implementation that stores an array of $Type$ values.
 * This class is generated. Do not edit it.
 */
public final class $Type$ArrayVector extends AbstractVector implements $Type$Vector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$ArrayVector.class);

$if(BytesRef)$
    private final BytesRefArray values;

$elseif(Point)$
    private final double[] xValues, yValues;

$else$
    private final $type$[] values;
$endif$

    private final $Type$Block block;

    public $Type$ArrayVector($typeArrayValues$, int positionCount) {
        this($arrayValues$, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public $Type$ArrayVector($typeArrayValues$, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
$if(Point)$
        this.xValues = xValues;
        this.yValues = yValues;
$else$
        this.values = values;
$endif$
        this.block = new $Type$VectorBlock(this);
    }

    @Override
    public $Type$Block asBlock() {
        return block;
    }

$if(BytesRef)$
    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return values.get(position, dest);
    }

$elseif(Point)$
    @Override
    public double getX(int position) {
        return xValues[position];
    }

    @Override
    public double getY(int position) {
        return yValues[position];
    }

$else$
    @Override
    public $type$ get$Type$(int position) {
        return values[position];
    }
$endif$

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public $Type$Vector filter(int... positions) {
    $if(BytesRef)$
        final var scratch = new BytesRef();
    $endif$
        try ($Type$Vector.Builder builder = blockFactory().new$Type$VectorBuilder(positions.length)) {
            for (int pos : positions) {
            $if(BytesRef)$
                builder.append$Type$(values.get(pos, scratch));
            $elseif(Point)$
                builder.append$Type$(xValues[pos], yValues[pos]);
            $else$
                builder.append$Type$(values[pos]);
            $endif$
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated($typeArrayValues$) {
    $if(Point)$
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(xValues) * 2;
    $else$
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    $endif$
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated($arrayValues$);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Vector that) {
            return $Type$Vector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Vector.hash(this);
    }

    @Override
    public String toString() {
$if(BytesRef)$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ']';
$elseif(Point)$
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", x="
            + Arrays.toString(xValues)
            + ", y="
            + Arrays.toString(yValues)
            + ']';
$else$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
$endif$
    }

$if(BytesRef)$
    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        blockFactory().adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed(), true);
        Releasables.closeExpectNoException(values);
    }
$endif$
}

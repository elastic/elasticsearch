/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.$Array$;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed $Type$Array.
 * This class is generated. Do not edit it.
 */
public final class $Type$BigArrayVector extends AbstractVector implements $Type$Vector, Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$BigArrayVector.class);

    private final $Array$ $arrayValues$;

    private final $Type$Block block;

$if(Point)$
    public $Type$BigArrayVector($Array$ xValues, $Array$ yValues, int positionCount) {
$else$
    public $Type$BigArrayVector($Array$ values, int positionCount) {
$endif$
        this($arrayValues$, positionCount, BlockFactory.getNonBreakingInstance());
    }

$if(Point)$
    public $Type$BigArrayVector($Array$ xValues, $Array$ yValues, int positionCount, BlockFactory blockFactory) {
$else$
    public $Type$BigArrayVector($Array$ values, int positionCount, BlockFactory blockFactory) {
$endif$
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

$if(Point)$
    @Override
    public double getX(int position) {
        return xValues.get(position);
    }

    @Override
    public double getY(int position) {
        return yValues.get(position);
    }

$else$
    @Override
    public $type$ get$Type$(int position) {
        return values.get(position);
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
    public long ramBytesUsed() {
$if(Point)$
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(xValues) * 2;
$else$
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
$endif$
    }

    @Override
    public $Type$Vector filter(int... positions) {
        var blockFactory = blockFactory();
    $if(boolean)$
        final BitArray filtered = new BitArray(positions.length, blockFactory.bigArrays());
    $elseif(Point)$
        final $Array$ xFiltered = blockFactory.bigArrays().new$Array$(positions.length);
        final $Array$ yFiltered = blockFactory.bigArrays().new$Array$(positions.length);
    $else$
        final $Array$ filtered = blockFactory.bigArrays().new$Array$(positions.length);
    $endif$
    $if(boolean)$
        for (int i = 0; i < positions.length; i++) {
            if (values.get(positions[i])) {
                filtered.set(i);
            }
        }
    $elseif(Point)$
        for (int i = 0; i < positions.length; i++) {
            xFiltered.set(i, xValues.get(positions[i]));
            yFiltered.set(i, yValues.get(positions[i]));
        }
    $else$
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
    $endif$
    $if(Point)$
        return new $Type$BigArrayVector(xFiltered, yFiltered, positions.length, blockFactory);
    $else$
        return new $Type$BigArrayVector(filtered, positions.length, blockFactory);
    $endif$
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
$if(Point)$
        xValues.close();
        yValues.close();
$else$
        values.close();
$endif$
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
$if(Point)$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", x=" + xValues + ", y=" + yValues + ']';
$else$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
$endif$
    }
}

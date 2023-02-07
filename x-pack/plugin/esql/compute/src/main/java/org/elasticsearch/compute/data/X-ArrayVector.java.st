/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;

$else$
import java.util.Arrays;
$endif$

/**
 * Vector implementation that stores an array of $type$ values.
 * This class is generated. Do not edit it.
 */
public final class $Type$ArrayVector extends AbstractVector implements $Type$Vector {

$if(BytesRef)$
    private final BytesRefArray values;

$else$
    private final $type$[] values;
$endif$

$if(int)$
    /**
     * {@code true} if this every element in this vector is {@code >=}
     * the previous element, {@code false} if there is some element
     * {@code <} a previous element, and {@code null} if it is unknown
     * if either thing is true.
     */
    private Boolean nonDecreasing;
$endif$

$if(BytesRef)$
    public $Type$ArrayVector(BytesRefArray values, int positionCount) {
$elseif(int)$
    public $Type$ArrayVector($type$[] values, int positionCount, Boolean nonDecreasing) {
$else$
    public $Type$ArrayVector($type$[] values, int positionCount) {
$endif$
        super(positionCount);
        this.values = values;
$if(int)$
        this.nonDecreasing = nonDecreasing;
$endif$
    }

    @Override
    public $Type$Block asBlock() {
        return new $Type$VectorBlock(this);
    }

$if(BytesRef)$
    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return values.get(position, dest);
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
        return new Filter$Type$Vector(this, positions);
    }

$if(int)$
    @Override
    public boolean isNonDecreasing() {
        if (nonDecreasing != null) {
            return nonDecreasing;
        }
        int prev = values[0];
        int p = 1;
        while (p < getPositionCount()) {
            if (prev > values[p]) {
                nonDecreasing = false;
                return false;
            }
            prev = values[p];
            p++;
        }
        nonDecreasing = true;
        return true;
    }
$endif$

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
$else$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
$endif$
    }
}

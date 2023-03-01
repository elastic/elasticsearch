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

$if(BytesRef)$
    public $Type$ArrayVector(BytesRefArray values, int positionCount) {
$else$
    public $Type$ArrayVector($type$[] values, int positionCount) {
$endif$
        super(positionCount);
        this.values = values;
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

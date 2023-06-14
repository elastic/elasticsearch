/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;

$else$
import java.util.Arrays;
$endif$

/**
 * Block build of $Type$Blocks.
 * This class is generated. Do not edit it.
 */
final class $Type$VectorBuilder extends AbstractVectorBuilder implements $Type$Vector.Builder {

$if(BytesRef)$
    private BytesRefArray values;

    BytesRefVectorBuilder(int estimatedSize) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE);
    }

    BytesRefVectorBuilder(int estimatedSize, BigArrays bigArrays) {
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

$else$
    private $type$[] values;

    $Type$VectorBuilder(int estimatedSize) {
        values = new $type$[Math.max(estimatedSize, 2)];
    }
$endif$

    @Override
    public $Type$VectorBuilder append$Type$($type$ value) {
        ensureCapacity();
$if(BytesRef)$
        values.append(value);
$else$
        values[valueCount] = value;
$endif$
        valueCount++;
        return this;
    }

    @Override
    protected int valuesLength() {
$if(BytesRef)$
        return Integer.MAX_VALUE; // allow the BytesRefArray through its own append
$else$
        return values.length;
$endif$
    }

    @Override
    protected void growValuesArray(int newSize) {
$if(BytesRef)$
        throw new AssertionError("should not reach here");
$else$
        values = Arrays.copyOf(values, newSize);
$endif$
    }

    @Override
    public $Type$Vector build() {
        if (valueCount == 1) {
$if(BytesRef)$
            return new ConstantBytesRefVector(values.get(0, new BytesRef()), 1);
        }
$else$
            return new Constant$Type$Vector(values[0], 1);
        }
        if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
            values = Arrays.copyOf(values, valueCount);
        }
$endif$
        return new $Type$ArrayVector(values, valueCount);
    }
}

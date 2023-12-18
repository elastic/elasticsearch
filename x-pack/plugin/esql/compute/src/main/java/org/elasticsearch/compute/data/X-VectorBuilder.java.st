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
import org.elasticsearch.core.Releasables;

$else$
import java.util.Arrays;
$endif$

/**
 * Builder for {@link $Type$Vector}s that grows as needed.
 * This class is generated. Do not edit it.
 */
final class $Type$VectorBuilder extends AbstractVectorBuilder implements $Type$Vector.Builder {

$if(BytesRef)$
    private BytesRefArray values;

    BytesRefVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }

    BytesRefVectorBuilder(int estimatedSize, BigArrays bigArrays, BlockFactory blockFactory) {
        super(blockFactory);
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

$elseif(Point)$
    private $type$[] $arrayValues$;

    $Type$VectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(initialSize);
        xValues = new $type$[Math.max(estimatedSize, 2)];
        yValues = new $type$[Math.max(estimatedSize, 2)];
    }

$else$
    private $type$[] $arrayValues$;

    $Type$VectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(initialSize);
        values = new $type$[Math.max(estimatedSize, 2)];
    }
$endif$

    @Override
    public $Type$VectorBuilder append$Type$($typeValue$) {
        ensureCapacity();
$if(BytesRef)$
        values.append(value);
$elseif(Point)$
        xValues[valueCount] = x;
        yValues[valueCount] = y;
$else$
        values[valueCount] = value;
$endif$
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return $if(BytesRef)$-1$else$$BYTES$$endif$;
    }

    @Override
    protected int valuesLength() {
$if(BytesRef)$
        return Integer.MAX_VALUE; // allow the BytesRefArray through its own append
$elseif(Point)$
        return xValues.length;
$else$
        return values.length;
$endif$
    }

    @Override
    protected void growValuesArray(int newSize) {
$if(BytesRef)$
        throw new AssertionError("should not reach here");
$elseif(Point)$
        xValues = Arrays.copyOf(xValues, newSize);
        yValues = Arrays.copyOf(yValues, newSize);
$else$
        values = Arrays.copyOf(values, newSize);
$endif$
    }

    @Override
    public $Type$Vector build() {
        finish();
        $Type$Vector vector;
$if(BytesRef)$
        assert estimatedBytes == 0;
        if (valueCount == 1) {
            vector = new ConstantBytesRefVector(BytesRef.deepCopyOf(values.get(0, new BytesRef())), 1, blockFactory);
            /*
             * Update the breaker with the actual bytes used.
             * We pass false below even though we've used the bytes. That's weird,
             * but if we break here we will throw away the used memory, letting
             * it be deallocated. The exception will bubble up and the builder will
             * still technically be open, meaning the calling code should close it
             * which will return all used memory to the breaker.
             */
            blockFactory.adjustBreaker(vector.ramBytesUsed(), false);
            Releasables.closeExpectNoException(values);
        } else {
            vector = new $Type$ArrayVector(values, valueCount, blockFactory);
            /*
             * Update the breaker with the actual bytes used.
             * We pass false below even though we've used the bytes. That's weird,
             * but if we break here we will throw away the used memory, letting
             * it be deallocated. The exception will bubble up and the builder will
             * still technically be open, meaning the calling code should close it
             * which will return all used memory to the breaker.
             */
            blockFactory.adjustBreaker(vector.ramBytesUsed() - values.bigArraysRamBytesUsed(), false);
        }
        values = null;
$elseif(Point)$
        if (valueCount == 1) {
            vector = blockFactory.newConstant$Type$BlockWith(xValues[0], yValues[0], 1, estimatedBytes).asVector();
        } else {
            if (valuesLength() - valueCount > 1024 || valueCount < (valuesLength() / 2)) {
                growValuesArray(valueCount);
            }
            vector = blockFactory.new$Type$ArrayVector($arrayValues$, valueCount, estimatedBytes);
        }
$else$
        if (valueCount == 1) {
            vector = blockFactory.newConstant$Type$BlockWith(values[0], 1, estimatedBytes).asVector();
        } else {
            if (valuesLength() - valueCount > 1024 || valueCount < (valuesLength() / 2)) {
                growValuesArray(valueCount);
            }
            vector = blockFactory.new$Type$ArrayVector($arrayValues$, valueCount, estimatedBytes);
        }
$endif$
        built();
        return vector;
    }
$if(BytesRef)$

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
$endif$
}

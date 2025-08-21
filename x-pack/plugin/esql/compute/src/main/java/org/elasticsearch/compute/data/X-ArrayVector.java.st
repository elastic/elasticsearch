/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
// end generated imports

/**
 * Vector implementation that stores an array of $type$ values.
$if(BytesRef)$
 * Does not take ownership of the given {@link BytesRefArray} and does not adjust circuit breakers to account for it.
$endif$
 * This class is generated. Edit {@code X-ArrayVector.java.st} instead.
 */
final class $Type$ArrayVector extends AbstractVector implements $Type$Vector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$ArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance($Type$VectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

$if(BytesRef)$
    private final BytesRefArray values;

$else$
    private final $type$[] values;
$endif$

$if(int)$
    /**
     * The minimum value in the block.
     */
    private Integer min;

    /**
     * The minimum value in the block.
     */
    private Integer max;
$endif$

    $Type$ArrayVector($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static $Type$ArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
$if(BytesRef)$
        final BytesRefArray values = new BytesRefArray(in, blockFactory.bigArrays());
        boolean success = false;
        try {
            final var block = new BytesRefArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(block.ramBytesUsed() - values.bigArraysRamBytesUsed());
            success = true;
            return block;
        } finally {
            if (success == false) {
                values.close();
            }
        }
$else$
        final long preAdjustedBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) positions * $BYTES$;
        blockFactory.adjustBreaker(preAdjustedBytes);
        boolean success = false;
        try {
            $type$[] values = new $type$[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = in.read$Type$();
            }
            final var block = new $Type$ArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(block.ramBytesUsed() - preAdjustedBytes);
            success = true;
            return block;
        } finally {
            if (success == false) {
                blockFactory.adjustBreaker(-preAdjustedBytes);
            }
        }
$endif$
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
$if(BytesRef)$
        values.writeTo(out);
$elseif(boolean)$
        // TODO: One bit for each boolean
        for (int i = 0; i < positions; i++) {
            out.writeBoolean(values[i]);
        }
$else$
        for (int i = 0; i < positions; i++) {
            out.write$Type$(values[i]);
        }
$endif$
    }

    @Override
    public $Type$Block asBlock() {
        return new $Type$VectorBlock(this);
    }

$if(BytesRef)$
    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return null;
    }
$endif$

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
    $if(BytesRef)$
        final var scratch = new BytesRef();
    $endif$
        try ($Type$Vector.Builder builder = blockFactory().new$Type$VectorBuilder(positions.length)) {
            for (int pos : positions) {
            $if(BytesRef)$
                builder.append$Type$(values.get(pos, scratch));
            $else$
                builder.append$Type$(values[pos]);
            $endif$
            }
            return builder.build();
        }
    }

    @Override
    public $Type$Block keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new $Type$VectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new $Type$VectorBlock(this);
            }
            return ($Type$Block) blockFactory().newConstantNullBlock(getPositionCount());
        }
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        try ($Type$Block.Builder builder = blockFactory().new$Type$BlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.append$Type$(get$Type$(p$if(BytesRef)$, scratch$endif$));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<$Type$Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new $Type$Lookup(asBlock(), positions, targetBlockSize);
    }

    public static long ramBytesEstimated($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

$if(int)$
    /**
     * The minimum value in the block.
     */
    @Override
    public int min() {
        if (min == null) {
            int v = Integer.MAX_VALUE;
            for (int i = 0; i < getPositionCount(); i++) {
                v = Math.min(v, values[i]);
            }
            min = v;
        }
        return min;
    }

    /**
     * The maximum value in the block.
     */
    @Override
    public int max() {
        if (max == null) {
            int v = Integer.MIN_VALUE;
            for (int i = 0; i < getPositionCount(); i++) {
                v = Math.max(v, values[i]);
            }
            max = v;
        }
        return max;
    }

$elseif(boolean)$
    /**
     * Are all values {@code true}? This will scan all values to check and always answer accurately.
     */
    @Override
    public boolean allTrue() {
        for (int i = 0; i < getPositionCount(); i++) {
            if (values[i] == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Are all values {@code false}? This will scan all values to check and always answer accurately.
     */
    @Override
    public boolean allFalse() {
        for (int i = 0; i < getPositionCount(); i++) {
            if (values[i]) {
                return false;
            }
        }
        return true;
    }
$endif$

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
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
        String valuesString = IntStream.range(0, getPositionCount())
            .limit(10)
            .mapToObj(n -> String.valueOf(values[n]))
            .collect(Collectors.joining(", ", "[", getPositionCount() > 10 ? ", ...]" : "]"));
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + valuesString + ']';
$endif$
    }

$if(BytesRef)$
    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link BytesRefArray} is adjusted outside
        // of this class.
        blockFactory().adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed());
        Releasables.closeExpectNoException(values);
    }
$endif$
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.$Array$;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector implementation that defers to an enclosed {@link $if(boolean)$Bit$else$$Type$$endif$Array}.
 * Does not take ownership of the array and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class $Type$BigArrayVector extends AbstractVector implements $Type$Vector, Releasable {

    private static final long BASE_RAM_BYTES_USED = 0; // FIXME

    private final $Array$ values;

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

    public $Type$BigArrayVector($Array$ values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static $Type$BigArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
$if(float)$
        throw new UnsupportedOperationException();
$else$
$if(boolean)$
        $Array$ values = new BitArray(blockFactory.bigArrays(), true, in);
$else$
        $Array$ values = blockFactory.bigArrays().new$Type$Array(positions, false);
$endif$
        boolean success = false;
        try {
$if(boolean)$$else$
            values.fillWith(in);
$endif$
            $Type$BigArrayVector vector = new $Type$BigArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(vector.ramBytesUsed() - RamUsageEstimator.sizeOf(values));
            success = true;
            return vector;
        } finally {
            if (success == false) {
                values.close();
            }
        }
$endif$
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
$if(float)$
        throw new UnsupportedOperationException();
$else$
        values.writeTo(out);
$endif$
    }

    @Override
    public $Type$Block asBlock() {
        return new $Type$VectorBlock(this);
    }

    @Override
    public $type$ get$Type$(int position) {
        return values.get(position);
    }

$if(int)$
    /**
     * The minimum value in the block.
     */
    @Override
    public int min() {
        if (min == null) {
            int v = values.get(0);
            for (int i = 1; i < getPositionCount(); i++) {
                v = Math.min(v, values.get(i));
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
            int v = values.get(0);
            for (int i = 1; i < getPositionCount(); i++) {
                v = Math.max(v, values.get(i));
            }
            max = v;
        }
        return max;
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
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public $Type$Vector filter(int... positions) {
        var blockFactory = blockFactory();
    $if(boolean)$
        final BitArray filtered = new BitArray(positions.length, blockFactory.bigArrays());
    $else$
        final $Array$ filtered = blockFactory.bigArrays().new$Array$(positions.length);
    $endif$
    $if(boolean)$
        for (int i = 0; i < positions.length; i++) {
            if (values.get(positions[i])) {
                filtered.set(i);
            }
        }
    $else$
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
    $endif$
        return new $Type$BigArrayVector(filtered, positions.length, blockFactory);
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
        try ($Type$Block.Builder builder = blockFactory().new$Type$BlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.append$Type$(get$Type$(p));
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

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link $if(boolean)$Bit$else$$Type$$endif$Array} is adjusted outside
        // of this class.
        values.close();
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
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}

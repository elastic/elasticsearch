/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.common.util.BigArrays;
$if(v1_boolean)$
import org.elasticsearch.common.util.ByteArray;
$elseif(v1_BytesRef)$
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
$else$
import org.elasticsearch.common.util.$v1_Type$Array;
$endif$
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.$v1_Type$Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.$v2_Type$Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;
// end generated imports

/**
 * A time-series aggregation function that collects the $Occurrence$ occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-AllValueByTimestampAggregator.java.st` instead.
 */
@Aggregator(
    processNulls = true,
    value = {
        @IntermediateState(name = "observed", type = "BOOLEAN"),
        @IntermediateState(name = "timestampPresent", type = "BOOLEAN"),
        @IntermediateState(name = "timestamp", type = "$v2_TYPE$"),
        @IntermediateState(name = "values", type = "$v1_TYPE$_BLOCK") }
)
@GroupingAggregator(
    processNulls = true,
    value = {
        @IntermediateState(name = "observed", type = "BOOLEAN_BLOCK"),
        @IntermediateState(name = "timestampsPresent", type = "BOOLEAN_BLOCK"),
        @IntermediateState(name = "timestamps", type = "$v2_TYPE$_BLOCK"),
        @IntermediateState(name = "values", type = "$v1_TYPE$_BLOCK") }
)
public class $Prefix$$Occurrence$$v1_Type$By$v2_Type$Aggregator {
    public static String describe() {
        $if(v1_BytesRef)$
        return "all_$occurrence$_bytesref_by_$v2_type$";
        $else$
        return "all_$occurrence$_$v1_type$_by_$v2_type$";
        $endif$
    }

    public static $Prefix$$v2_Type$$v1_Type$State initSingle(DriverContext driverContext) {
        return new $Prefix$$v2_Type$$v1_Type$State(driverContext.bigArrays());
    }

    $if(v1_long || v1_int || (v1_float && v2_int))$
    private static void overrideState($Prefix$$v2_Type$$v1_Type$State current, boolean timestampPresent, $v2_type$ timestamp, $v1_Type$Block values, int position) {
    $else$
    private static void overrideState(
        $Prefix$$v2_Type$$v1_Type$State current,
        boolean timestampPresent,
        $v2_type$ timestamp,
        $v1_Type$Block values,
        int position
    ) {
    $endif$
        current.observed(true);
        $if(v2_long)$
        current.v1(timestampPresent ? timestamp : -1L);
        $else$
        current.v1(timestampPresent ? timestamp : -1);
        $endif$
        current.v1Seen(timestampPresent);
        if (values.isNull(position)) {
            Releasables.close(current.v2());
            current.v2(null);
        } else {
            int count = values.getValueCount(position);
            int offset = values.getFirstValueIndex(position);
            $if(v1_BytesRef)$
            BytesRefArray a = null;
            $elseif(v1_boolean)$
            ByteArray a = null;
            $else$
            $v1_Type$Array a = null;
            $endif$
            boolean success = false;
            try {
                $if(v1_BytesRef)$
                a = new BytesRefArray(0, current.bigArrays());
                $elseif(v1_boolean)$
                a = current.bigArrays().newByteArray(count);
                $else$
                a = current.bigArrays().new$v1_Type$Array(count);
                $endif$
                for (int i = 0; i < count; ++i) {
                    $if(v1_BytesRef)$
                    BytesRef bytesScratch = new BytesRef();
                    values.getBytesRef(offset + i, bytesScratch);
                    a.append(bytesScratch);
                    $elseif(v1_boolean)$
                    a.set(i, (byte) (values.get$v1_Type$(offset + i) ? 1 : 0));
                    $else$
                    a.set(i, values.get$v1_Type$(offset + i));
                    $endif$
                }
                success = true;
                Releasables.close(current.v2());
                current.v2(a);
            } finally {
                if (success == false) {
                    Releasables.close(a);
                }
            }
        }
    }

    private static $v2_type$ dominantTimestampAtPosition(int position, $v2_Type$Block timestamps) {
        assert timestamps.isNull(position) == false : "The timestamp is null at this position";
        int lo = timestamps.getFirstValueIndex(position);
        int hi = lo + timestamps.getValueCount(position);
        $v2_type$ result = timestamps.get$v2_Type$(lo++);

        for (int i = lo; i < hi; i++) {
            $if(First)$
            result = Math.min(result, timestamps.get$v2_Type$(i));
            $else$
            result = Math.max(result, timestamps.get$v2_Type$(i));
            $endif$
        }

        return result;
    }

    public static void combine($Prefix$$v2_Type$$v1_Type$State current, @Position int position, $v1_Type$Block values, $v2_Type$Block timestamps) {
        $v2_type$ timestamp = timestamps.isNull(position) ? -1 : dominantTimestampAtPosition(position, timestamps);
        boolean timestampPresent = timestamps.isNull(position) == false;

        if (current.observed() == false) {
            // We never saw a timestamp before, regardless of nullability.
            overrideState(current, timestampPresent, timestamp, values, position);
        } else if (timestampPresent && (current.v1Seen() == false || timestamp $if(First)$<$else$>$endif$ current.v1())) {
            // The incoming timestamp wins against the current one because the latter was either null or older/newer.
            overrideState(current, true, timestamp, values, position);
        }
    }

    public static void combineIntermediate(
        $Prefix$$v2_Type$$v1_Type$State current,
        boolean observed,
        boolean timestampPresent,
        $v2_type$ timestamp,
        $v1_Type$Block values
    ) {
        if (observed == false) {
            // The incoming state hasn't observed anything. No work is needed.
            return;
        }

        if (current.observed()) {
            // Both the incoming shard and the current shard observed a value, so we must compare timestamps.
            if (current.v1Seen() == false && timestampPresent == false) {
                // Both observations have null timestamps. No work is needed.
                return;
            }
            if (timestampPresent && (current.v1Seen() == false || timestamp $if(First)$<$else$>$endif$ current.v1())) {
                overrideState(current, timestampPresent, timestamp, values, 0);
            }
        } else {
            // The incoming state has observed a value, but we didn't. So we must update.
            overrideState(current, timestampPresent, timestamp, values, 0);
        }
    }

    public static Block evaluateFinal($Prefix$$v2_Type$$v1_Type$State current, DriverContext ctx) {
        return current.valuesBlock(ctx);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        $if(v1_BytesRef)$
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
        $else$
        return new GroupingState(driverContext.bigArrays());
        $endif$
    }

    public static void combine(GroupingState current, int group, @Position int position, $v1_Type$Block values, $v2_Type$Block timestamps) {
        $if(v2_long)$
        $v2_type$ timestamp = timestamps.isNull(position) ? 0L : dominantTimestampAtPosition(position, timestamps);
        $else$
        $v2_type$ timestamp = timestamps.isNull(position) ? 0 : dominantTimestampAtPosition(position, timestamps);
        $endif$
        current.collectValue(group, timestamps.isNull(position) == false, timestamp, position, values);
    }

    public static void combineIntermediate(
        GroupingState current,
        int group,
        BooleanBlock observed,
        BooleanBlock timestampPresent,
        $v2_Type$Block timestamps,
        $v1_Type$Block values,
        int otherPosition
    ) {
        if (group < observed.getPositionCount() && observed.getBoolean(observed.getFirstValueIndex(otherPosition)) == false) {
            // The incoming state hasn't observed anything for this particular group. No work is needed.
            return;
        }
        $v2_type$ timestamp = timestamps.isNull(otherPosition) ? -1 : timestamps.get$v2_Type$(timestamps.getFirstValueIndex(otherPosition));
        boolean hasTimestamp = timestampPresent.getBoolean(timestampPresent.getFirstValueIndex(otherPosition));
        current.collectValue(group, hasTimestamp, timestamp, otherPosition, values);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractAllBy$v2_Type$GroupingState {

        /**
         * First values, stored in a dense array to minimize per-group overhead.
         */
        $if(v1_BytesRef)$
        private ObjectArray<BreakingBytesRefBuilder> firstValues;
        private final CircuitBreaker breaker;
        $elseif(v1_boolean)$
        private ByteArray firstValues;
        $else$
        private $v1_Type$Array firstValues;
        $endif$

        /**
         * The second-and-beyond values. Null for groups with zero or one value.
         */
        private ObjectArray<$v1_TailArray$> tailValues;

        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays$if(v1_BytesRef)$, CircuitBreaker breaker$endif$) {
            super(bigArrays);
            $if(v1_BytesRef)$
            this.breaker = breaker;
            $endif$
            boolean success = false;
            try {
                $if(v1_BytesRef)$
                this.firstValues = bigArrays.newObjectArray(1);
                this.firstValues.set(0, null);
                $elseif(v1_boolean)$
                this.firstValues = bigArrays.newByteArray(1, false);
                $else$
                this.firstValues = bigArrays.new$v1_Type$Array(1, false);
                $endif$
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(firstValues, super::close);
                }
            }
        }

        void collectValue(int group, boolean timestampPresent, $v2_type$ timestamp, int position, $v1_Type$Block valuesBlock) {
            if (group <= maxGroupId) {
                if (hasValue(group) == false // We never saw this group before, even if it's within bounds.
                    || (nullKey(group) && timestampPresent) // Or, the incoming non-null timestamp wins against the null one in the state.
                    || (timestampPresent && timestamp $if(First)$<$else$>$endif$ key(group)) // Or, we found a better timestamp for this group.
                ) {
                    updateValue(group, timestampPresent, timestamp, valuesBlock, position);
                }
            } else {
                // We must grow all arrays to accommodate this group id
                grow(group);
                maxGroupId = group;
                updateValue(group, timestampPresent, timestamp, valuesBlock, position);
            }
            trackGroupId(group);
        }

        private void updateValue(int group, boolean timestampPresent, $v2_type$ timestamp, $v1_Type$Block valuesBlock, int position) {
            if (valuesBlock.isNull(position)) {
                markNullValue(group);
            } else {
                clearNullValue(group);
            }
            if (timestampPresent == false) {
                markNullKey(group);
            } else {
                clearNullKey(group);
            }
            key(group, timestamp);
            if (valuesBlock.isNull(position) == false) {
                int count = valuesBlock.getValueCount(position);
                int offset = valuesBlock.getFirstValueIndex(position);
                $if(v1_BytesRef)$
                BytesRef firstScratch = new BytesRef();
                valuesBlock.getBytesRef(offset, firstScratch);
                BreakingBytesRefBuilder firstBuilder = firstValues.get(group);
                if (firstBuilder == null) {
                    firstBuilder = new BreakingBytesRefBuilder(breaker, "$Occurrence$", firstScratch.length);
                    firstValues.set(group, firstBuilder);
                }
                firstBuilder.copyBytes(firstScratch);
                $elseif(v1_boolean)$
                firstValues.set(group, (byte) (valuesBlock.getBoolean(offset) ? 1 : 0));
                $else$
                firstValues.set(group, valuesBlock.get$v1_Type$(offset));
                $endif$
                if (count > 1) {
                    $v1_TailArray$ tail = getTailForWriting(group, count - 1);
                    $if(v1_BytesRef)$
                    BytesRef scratch = new BytesRef();
                    for (int i = 1; i < count; ++i) {
                        tail.append(valuesBlock.getBytesRef(offset + i, scratch));
                    }
                    $elseif(v1_boolean)$
                    for (int i = 1; i < count; ++i) {
                        tail.set(i - 1, (byte) (valuesBlock.getBoolean(offset + i) ? 1 : 0));
                    }
                    $else$
                    for (int i = 1; i < count; ++i) {
                        tail.set(i - 1, valuesBlock.get$v1_Type$(offset + i));
                    }
                    $endif$
                } else {
                    clearTailValues(group);
                }
            } else {
                clearTailValues(group);
            }
        }

        @Override
        protected void grow(int group) {
            super.grow(group);
            firstValues = bigArrays.grow(firstValues, group + 1);
        }

        @Override
        public void close() {
            $if(v1_BytesRef)$
            for (long i = 0; i < firstValues.size(); i++) {
                Releasables.close(firstValues.get(i));
            }
            $endif$
            if (tailValues != null) {
                for (long i = 0; i < tailValues.size(); i++) {
                    Releasables.close(tailValues.get(i));
                }
            }
            Releasables.close(firstValues, tailValues, super::close);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var hasTimestampBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var timestampsBuilder = driverContext.blockFactory().new$v2_Type$BlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group <= maxGroupId) {
                        // We must have seen this group before and saved its state
                        observedBlockBuilder.appendBoolean(hasValue(group));
                        hasTimestampBuilder.appendBoolean(nullKey(group) == false);
                        timestampsBuilder.append$v2_Type$(key(group));
                    } else {
                        // Unknown group so we append nulls everywhere
                        observedBlockBuilder.appendBoolean(false);
                        hasTimestampBuilder.appendBoolean(false);
                        timestampsBuilder.appendNull();
                    }
                }

                // Create all intermediate state blocks
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = hasTimestampBuilder.build();
                blocks[offset + 2] = timestampsBuilder.build();
                blocks[offset + 3] = valuesBlock(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return valuesBlock(groups, evalContext.blockFactory());
        }

        private $v1_TailArray$ getTail(int group) {
            if (tailValues == null) {
                return null;
            }
            if (group >= tailValues.size()) {
                return null;
            }
            return tailValues.get(group);
        }

$if(v1_BytesRef)$
        private BytesRefArray getTailForWriting(int group, int count) {
            if (tailValues == null) {
                tailValues = bigArrays.newObjectArray(group + 1);
            } else if (group >= tailValues.size()) {
                tailValues = bigArrays.grow(tailValues, group + 1);
            } else {
                Releasables.close(tailValues.get(group));
            }
            // BytesRefArray is append-only so we always need a fresh one; there is no way to clear and reuse it.
            BytesRefArray tail = new BytesRefArray(count, bigArrays);
            tailValues.set(group, tail);
            return tail;
        }

$else$
        private $v1_TailArray$ getTailForWriting(int group, int count) {
            $v1_TailArray$ existing;
            if (tailValues == null) {
                tailValues = bigArrays.newObjectArray(group + 1);
                existing = null;
            } else if (group >= tailValues.size()) {
                tailValues = bigArrays.grow(tailValues, group + 1);
                existing = null;
            } else {
                existing = tailValues.get(group);
            }
            if (existing == null) {
                $v1_TailArray$ tail = $if(v1_boolean)$bigArrays.newByteArray(count)$else$bigArrays.new$v1_Type$Array(count)$endif$;
                tailValues.set(group, tail);
                return tail;
            }
            if (existing.size() == count) {
                return existing;
            }
            $v1_TailArray$ resized = bigArrays.resize(existing, count);
            tailValues.set(group, resized);
            return resized;
        }
$endif$

        private void clearTailValues(int group) {
            $v1_TailArray$ tail = getTail(group);
            if (tail != null) {
                Releasables.close(tail);
                tailValues.set(group, null);
            }
        }

        private Block valuesBlock(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.new$v1_Type$BlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); p++) {
                    int group = groups.getInt(p);
                    if (group > maxGroupId || hasValue(group) == false || nullValue(group)) {
                        valuesBuilder.appendNull();
                        continue;
                    }
                    $v1_TailArray$ tail = getTail(group);
                    int tailCount = tail == null ? 0 : (int) tail.size();
                    if (tailCount == 0) {
                        $if(v1_BytesRef)$
                        valuesBuilder.appendBytesRef(firstValues.get(group).bytesRefView());
                        $elseif(v1_boolean)$
                        valuesBuilder.appendBoolean(firstValues.get(group) == 1);
                        $else$
                        valuesBuilder.append$v1_Type$(firstValues.get(group));
                        $endif$
                        continue;
                    }
                    valuesBuilder.beginPositionEntry();
                    $if(v1_BytesRef)$
                    valuesBuilder.appendBytesRef(firstValues.get(group).bytesRefView());
                    $elseif(v1_boolean)$
                    valuesBuilder.appendBoolean(firstValues.get(group) == 1);
                    $else$
                    valuesBuilder.append$v1_Type$(firstValues.get(group));
                    $endif$
                    for (int i = 0; i < tailCount; ++i) {
                        $if(v1_BytesRef)$
                        BytesRef bytesScratch = new BytesRef();
                        tail.get(i, bytesScratch);
                        valuesBuilder.appendBytesRef(bytesScratch);
                        $elseif(v1_boolean)$
                        valuesBuilder.appendBoolean(tail.get(i) == 1);
                        $else$
                        valuesBuilder.append$v1_Type$(tail.get(i));
                        $endif$
                    }
                    valuesBuilder.endPositionEntry();
                }
                return valuesBuilder.build();
            }
        }
    }
}

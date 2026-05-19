/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
$if(v_BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
$if(v_boolean)$
import org.elasticsearch.common.util.ByteArray;
$elseif(v_BytesRef)$
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
$else$
import org.elasticsearch.common.util.$v_Type$Array;
$endif$
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
$if(!v_long)$
import org.elasticsearch.compute.data.$v_Type$Block;
import org.elasticsearch.compute.data.IntVector;
$else$
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.$v_Type$Block;
$endif$
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * An aggregator that retrieves the first value it encounters. It is useful in cases where we want to get back the
 * first (or last) value of a field, where the sort field is either null or a constant literal. In such cases, any
 * value from the search field will suffice, and the state needs to be updated at most once.
 *
 * This class is generated. Edit `X-AnyValueAggregator.java.st` instead.
 */
@Aggregator({ @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "$v_TYPE$_BLOCK") })
$if(v_boolean || v_BytesRef)$
@GroupingAggregator(
    { @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "$v_TYPE$_BLOCK") }
)
$else$
@GroupingAggregator({ @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "$v_TYPE$_BLOCK") })
$endif$
public class Any$v_Type$Aggregator {
    public static String describe() {
        return "any_$v_type$_aggregator";
    }

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.bigArrays());
    }

    public static void combine(SingleState current, @Position int position, $v_Type$Block values) {
        if (current.observed) {
            // Short-circuit if we've already observed a value in this state
            return;
        }
        overrideState(current, values, position);
    }

    public static void combineIntermediate(SingleState current, boolean observed, $v_Type$Block values) {
        if (observed == false || current.observed) {
            // We've already observed a value, or the incoming state didn't. In both cases, we can short-circuit.
            return;
        }
        overrideState(current, values, 0);
    }

    public static Block evaluateFinal(SingleState current, DriverContext ctx) {
        return current.intermediateValuesBlockBuilder(ctx);
    }

    private static void overrideState(SingleState current, $v_Type$Block values, int position) {
        current.observed = true;
        if (values.isNull(position)) {
            Releasables.close(current.values);
            current.values = null;
        } else {
            int count = values.getValueCount(position);
            int offset = values.getFirstValueIndex(position);
            $if(v_BytesRef)$
            BytesRefArray a = null;
            $elseif(v_boolean)$
            ByteArray a = null;
            $else$
            $v_Type$Array a = null;
            $endif$
            boolean success = false;
            try {
                $if(v_BytesRef)$
                a = new BytesRefArray(0, current.bigArrays);
                $elseif(v_boolean)$
                a = current.bigArrays.newByteArray(count);
                $else$
                a = current.bigArrays.new$v_Type$Array(count);
                $endif$
                for (int i = 0; i < count; ++i) {
                    $if(v_BytesRef)$
                    BytesRef bytesScratch = new BytesRef();
                    values.getBytesRef(offset + i, bytesScratch);
                    a.append(bytesScratch);
                    $elseif(v_boolean)$
                    a.set(i, (byte) (values.get$v_Type$(offset + i) ? 1 : 0));
                    $else$
                    a.set(i, values.get$v_Type$(offset + i));
                    $endif$
                }
                success = true;
                Releasables.close(current.values);
                current.values = a;
            } finally {
                if (success == false) {
                    Releasables.close(a);
                }
            }
        }
    }

    public static class SingleState implements AggregatorState {
        private final BigArrays bigArrays;
        private boolean observed;
        private $if(v_boolean)$ByteArray$else$$v_Type$Array$endif$ values;

        public SingleState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 2;
            blocks[offset + 0] = driverContext.blockFactory().newConstantBooleanBlockWith(observed, 1);
            blocks[offset + 1] = intermediateValuesBlockBuilder(driverContext);
        }

        Block intermediateValuesBlockBuilder(DriverContext driverContext) {
            if (values == null) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            int size = (int) values.size();
            $if(v_BytesRef)$
            var result = driverContext.blockFactory()
                .newBytesRefArrayBlock(values, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
            values = null; // transfer the ownership of values to the block
            return result;
            $else$
            $v_type$[] vals = new $v_type$[size];
            for (int i = 0; i < size; ++i) {
                vals[i] = $if(v_boolean)$values.get(i) == 1$else$values.get(i)$endif$;
            }
            return driverContext.blockFactory().new$v_Type$ArrayBlock(vals, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
            $endif$
        }

        @Override
        public void close() {
            Releasables.close(values);
        }
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        $if(v_BytesRef)$
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
        $else$
        return new GroupingState(driverContext.bigArrays());
        $endif$
    }

    public static void combine(GroupingState current, int group, @Position int position, $v_Type$Block values) {
        current.collectValue(group, position, values);
    }

    public static void combineIntermediate(GroupingState current, int group, boolean observed, $v_Type$Block values, int otherPosition) {
        if (observed == false) {
            return;
        }
        current.collectValue(group, otherPosition, values);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {

        $if(v_BytesRef)$
        /**
         * First values, stored in a dense array to minimize per-group overhead.
         */
        private ObjectArray<BreakingBytesRefBuilder> firstValues;
        private final CircuitBreaker breaker;

        /**
         * The second-and-beyond values. Null for groups with zero or one value.
         */
        private ObjectArray<BytesRefArray> tailValues;
        $elseif(v_boolean)$
        /**
         * First values, stored in a dense array to minimize per-group overhead.
         */
        private ByteArray firstValues;

        /**
         * The second-and-beyond values. Null for groups with zero or one value.
         */
        private ObjectArray<ByteArray> tailValues;
        $else$
        /**
         * First values, stored in a dense array to minimize per-group overhead.
         */
        private $v_Type$Array firstValues;

        /**
         * The second-and-beyond values. Null for groups with zero or one value.
         */
        private ObjectArray<$v_Type$Array> tailValues;
        $endif$

        /**
         * Lazy; set bit means the group observed a null value.
         */
        private BitArray nullValue;

        /**
         * Highest group id seen so far; -1 when no groups have been processed.
         */
        private int maxGroupId = -1;

        $if(v_BytesRef)$
        GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            super(bigArrays);
            this.breaker = breaker;
        $else$
        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
        $endif$
            boolean success = false;
            try {
                $if(v_BytesRef)$
                this.firstValues = bigArrays.newObjectArray(1);
                this.firstValues.set(0, null);
                $elseif(v_boolean)$
                this.firstValues = bigArrays.newByteArray(1, false);
                $else$
                this.firstValues = bigArrays.new$v_Type$Array(1, false);
                $endif$
                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(firstValues, super::close);
                }
            }
        }

        void collectValue(int group, int position, $v_Type$Block valuesBlock) {
            if (group <= maxGroupId && hasValue(group)) {
                // Short-circuit: we already have a value for this group.
                return;
            }
            if (group > maxGroupId) {
                grow(group);
                maxGroupId = group;
            }
            if (valuesBlock.isNull(position)) {
                setNullValue(group);
            } else {
                int count = valuesBlock.getValueCount(position);
                int offset = valuesBlock.getFirstValueIndex(position);
                $if(v_BytesRef)$
                BytesRef firstScratch = new BytesRef();
                valuesBlock.getBytesRef(offset, firstScratch);
                BreakingBytesRefBuilder firstBuilder = new BreakingBytesRefBuilder(breaker, "any", firstScratch.length);
                firstValues.set(group, firstBuilder);
                firstBuilder.copyBytes(firstScratch);
                if (count > 1) {
                    BytesRefArray tail = getTailForWriting(group, count - 1);
                    BytesRef scratch = new BytesRef();
                    for (int i = 1; i < count; ++i) {
                        tail.append(valuesBlock.getBytesRef(offset + i, scratch));
                    }
                }
                $elseif(v_boolean)$
                firstValues.set(group, (byte) (valuesBlock.getBoolean(offset) ? 1 : 0));
                if (count > 1) {
                    $v_TailArray$ tail = getTailForWriting(group, count - 1);
                    for (int i = 1; i < count; ++i) {
                        tail.set(i - 1, (byte) (valuesBlock.getBoolean(offset + i) ? 1 : 0));
                    }
                }
                $else$
                firstValues.set(group, valuesBlock.get$v_Type$(offset));
                if (count > 1) {
                    $v_TailArray$ tail = getTailForWriting(group, count - 1);
                    for (int i = 1; i < count; ++i) {
                        tail.set(i - 1, valuesBlock.get$v_Type$(offset + i));
                    }
                }
                $endif$
            }
            trackGroupId(group);
        }

        private boolean nullValue(int group) {
            return nullValue != null && nullValue.get(group);
        }

        private void setNullValue(int group) {
            if (nullValue == null) {
                nullValue = new BitArray(group + 1, bigArrays);
            }
            nullValue.set(group);
        }

        private void grow(int group) {
            firstValues = bigArrays.grow(firstValues, group + 1);
        }

        $if(v_BytesRef)$
        private BytesRefArray getTail(int group) {
            if (tailValues == null || group >= tailValues.size()) {
                return null;
            }
            return tailValues.get(group);
        }

        private BytesRefArray getTailForWriting(int group, int count) {
            if (tailValues == null) {
                tailValues = bigArrays.newObjectArray(group + 1);
            } else if (group >= tailValues.size()) {
                tailValues = bigArrays.grow(tailValues, group + 1);
            } else {
                Releasables.close(tailValues.get(group));
            }
            BytesRefArray tail = new BytesRefArray(count, bigArrays);
            tailValues.set(group, tail);
            return tail;
        }
        $else$
        private $v_TailArray$ getTail(int group) {
            if (tailValues == null || group >= tailValues.size()) {
                return null;
            }
            return tailValues.get(group);
        }

        private $v_TailArray$ getTailForWriting(int group, int count) {
            $v_TailArray$ existing;
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
                $if(v_boolean)$
                $v_TailArray$ tail = bigArrays.newByteArray(count);
                $else$
                $v_TailArray$ tail = bigArrays.new$v_Type$Array(count);
                $endif$
                tailValues.set(group, tail);
                return tail;
            }
            if (existing.size() == count) {
                return existing;
            }
            $v_TailArray$ resized = bigArrays.resize(existing, count);
            tailValues.set(group, resized);
            return resized;
        }
        $endif$

        @Override
        public void close() {
            $if(v_BytesRef)$
            for (long i = 0; i < firstValues.size(); i++) {
                Releasables.close(firstValues.get(i));
            }
            $endif$
            if (tailValues != null) {
                for (long i = 0; i < tailValues.size(); i++) {
                    Releasables.close(tailValues.get(i));
                }
            }
            Releasables.close(nullValue, firstValues, tailValues, super::close);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); ++p) {
                    int group = selected.getInt(p);
                    observedBlockBuilder.appendBoolean(group <= maxGroupId && hasValue(group));
                }
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = intermediateValuesBlockBuilder(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return intermediateValuesBlockBuilder(groups, evalContext.blockFactory());
        }

        private Block intermediateValuesBlockBuilder(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.new$v_Type$BlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); ++p) {
                    int group = groups.getInt(p);
                    if (group > maxGroupId || hasValue(group) == false || nullValue(group)) {
                        valuesBuilder.appendNull();
                        continue;
                    }
                    $if(v_BytesRef)$
                    BreakingBytesRefBuilder first = firstValues.get(group);
                    BytesRefArray tail = getTail(group);
                    int tailCount = tail == null ? 0 : (int) tail.size();
                    if (tailCount == 0) {
                        valuesBuilder.appendBytesRef(first.bytesRefView());
                        continue;
                    }
                    valuesBuilder.beginPositionEntry();
                    valuesBuilder.appendBytesRef(first.bytesRefView());
                    for (int i = 0; i < tailCount; ++i) {
                        BytesRef bytesScratch = new BytesRef();
                        tail.get(i, bytesScratch);
                        valuesBuilder.appendBytesRef(bytesScratch);
                    }
                    valuesBuilder.endPositionEntry();
                    $elseif(v_boolean)$
                    ByteArray tail = getTail(group);
                    int tailCount = tail == null ? 0 : (int) tail.size();
                    if (tailCount == 0) {
                        valuesBuilder.appendBoolean(firstValues.get(group) == 1);
                        continue;
                    }
                    valuesBuilder.beginPositionEntry();
                    valuesBuilder.appendBoolean(firstValues.get(group) == 1);
                    for (int i = 0; i < tailCount; ++i) {
                        valuesBuilder.appendBoolean(tail.get(i) == 1);
                    }
                    valuesBuilder.endPositionEntry();
                    $else$
                    $v_Type$Array tail = getTail(group);
                    int tailCount = tail == null ? 0 : (int) tail.size();
                    if (tailCount == 0) {
                        valuesBuilder.append$v_Type$(firstValues.get(group));
                        continue;
                    }
                    valuesBuilder.beginPositionEntry();
                    valuesBuilder.append$v_Type$(firstValues.get(group));
                    for (int i = 0; i < tailCount; ++i) {
                        valuesBuilder.append$v_Type$(tail.get(i));
                    }
                    valuesBuilder.endPositionEntry();
                    $endif$
                }
                return valuesBuilder.build();
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

$if(v_BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
$if(!v_boolean)$
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
        return new GroupingState(driverContext.bigArrays());
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
        private final BigArrays bigArrays;

        /**
         * The group-indexed observed flags
         */
        private ByteArray observed;

        /**
         * The group-indexed values
         */
        private ObjectArray<$if(v_BytesRef)$BytesRefArray$elseif(v_boolean)$ByteArray$else$$v_Type$Array$endif$> values;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            ByteArray observed = null;
            try {
                // Initialize observed
                observed = bigArrays.newByteArray(1, false);
                observed.set(0, (byte) -1);
                this.observed = observed;

                // Initialize values
                this.values = bigArrays.newObjectArray(1);
                this.values.set(0, null);

                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    if (values != null) {
                        for (long i = 0; i < values.size(); ++i) {
                            Releasables.close(values.get(i));
                        }
                    }
                    Releasables.close(observed, values, super::close);
                }
            }
        }

        void collectValue(int group, int position, $v_Type$Block valuesBlock) {
            if (withinBounds(group) && observed.get(group) == 1) {
                // We have already observed this group. We can short-circuit since any value is fine for this aggregator.
                return;
            }
            if (withinBounds(group) == false) {
                observed = bigArrays.grow(observed, group + 1);
                values = bigArrays.grow(values, group + 1);
            }

            // We always want to update here
            observed.set(group, (byte) 1);
            boolean success = false;
            $if(v_BytesRef)$
            BytesRefArray groupValues = null;
            try {
                if (valuesBlock.isNull(position) == false) {
                    int count = valuesBlock.getValueCount(position);
                    int offset = valuesBlock.getFirstValueIndex(position);
                    groupValues = new BytesRefArray(count, bigArrays);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < count; ++i) {
                        groupValues.append(valuesBlock.getBytesRef(i + offset, scratch));
                    }
                }
                success = true;
                Releasables.close(values.get(group));
                values.set(group, groupValues);
            } finally {
                if (success == false) {
                    Releasables.close(groupValues);
                }
            }
            $else$
            $if(v_boolean)$ByteArray$else$$v_Type$Array$endif$ groupValues = null;
            try {
                if (valuesBlock.isNull(position) == false) {
                    int count = valuesBlock.getValueCount(position);
                    int offset = valuesBlock.getFirstValueIndex(position);
                    $if(v_boolean)$
                    groupValues = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(count);
                    for (int i = 0; i < count; ++i) {
                        groupValues.set(i, (byte) (valuesBlock.get$v_Type$(i + offset) ? 1 : 0));
                    }
                    $else$
                    groupValues = bigArrays.new$v_Type$Array(count);
                    for (int i = 0; i < count; ++i) {
                        groupValues.set(i, valuesBlock.get$v_Type$(i + offset));
                    }
                    $endif$
                }
                success = true;
                Releasables.close(values.get(group));
                values.set(group, groupValues);
            } finally {
                if (success == false) {
                    Releasables.close(groupValues);
                }
            }
            $endif$
            trackGroupId(group);
        }

        @Override
        public void close() {
            for (long i = 0; i < values.size(); ++i) {
                Releasables.close(values.get(i));
            }
            Releasables.close(observed, values, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); ++p) {
                    int group = selected.getInt(p);
                    if (withinBounds(group)) {
                        // We must have seen this group before and saved its state
                        observedBlockBuilder.appendBoolean(observed.get(group) == 1);
                    } else {
                        observedBlockBuilder.appendBoolean(false);
                    }
                }

                // Create all intermediate state blocks
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = intermediateValuesBlockBuilder(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return intermediateValuesBlockBuilder(groups, evalContext.blockFactory());
        }

        private boolean withinBounds(int group) {
            return group < Math.min(values.size(), observed.size());
        }

        private Block intermediateValuesBlockBuilder(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.new$v_Type$BlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); ++p) {
                    int group = groups.getInt(p);
                    int count = 0;
                    if (withinBounds(group) && observed.get(group) == 1 && values.get(group) != null) {
                        count = (int) values.get(group).size();
                    }
                    switch (count) {
                        case 0 -> valuesBuilder.appendNull();
                        $if(v_BytesRef)$
                        case 1 -> {
                            BytesRef bytesScratch = new BytesRef();
                            values.get(group).get(0, bytesScratch);
                            valuesBuilder.appendBytesRef(bytesScratch);
                        }
                        $else$
                        $if(v_boolean)$
                        case 1 -> valuesBuilder.append$v_Type$(values.get(group).get(0) == 1);
                        $else$
                        case 1 -> valuesBuilder.append$v_Type$(values.get(group).get(0));
                        $endif$
                        $endif$
                        default -> {
                            valuesBuilder.beginPositionEntry();
                            for (int i = 0; i < count; ++i) {
                                $if(v_BytesRef)$
                                BytesRef bytesScratch = new BytesRef();
                                values.get(group).get(i, bytesScratch);
                                valuesBuilder.appendBytesRef(bytesScratch);
                                $else$
                                $if(v_boolean)$
                                valuesBuilder.append$v_Type$(values.get(group).get(i) == 1);
                                $else$
                                valuesBuilder.append$v_Type$(values.get(group).get(i));
                                $endif$
                                $endif$
                            }
                            valuesBuilder.endPositionEntry();
                        }
                    }
                }
                return valuesBuilder.build();
            }
        }
    }
}

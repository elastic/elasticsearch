/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.function.Function;

public class ValueAt extends EsqlScalarFunction {
    private final Expression index;
    private final Expression values;

    // NOCOMMIT description annotations
    public ValueAt(Source source, Expression index, Expression values) {
        super(source, List.of(index, values));
        this.index = index;
        this.values = values;
    }

    public Expression index() {
        return index;
    }

    public Expression values() {
        return values;
    }

    @Override
    public DataType dataType() {
        return values.dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = TypeResolutions.isInteger(index, sourceText(), TypeResolutions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolutions.isFoldable(values, sourceText(), TypeResolutions.ParamOrdinal.SECOND);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ValueAt(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ValueAt::new, index, values);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        BlockFactory nonBreakingBlockFactory = BlockFactory.getInstance(
            new NoopCircuitBreaker("NOCOMMIT"),
            BigArrays.NON_RECYCLING_INSTANCE
        ); // NOCOMMIT track me
        return switch (PlannerUtils.toElementType(values.dataType())) {
            case BYTES_REF -> new ValueAtBytesRefEvaluator.Factory(
                source(),
                toEvaluator.apply(index),
                valuesAsBytesRefBlock(nonBreakingBlockFactory, values)
            );
            case INT -> new ValueAtIntEvaluator.Factory(
                source(),
                toEvaluator.apply(index),
                valuesAsIntBlock(nonBreakingBlockFactory, values)
            );
            case LONG -> new ValueAtLongEvaluator.Factory(
                source(),
                toEvaluator.apply(index),
                valuesAsLongBlock(nonBreakingBlockFactory, values)
            );
            case DOUBLE -> new ValueAtDoubleEvaluator.Factory(
                source(),
                toEvaluator.apply(index),
                valuesAsDoubleBlock(nonBreakingBlockFactory, values)
            );
            default -> throw new UnsupportedOperationException("NOCOMMIT");
        };
    }

    static BytesRefBlock valuesAsBytesRefBlock(BlockFactory factory, Expression values) {
        Object v = values.fold();
        if (v instanceof List<?> vl) {
            try (BytesRefBlock.Builder b = factory.newBytesRefBlockBuilder(vl.size())) {
                for (int i = 0; i < vl.size(); i++) {
                    b.appendBytesRef((BytesRef) vl.get(i));
                }
                return b.build();
            }
        }
        throw new UnsupportedOperationException("NOCOMMIT think about this");
    }

    static IntBlock valuesAsIntBlock(BlockFactory factory, Expression values) {
        Object v = values.fold();
        if (v instanceof List<?> vl) {
            try (IntBlock.Builder b = factory.newIntBlockBuilder(vl.size())) {
                for (int i = 0; i < vl.size(); i++) {
                    b.appendInt(((Number) vl.get(i)).intValue());
                }
                return b.build();
            }
        }
        throw new UnsupportedOperationException("NOCOMMIT think about this");
    }

    static LongBlock valuesAsLongBlock(BlockFactory factory, Expression values) {
        Object v = values.fold();
        if (v instanceof List<?> vl) {
            try (LongBlock.Builder b = factory.newLongBlockBuilder(vl.size())) {
                for (int i = 0; i < vl.size(); i++) {
                    b.appendLong(((Number) vl.get(i)).longValue());
                }
                return b.build();
            }
        }
        throw new UnsupportedOperationException("NOCOMMIT think about this");
    }

    static DoubleBlock valuesAsDoubleBlock(BlockFactory factory, Expression values) {
        Object v = values.fold();
        if (v instanceof List<?> vl) {
            try (DoubleBlock.Builder b = factory.newDoubleBlockBuilder(vl.size())) {
                for (int i = 0; i < vl.size(); i++) {
                    b.appendDouble(((Number) vl.get(i)).doubleValue());
                }
                return b.build();
            }
        }
        throw new UnsupportedOperationException("NOCOMMIT think about this");
    }

    @Evaluator(extraName = "BytesRef")
    public static void processBytesRef(BytesRefBlock.Builder copyTo, int index, @Fixed BytesRefBlock copyFrom) {
        // TODO call copyFromBlock and copyFromVector directly and pass in fixed scratch
        if (index >= 0 && index < copyFrom.getPositionCount()) {
            copyTo.copyFrom(copyFrom, index, index + 1);
        } else {
            copyTo.appendNull();
        }
    }

    @Evaluator(extraName = "Int")
    public static void processInt(IntBlock.Builder copyTo, int index, @Fixed IntBlock copyFrom) {
        // TODO call copyFromBlock and copyFromVector directly. Maybe copy a single position?
        if (index >= 0 && index < copyFrom.getPositionCount()) {
            copyTo.copyFrom(copyFrom, index, index + 1);
        } else {
            copyTo.appendNull();
        }
    }

    @Evaluator(extraName = "Long")
    public static void processLong(LongBlock.Builder copyTo, int index, @Fixed LongBlock copyFrom) {
        // TODO call copyFromBlock and copyFromVector directly. Maybe copy a single position?
        if (index >= 0 && index < copyFrom.getPositionCount()) {
            copyTo.copyFrom(copyFrom, index, index + 1);
        } else {
            copyTo.appendNull();
        }
    }

    @Evaluator(extraName = "Double")
    public static void processDouble(DoubleBlock.Builder copyTo, int index, @Fixed DoubleBlock copyFrom) {
        // TODO call copyFromBlock and copyFromVector directly. Maybe copy a single position?
        if (index >= 0 && index < copyFrom.getPositionCount()) {
            copyTo.copyFrom(copyFrom, index, index + 1);
        } else {
            copyTo.appendNull();
        }
    }
}

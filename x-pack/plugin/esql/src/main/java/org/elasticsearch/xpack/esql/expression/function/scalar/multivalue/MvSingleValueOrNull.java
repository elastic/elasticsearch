/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;

/**
 * Internal optimizer function that returns a field's value if it is single-valued, or null if it is multi-valued.
 * <p>
 * This is not a user-visible function; it is injected by the query planner to enable rewrites such as:
 * {@code SUM(field + c) → SUM(SINGLE_VALUE_OR_NULL(field)) + c * COUNT(SINGLE_VALUE_OR_NULL(field))}
 * which allows multiple {@code SUM(field + c_i)} expressions to share a single SUM and COUNT computation.
 * </p>
 */
public class MvSingleValueOrNull extends AbstractMultivalueFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvSingleValueOrNull",
        MvSingleValueOrNull::new
    );

    public MvSingleValueOrNull(Source source, Expression field) {
        super(source, field);
    }

    private MvSingleValueOrNull(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(field(), sourceText(), DEFAULT);
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        ElementType elementType = PlannerUtils.toElementType(field().dataType());
        if (elementType == ElementType.NULL) {
            return ConstantEvaluators.CONSTANT_NULL_FACTORY;
        }
        return new Factory(fieldEval);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvSingleValueOrNull(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvSingleValueOrNull::new, field());
    }

    /**
     * Evaluator for {@link MvSingleValueOrNull}.
     * <p>
     * For single-valued blocks the default pass-through in {@link AbstractEvaluator} is used.
     * For multi-valued blocks, each position is emitted as its value if it has exactly one value,
     * or as null if it has zero (already null) or more than one value.
     * </p>
     */
    static class Evaluator extends AbstractEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        Evaluator(DriverContext driverContext, ExpressionEvaluator field) {
            super(driverContext, field);
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
        }

        @Override
        protected String name() {
            return "MvSingleValueOrNull";
        }

        @Override
        protected Block evalNotNullable(Block block) {
            return filterToSingleValue(block);
        }

        @Override
        protected Block evalNullable(Block block) {
            return filterToSingleValue(block);
        }

        private Block filterToSingleValue(Block block) {
            try (Block.Builder builder = block.elementType().newBlockBuilder(block.getPositionCount(), driverContext.blockFactory())) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    if (block.getValueCount(p) == 1) {
                        builder.copyFrom(block, p, p + 1);
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }

    static class Factory implements ExpressionEvaluator.Factory {
        private final ExpressionEvaluator.Factory field;

        Factory(ExpressionEvaluator.Factory field) {
            this.field = field;
        }

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, field.get(context));
        }

        @Override
        public String toString() {
            return "MvSingleValueOrNull[field=" + field + "]";
        }
    }
}

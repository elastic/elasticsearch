/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;

/**
 * Internal optimizer function that returns an expression's value if it is single-valued, or null if it is multi-valued.
 * <p>
 * This is not a user-visible function; it is injected by the query planner to enable rewrites such as:
 * {@code SUM(expr + c) → SUM(SINGLE_VALUE_OR_NULL(expr)) + c * COUNT(SINGLE_VALUE_OR_NULL(expr))}.
 * The wrapped expression is not limited to field references — it can be any expression whose result
 * may be multi-valued.
 * </p>
 */
public class MvSingleValueOrNull extends AbstractMultivalueFunction {
    public static final TransportVersion MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION = TransportVersion.fromName("esql_mv_single_value_or_null");

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
        return new Factory(source(), fieldEval);
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
     * For single-valued blocks the default pass-through in {@link AbstractEvaluator} is used
     * ({@link AbstractEvaluator#evalSingleValuedNotNullable} returns the block as-is).
     * For blocks that may contain multi-valued positions, a boolean mask is built where only
     * single-valued positions are kept, then {@link Block#keepMask} applies it using
     * type-specialized logic.
     * </p>
     */
    static class Evaluator extends AbstractEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final Source source;
        private Warnings warnings;

        Evaluator(DriverContext driverContext, ExpressionEvaluator field, Source source) {
            super(driverContext, field);
            this.source = source;
        }

        private Warnings warnings() {
            if (warnings == null) {
                warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
            }
            return warnings;
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
            return nullifyMultiValued(block);
        }

        @Override
        protected Block evalNullable(Block block) {
            return nullifyMultiValued(block);
        }

        private Block nullifyMultiValued(Block block) {
            try (
                BooleanVector.FixedBuilder maskBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(block.getPositionCount())
            ) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    int valueCount = block.getValueCount(p);
                    if (valueCount > 1) {
                        warnings().registerException(IllegalArgumentException.class, "single-value function encountered multi-value");
                    }
                    maskBuilder.appendBoolean(valueCount == 1);
                }
                try (BooleanVector mask = maskBuilder.build()) {
                    return block.keepMask(mask);
                }
            }
        }
    }

    static class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory field;

        Factory(Source source, ExpressionEvaluator.Factory field) {
            this.source = source;
            this.field = field;
        }

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, field.get(context), source);
        }

        @Override
        public String toString() {
            return "MvSingleValueOrNull[field=" + field + "]";
        }
    }
}

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
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;

/**
 * Internal optimizer function that returns an expression's value if it is single-valued, or null if it is multi-valued.
 * <p>
 * This is not a user-visible function; it is injected by the query planner to enable rewrites such as:
 * </p>
 * <pre>
 *   | STATS s1 = SUM(expr + c1), s2 = SUM(expr + c2)
 *   →
 *   | STATS svSum = SUM(SINGLE_VALUE_OR_NULL(expr)), svCount = COUNT(SINGLE_VALUE_OR_NULL(expr))
 *   | EVAL s1 = svSum + c1 * svCount, s2 = svSum + c2 * svCount
 * </pre>
 * <p>
 * which applies when two or more {@code SUM(expr ± c)} expressions share the same {@code expr}.
 * The wrapped expression is not limited to field references — it can be any expression whose result
 * may be multi-valued.
 * </p>
 * <p>
 * A single node may carry multiple {@link Source} entries in {@code warningSources}, one per
 * original expression that the node represents. On encountering a multi-valued position the
 * evaluator emits a separate warning for each source, so callers that share one node across
 * several expressions still get a distinct warning per expression.
 * </p>
 */
public class MvSingleValueOrNull extends AbstractMultivalueFunction {
    public static final TransportVersion MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION = TransportVersion.fromName(
        "esql_mv_single_value_or_null"
    );

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvSingleValueOrNull",
        MvSingleValueOrNull::new
    );

    private final List<Source> warningSources;

    public List<Source> warningSources() {
        return warningSources;
    }

    public MvSingleValueOrNull(Source source, Expression field) {
        this(source, field, List.of(source));
    }

    public MvSingleValueOrNull(Source source, Expression field, List<Source> warningSources) {
        super(source, field);
        this.warningSources = warningSources;
    }

    private MvSingleValueOrNull(StreamInput in) throws IOException {
        super(in);
        int n = in.readVInt();
        PlanStreamInput planIn = (PlanStreamInput) in;
        warningSources = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            warningSources.add(Source.readFrom(planIn));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(warningSources.size());
        for (Source s : warningSources) {
            s.writeTo(out);
        }
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
        return new Factory(warningSources, fieldEval);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvSingleValueOrNull(source(), newChildren.get(0), warningSources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), warningSources);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        return Objects.equals(warningSources, ((MvSingleValueOrNull) obj).warningSources);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvSingleValueOrNull::new, field(), warningSources);
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
     * <p>
     * When a multi-valued position is encountered, one warning is emitted per entry in
     * {@code warningSources}, so that each original {@code SUM(x ± c)} clause gets its own
     * warning attributed to the correct source location.
     * </p>
     */
    static class Evaluator extends AbstractEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final List<Source> warningSources;
        private List<Warnings> warningsList;

        Evaluator(DriverContext driverContext, ExpressionEvaluator field, List<Source> warningSources) {
            super(driverContext, field);
            this.warningSources = warningSources;
        }

        private List<Warnings> allWarnings() {
            if (warningsList == null) {
                warningsList = new ArrayList<>(warningSources.size());
                for (Source s : warningSources) {
                    warningsList.add(Warnings.createWarnings(driverContext.warningsMode(), s));
                }
            }
            return warningsList;
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
            // Fast path: the block is already guaranteed to be single-valued (e.g. a vector block).
            if (block.mayHaveMultivaluedFields() == false) {
                block.incRef();
                return block;
            }
            boolean anyMultiValued = false;
            try (
                BooleanVector.FixedBuilder maskBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(block.getPositionCount())
            ) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    int valueCount = block.getValueCount(p);
                    if (valueCount > 1) {
                        anyMultiValued = true;
                        for (Warnings w : allWarnings()) {
                            w.registerException(IllegalArgumentException.class, "single-value function encountered multi-value");
                        }
                    }
                    maskBuilder.appendBoolean(valueCount == 1);
                }
                // If no position was actually multivalued, skip keepMask to preserve block structure.
                if (anyMultiValued == false) {
                    block.incRef();
                    return block;
                }
                try (BooleanVector mask = maskBuilder.build()) {
                    return block.keepMask(mask);
                }
            }
        }
    }

    static class Factory implements ExpressionEvaluator.Factory {
        private final List<Source> warningSources;
        private final ExpressionEvaluator.Factory field;

        Factory(List<Source> warningSources, ExpressionEvaluator.Factory field) {
            this.warningSources = warningSources;
            this.field = field;
        }

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, field.get(context), warningSources);
        }

        @Override
        public String toString() {
            return "MvSingleValueOrNull[field=" + field + "]";
        }
    }
}

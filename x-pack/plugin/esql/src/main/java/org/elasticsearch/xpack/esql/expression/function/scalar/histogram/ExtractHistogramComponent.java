/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Extracts a {@link org.elasticsearch.compute.data.ExponentialHistogramBlock.Component} from an exponential histogram.
 * Note that this function is currently only intended for usage in surrogates and not available as a user-facing function.
 * Therefore, it is intentionally not registered in {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 */
public class ExtractHistogramComponent extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ExtractHistogramComponent",
        ExtractHistogramComponent::new
    );

    private final Expression field;
    private final Expression componentOrdinal;

    /**
     * Constructor for this function.
     *
     * @param source the source
     * @param field the exponential_histogram field to extract the value from
     * @param componentOrdinal The {@link org.elasticsearch.compute.data.ExponentialHistogramBlock.Component#ordinal()}
     *                         as integer-expression, must be foldable
     */
    @FunctionInfo(returnType = { "long", "double" })
    public ExtractHistogramComponent(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram" }) Expression field,
        @Param(name = "component", type = { "integer" }) Expression componentOrdinal
    ) {
        super(source, List.of(field, componentOrdinal));
        this.field = field;
        this.componentOrdinal = componentOrdinal;
    }

    private ExtractHistogramComponent(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    public static Expression create(Source source, Expression field, ExponentialHistogramBlock.Component component) {
        return new ExtractHistogramComponent(source, field, new Literal(source, component.ordinal(), INTEGER));
    }

    Expression field() {
        return field;
    }

    Expression componentOrdinal() {
        return componentOrdinal;
    }

    private ExponentialHistogramBlock.Component component() {
        if (componentOrdinal.foldable() == false) {
            throw new EsqlIllegalArgumentException("Received a non-foldable value for component ordinal");
        }
        final Number ordinal = ((Number) componentOrdinal.fold(FoldContext.small()));
        if (ordinal == null) {
            return null;
        }
        return ExponentialHistogramBlock.Component.values()[ordinal.intValue()];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(componentOrdinal);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return switch (component()) {
            case null -> DataType.NULL;
            case MIN, MAX, SUM -> DOUBLE;
            case COUNT -> LONG;
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ExtractHistogramComponent(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ExtractHistogramComponent::new, field, componentOrdinal);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution histoTypeCheck = isType(
            field,
            dt -> dt == DataType.EXPONENTIAL_HISTOGRAM,
            sourceText(),
            FIRST,
            "exponential_histogram"
        );
        TypeResolution componentOrdinalCheck = isType(componentOrdinal, dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer").and(
            isFoldable(componentOrdinal, sourceText(), SECOND)
        );
        return histoTypeCheck.and(componentOrdinalCheck);
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return new EvalOperator.ExpressionEvaluator.Factory() {

            @Override
            public String toString() {
                return "ExtractHistogramComponentEvaluator[" + "field=" + fieldEvaluator + ",component=" + component() + "]";
            }

            @Override
            public EvalOperator.ExpressionEvaluator get(DriverContext context) {
                return new Evaluator(fieldEvaluator.get(context), component());
            }
        };
    }

    private record Evaluator(EvalOperator.ExpressionEvaluator fieldEvaluator, ExponentialHistogramBlock.Component componentToExtract)
        implements
            EvalOperator.ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        @Override
        public Block eval(Page page) {
            try (Block block = fieldEvaluator.eval(page)) {
                return ((ExponentialHistogramBlock) block).buildExponentialHistogramComponentBlock(componentToExtract);
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + fieldEvaluator.baseRamBytesUsed();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(fieldEvaluator);
        }

        @Override
        public String toString() {
            return "ExtractHistogramComponentEvaluator[field=" + fieldEvaluator + ",component=" + componentToExtract + "]";
        }
    }

}

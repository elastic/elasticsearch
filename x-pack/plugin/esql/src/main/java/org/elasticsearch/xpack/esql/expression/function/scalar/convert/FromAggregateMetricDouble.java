/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

public class FromAggregateMetricDouble extends EsqlScalarFunction implements ConvertFunction, BlockLoaderExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FromAggregateMetricDouble",
        FromAggregateMetricDouble::new
    );

    private final Expression field;
    private final Expression subfieldIndex;

    @FunctionInfo(returnType = { "long", "double" }, description = "Convert aggregate double metric to a block of a single subfield.")
    public FromAggregateMetricDouble(
        Source source,
        @Param(
            name = "aggregate_metric_double",
            type = { "aggregate_metric_double", "int", "double", "long" },
            description = "Aggregate double metric to convert."
        ) Expression field,
        @Param(name = "subfieldIndex", type = "int", description = "Index of subfield") Expression subfieldIndex
    ) {
        super(source, List.of(field, subfieldIndex));
        this.field = field;
        this.subfieldIndex = subfieldIndex;
    }

    public static FromAggregateMetricDouble withMetric(Source source, Expression field, AggregateMetricDoubleBlockBuilder.Metric metric) {
        return new FromAggregateMetricDouble(source, field, new Literal(source, metric.getIndex(), INTEGER));
    }

    private FromAggregateMetricDouble(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(subfieldIndex);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (subfieldIndex.foldable() == false) {
            throw new EsqlIllegalArgumentException("Received a non-foldable value for subfield index");
        }
        var folded = subfieldIndex.fold(FoldContext.small());
        if (folded == null) {
            return NULL;
        }
        var subfield = ((Number) folded).intValue();
        if (subfield == AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex()) {
            return INTEGER;
        }
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FromAggregateMetricDouble(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FromAggregateMetricDouble::new, field, subfieldIndex);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(field, dt -> dt == AGGREGATE_METRIC_DOUBLE, sourceText(), DEFAULT, "aggregate_metric_double only");
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
                return "FromAggregateMetricDoubleEvaluator[" + "field=" + fieldEvaluator + ",subfieldIndex=" + subfieldIndex + "]";
            }

            @Override
            public EvalOperator.ExpressionEvaluator get(DriverContext context) {
                final EvalOperator.ExpressionEvaluator eval = fieldEvaluator.get(context);
                final int subFieldIndex = ((Number) subfieldIndex.fold(FoldContext.small())).intValue();
                return new Evaluator(context.blockFactory(), eval, subFieldIndex);
            }
        };
    }

    private record Evaluator(BlockFactory blockFactory, EvalOperator.ExpressionEvaluator eval, int subFieldIndex)
        implements
            EvalOperator.ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        @Override
        public Block eval(Page page) {
            Block block = eval.eval(page);
            try {
                if (block.areAllValuesNull()) {
                    return blockFactory.newConstantNullBlock(block.getPositionCount());
                }
                int actualSubfieldIndex = subFieldIndex;
                if (subFieldIndex == AggregateMetricDoubleBlockBuilder.Metric.DEFAULT.getIndex()) {
                    actualSubfieldIndex = AggregateMetricDoubleBlockBuilder.Metric.MAX.getIndex();
                }
                Block resultBlock = ((AggregateMetricDoubleBlock) block).getMetricBlock(actualSubfieldIndex);
                resultBlock.incRef();
                return resultBlock;
            } finally {
                block.close();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + eval.baseRamBytesUsed();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(eval);
        }

        @Override
        public String toString() {
            return "FromAggregateMetricDoubleEvaluator[field=" + eval + ",subfieldIndex=" + subFieldIndex + "]";
        }
    }

    @Override
    public Expression field() {
        return field;
    }

    @Override
    public Set<DataType> supportedTypes() {
        return Set.of(AGGREGATE_METRIC_DOUBLE, INTEGER, LONG, DOUBLE);
    }

    @Override
    public PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats) {
        if (field() instanceof FieldAttribute f
            && f.dataType() == AGGREGATE_METRIC_DOUBLE
            && (f.field() instanceof MultiTypeEsField) == false) {
            var folded = subfieldIndex.fold(FoldContext.small());
            if (folded == null) {
                throw new IllegalArgumentException("Subfield Index was null");
            }
            var subfield = ((Number) folded).intValue();
            var functionConfig = switch (AggregateMetricDoubleBlockBuilder.Metric.indexToMetric(subfield)) {
                case MIN -> BlockLoaderFunctionConfig.Function.AMD_MIN;
                case MAX -> BlockLoaderFunctionConfig.Function.AMD_MAX;
                case SUM -> BlockLoaderFunctionConfig.Function.AMD_SUM;
                case COUNT -> BlockLoaderFunctionConfig.Function.AMD_COUNT;
                case DEFAULT -> BlockLoaderFunctionConfig.Function.AMD_DEFAULT;
                case null -> throw new IllegalArgumentException("Received invalid subfield index [" + subfield + "].");
            };
            return new PushedBlockLoaderExpression(f, new BlockLoaderFunctionConfig.JustFunction(functionConfig));
        }
        return null;
    }
}

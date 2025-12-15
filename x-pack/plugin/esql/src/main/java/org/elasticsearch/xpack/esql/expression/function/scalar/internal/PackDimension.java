/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.internal;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SOURCE;

/**
 * An internal convert function that packs dimension values into a single BytesRef
 */
public class PackDimension extends UnaryScalarFunction {
    public PackDimension(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(
            field(),
            sourceText(),
            DEFAULT
        );
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PackDimension must be used on the coordinator only");
    }

    @Override
    public DataType dataType() {
        return SOURCE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new PackDimension(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, PackDimension::new, field());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ElementType elementType = PlannerUtils.toElementType(field.dataType());
        return switch (elementType) {
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            case BYTES_REF -> ctx -> new PackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.packBytesValues(ctx, (BytesRefBlock) block)
            );
            case LONG -> ctx -> new PackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.packLongValues(ctx, (LongBlock) block)
            );
            case INT -> ctx -> new PackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.packIntValues(ctx, (IntBlock) block)
            );
            case BOOLEAN -> ctx -> new PackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.packBooleanValues(ctx, (BooleanBlock) block)
            );
            default -> throw new IllegalStateException("unsupported element type [" + field.dataType() + "]");
        };
    }

    record PackValuesEvaluator(EvalOperator.ExpressionEvaluator field, Function<Block, BytesRefBlock> pack)
        implements
            EvalOperator.ExpressionEvaluator {
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PackValuesEvaluator.class);

        @Override
        public BytesRefBlock eval(Page page) {
            try (var fieldVal = field.eval(page)) {
                BytesRefBlock result = pack.apply(fieldVal);
                assert result.doesHaveMultivaluedFields() == false : "packed block must not have multi-valued";
                return result;
            }
        }

        @Override
        public String toString() {
            return "PackDimension[field=" + field + "]";
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
        }

        @Override
        public void close() {
            Releasables.close(field);
        }
    }
}

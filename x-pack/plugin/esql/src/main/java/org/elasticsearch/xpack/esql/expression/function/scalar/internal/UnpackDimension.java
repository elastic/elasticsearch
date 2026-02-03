/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.internal;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.function.Function;

/**
 * An internal convert function that unpacks dimension values were packed by {@link PackDimension}
 */
public final class UnpackDimension extends UnaryScalarFunction {
    private final DataType resultType;

    public UnpackDimension(Source source, Expression field, DataType resultType) {
        super(source, field);
        this.resultType = resultType;
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("UnpackDimension must be used on the coordinator only");
    }

    @Override
    public DataType dataType() {
        return resultType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UnpackDimension(source(), newChildren.getFirst(), resultType);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UnpackDimension::new, field(), dataType());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(resultType)) {
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            case BYTES_REF -> ctx -> new UnpackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.unpackBytesValues(ctx, block)
            );
            case LONG -> ctx -> new UnpackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.unpackLongValues(ctx, block)
            );
            case INT -> ctx -> new UnpackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.unpackIntValues(ctx, block)
            );
            case BOOLEAN -> ctx -> new UnpackValuesEvaluator(
                toEvaluator.apply(field).get(ctx),
                block -> InternalPacks.unpackBooleanValues(ctx, block)
            );
            default -> throw new IllegalStateException("unsupported element type [" + field.dataType() + "]");
        };
    }

    record UnpackValuesEvaluator(EvalOperator.ExpressionEvaluator field, Function<BytesRefBlock, Block> unpack)
        implements
            EvalOperator.ExpressionEvaluator {
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(UnpackValuesEvaluator.class);

        @Override
        public Block eval(Page page) {
            try (var fieldVal = field.eval(page)) {
                assert fieldVal.doesHaveMultivaluedFields() == false : "packed block cannot be multivalued";
                return unpack.apply((BytesRefBlock) fieldVal);
            }
        }

        @Override
        public String toString() {
            return "UnpackDimension[field=" + field + "]";
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

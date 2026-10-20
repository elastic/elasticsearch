/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.session.SessionUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

/**
 * Turns a materialized JOIN subquery into a local-relation join.
 */
public final class SubqueryJoinInliner {
    private SubqueryJoinInliner() {}

    /**
     * Creates a request-breaker-backed factory whose primitive blocks stay serializable at the local-relation size limit.
     */
    public static BlockFactory blockFactory(BlockFactory blockFactory) {
        BlockFactory parent = blockFactory.parent();
        return BlockFactory.builder(parent.bigArrays())
            .breaker(parent.breaker())
            .maxPrimitiveArraySize(Integer.MAX_VALUE)
            .bytesRefRamOverestimateThreshold(parent.bytesRefRamOverestimateThreshold())
            .bytesRefRamOverestimateFactor(parent.bytesRefRamOverestimateFactor())
            .build();
    }

    static LogicalPlan inline(SubqueryJoin join, LocalRelation data, SubqueryResultContext context) {
        List<Attribute> leftFields = join.config().leftFields();
        List<Attribute> rightFields = join.config().rightFields();
        if (leftFields.isEmpty() || leftFields.size() != rightFields.size()) {
            throw new IllegalStateException("JOIN subquery requires matching, non-empty left and right keys");
        }
        if (leftFields.stream().anyMatch(field -> field.dataType() == DataType.NULL)) {
            releasePage(context.pageHolder());
            return noMatchPlan(join);
        }

        Page sourcePage = data.supplier().get();
        Page resultPage = null;
        Block[] resultBlocks = null;
        boolean transferred = false;
        try {
            List<Attribute> schema = data.output();
            if (sourcePage.getBlockCount() != schema.size()) {
                throw new IllegalStateException(
                    "materialized subquery join schema has ["
                        + schema.size()
                        + "] columns but its page has ["
                        + sourcePage.getBlockCount()
                        + "]"
                );
            }
            Map<NameId, Integer> columnById = new HashMap<>();
            for (int i = 0; i < schema.size(); i++) {
                columnById.put(schema.get(i).id(), i);
            }
            int[] keyColumns = new int[rightFields.size()];
            for (int i = 0; i < rightFields.size(); i++) {
                Integer column = columnById.get(rightFields.get(i).id());
                if (column == null) {
                    throw new IllegalStateException("materialized subquery join is missing right key [" + rightFields.get(i).name() + "]");
                }
                keyColumns[i] = column;
            }

            int[] keptPositions = new int[sourcePage.getPositionCount()];
            int keptPositionCount = 0;
            boolean hasMultiValuedKeys = false;
            for (int p = 0; p < sourcePage.getPositionCount(); p++) {
                boolean matchable = true;
                for (int column : keyColumns) {
                    int valueCount = sourcePage.getBlock(column).getValueCount(p);
                    hasMultiValuedKeys |= valueCount > 1;
                    matchable &= valueCount == 1;
                }
                if (matchable) {
                    keptPositions[keptPositionCount++] = p;
                }
            }
            if (hasMultiValuedKeys) {
                warnMultiValuedRightKey(join.source());
            }
            if (keptPositionCount == 0) {
                releasePage(context.pageHolder());
                return noMatchPlan(join);
            }
            keptPositions = Arrays.copyOf(keptPositions, keptPositionCount);

            List<Attribute> normalizedSchema = new ArrayList<>(schema);
            List<Attribute> normalizedRightFields = new ArrayList<>(rightFields);
            List<Attribute> normalizedLeftFields = new ArrayList<>(leftFields);
            Map<Integer, ElementType> castsByColumn = new HashMap<>();
            List<Alias> leftKeyCasts = new ArrayList<>();
            for (int i = 0; i < leftFields.size(); i++) {
                Attribute leftField = leftFields.get(i);
                Attribute rightField = rightFields.get(i);
                if (leftField.dataType().isNumeric() && rightField.dataType().isNumeric()) {
                    DataType commonDataType = commonType(leftField.dataType(), rightField.dataType());
                    if (commonDataType == null) {
                        throw new IllegalStateException(
                            "incompatible numeric JOIN key types [" + leftField.dataType() + "] and [" + rightField.dataType() + "]"
                        );
                    }
                    ElementType commonElementType = numericElementType(commonDataType);
                    if (numericElementType(leftField.dataType()) != commonElementType) {
                        Alias cast = new Alias(
                            join.source(),
                            "$$subquery_join_key_" + i,
                            castNumericExpression(join.source(), leftField, commonElementType),
                            null,
                            true
                        );
                        leftKeyCasts.add(cast);
                        normalizedLeftFields.set(i, cast.toAttribute());
                    }

                    int rightColumn = keyColumns[i];
                    Block rightKeyBlock = sourcePage.getBlock(rightColumn);
                    if (rightKeyBlock.elementType() != commonElementType) {
                        castsByColumn.put(rightColumn, commonElementType);
                    }
                    DataType normalizedType = dataTypeFor(commonElementType);
                    if (rightField.dataType() != normalizedType) {
                        Attribute normalized = new ReferenceAttribute(
                            rightField.source(),
                            rightField.qualifier(),
                            rightField.name(),
                            normalizedType,
                            rightField.nullable(),
                            rightField.id(),
                            rightField.synthetic()
                        );
                        normalizedSchema.set(rightColumn, normalized);
                        normalizedRightFields.set(i, normalized);
                    }
                }
            }

            resultBlocks = new Block[schema.size()];
            for (int column = 0; column < schema.size(); column++) {
                Block filtered = sourcePage.getBlock(column).filter(false, keptPositions);
                ElementType targetType = castsByColumn.get(column);
                if (targetType == null) {
                    resultBlocks[column] = filtered;
                } else {
                    try {
                        resultBlocks[column] = castNumericBlock(
                            filtered,
                            schema.get(column).dataType(),
                            targetType,
                            context.blockFactory()
                        );
                    } finally {
                        Releasables.closeExpectNoException(filtered);
                    }
                }
            }
            resultPage = new Page(resultBlocks);
            resultBlocks = null;
            SessionUtils.checkPagesBelowSize(
                List.of(resultPage),
                context.intermediateLocalRelationMaxSize(),
                actual -> "sub-plan execution results too large ["
                    + ByteSizeValue.ofBytes(actual)
                    + "] > "
                    + context.intermediateLocalRelationMaxSize()
            );

            LocalRelation normalizedData = new LocalRelation(join.source(), normalizedSchema, LocalSupplier.of(resultPage));
            transferPageOwnership(context.pageHolder(), resultPage);
            resultPage = null;
            transferred = true;

            LogicalPlan left = join.left();
            if (leftKeyCasts.isEmpty() == false) {
                left = new Eval(join.source(), left, leftKeyCasts);
            }
            MaterializedSubqueryJoin materializedJoin = new MaterializedSubqueryJoin(
                join.source(),
                left,
                normalizedData,
                join.config().type(),
                normalizedLeftFields,
                normalizedRightFields,
                join.config().joinOnConditions(),
                join.executesOn()
            );
            return leftKeyCasts.isEmpty() ? materializedJoin : new Project(join.source(), materializedJoin, join.output());
        } finally {
            if (resultBlocks != null) {
                Releasables.closeExpectNoException(resultBlocks);
            }
            Releasables.closeExpectNoException(resultPage);
            if (transferred == false) {
                releasePage(context.pageHolder());
            }
        }
    }

    private static LogicalPlan noMatchPlan(SubqueryJoin join) {
        LogicalPlan left = join.config().type() == JoinTypes.INNER
            ? new Filter(join.source(), join.left(), new Literal(join.source(), false, DataType.BOOLEAN))
            : join.left();
        List<Attribute> rightOutputFields = join.rightOutputFields();
        if (rightOutputFields.isEmpty()) {
            return left;
        }
        List<Alias> nullPayloads = new ArrayList<>(rightOutputFields.size());
        for (Attribute field : rightOutputFields) {
            nullPayloads.add(
                new Alias(join.source(), field.name(), new Literal(join.source(), null, field.dataType()), field.id(), field.synthetic())
            );
        }
        Eval eval = new Eval(join.source(), left, nullPayloads);
        return new Project(join.source(), eval, join.output());
    }

    private static void warnMultiValuedRightKey(Source source) {
        HeaderWarning.addWarning(
            "Line {}:{}: evaluation of [{}] failed, treating result as null. Only first 20 failures recorded",
            source.lineNumber(),
            source.columnNumber(),
            source.text()
        );
        HeaderWarning.addWarning(
            "Line {}:{}: {}: JOIN subquery encountered multi-value",
            source.lineNumber(),
            source.columnNumber(),
            IllegalArgumentException.class.getName()
        );
    }

    private static ElementType numericElementType(DataType dataType) {
        return switch (dataType) {
            case BYTE, SHORT, INTEGER, COUNTER_INTEGER -> ElementType.INT;
            case LONG, UNSIGNED_LONG, COUNTER_LONG -> ElementType.LONG;
            case DOUBLE, FLOAT, HALF_FLOAT, SCALED_FLOAT, COUNTER_DOUBLE -> ElementType.DOUBLE;
            default -> throw new IllegalArgumentException("not a numeric type [" + dataType + "]");
        };
    }

    private static DataType dataTypeFor(ElementType elementType) {
        return switch (elementType) {
            case INT -> DataType.INTEGER;
            case LONG -> DataType.LONG;
            case DOUBLE -> DataType.DOUBLE;
            default -> throw new IllegalArgumentException("not a numeric element type [" + elementType + "]");
        };
    }

    private static Expression castNumericExpression(Source source, Attribute field, ElementType targetType) {
        return switch (targetType) {
            case INT -> new ToInteger(source, field);
            case LONG -> new ToLong(source, field);
            case DOUBLE -> new ToDouble(source, field);
            default -> throw new IllegalArgumentException("not a numeric element type [" + targetType + "]");
        };
    }

    private static Block castNumericBlock(Block block, DataType sourceType, ElementType targetType, BlockFactory blockFactory) {
        if (block.getPositionCount() == 0) {
            try (Block.Builder builder = targetType.newBlockBuilder(0, blockFactory)) {
                return builder.build();
            }
        }
        return switch (targetType) {
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(block.getPositionCount())) {
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        int valueIndex = singleValueIndex(block, p);
                        if (block.elementType() == ElementType.INT) {
                            builder.appendLong(((IntBlock) block).getInt(valueIndex));
                        } else {
                            throw new IllegalStateException("cannot widen numeric key block [" + block.elementType() + "] to [LONG]");
                        }
                    }
                    yield builder.build();
                }
            }
            case DOUBLE -> {
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(block.getPositionCount())) {
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        int valueIndex = singleValueIndex(block, p);
                        double value = switch (block.elementType()) {
                            case INT -> ((IntBlock) block).getInt(valueIndex);
                            case LONG -> sourceType == DataType.UNSIGNED_LONG
                                ? EsqlDataTypeConverter.unsignedLongToDouble(((LongBlock) block).getLong(valueIndex))
                                : ((LongBlock) block).getLong(valueIndex);
                            default -> throw new IllegalStateException(
                                "cannot widen numeric key block [" + block.elementType() + "] to [DOUBLE]"
                            );
                        };
                        builder.appendDouble(value);
                    }
                    yield builder.build();
                }
            }
            default -> throw new IllegalArgumentException("unsupported numeric key block conversion to [" + targetType + "]");
        };
    }

    private static int singleValueIndex(Block block, int position) {
        if (block.getValueCount(position) != 1) {
            throw new IllegalStateException("normalized subquery join key must have exactly one value");
        }
        return block.getFirstValueIndex(position);
    }

    private static void transferPageOwnership(AtomicReference<Page> pageHolder, Page replacement) {
        if (pageHolder != null) {
            Page original = pageHolder.getAndSet(replacement);
            Releasables.closeExpectNoException(original);
        }
    }

    private static void releasePage(AtomicReference<Page> pageHolder) {
        if (pageHolder != null) {
            Releasables.closeExpectNoException(pageHolder.getAndSet(null));
        }
    }
}

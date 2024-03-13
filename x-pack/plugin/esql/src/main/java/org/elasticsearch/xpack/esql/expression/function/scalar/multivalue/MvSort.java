/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.MultivalueDedupeBoolean;
import org.elasticsearch.compute.operator.MultivalueDedupeBytesRef;
import org.elasticsearch.compute.operator.MultivalueDedupeDouble;
import org.elasticsearch.compute.operator.MultivalueDedupeInt;
import org.elasticsearch.compute.operator.MultivalueDedupeLong;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failures;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Sorts a multivalued field in lexicographical order.
 */
public class MvSort extends EsqlScalarFunction implements OptionalArgument, Validatable {
    private final Expression field, order;

    private static final Literal ASC = new Literal(Source.EMPTY, "ASC", DataTypes.KEYWORD);

    @FunctionInfo(
        returnType = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
        description = "Sorts a multivalued field in lexicographical order."
    )
    public MvSort(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "A multivalued field"
        ) Expression field,
        @Param(name = "order", type = { "keyword" }, description = "sort order", optional = true) Expression order
    ) {
        super(source, order == null ? Arrays.asList(field, ASC) : Arrays.asList(field, order));
        this.field = field;
        this.order = order == null ? ASC : order;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(field, EsqlDataTypes::isRepresentable, sourceText(), FIRST, "representable");

        if (resolution.unresolved()) {
            return resolution;
        }

        return isString(order, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return field.foldable() && (order == null || order.foldable());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        boolean ordering = order.foldable() && ((BytesRef) order.fold()).utf8ToString().equalsIgnoreCase("DESC") ? false : true;
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case BOOLEAN -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                ordering,
                (blockFactory, fieldBlock, sortOrder) -> new MultivalueDedupeBoolean((BooleanBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    sortOrder
                ),
                ElementType.BOOLEAN
            );
            case BYTES_REF -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                ordering,
                (blockFactory, fieldBlock, sortOrder) -> new MultivalueDedupeBytesRef((BytesRefBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    sortOrder
                ),
                ElementType.BYTES_REF
            );
            case INT -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                ordering,
                (blockFactory, fieldBlock, sortOrder) -> new MultivalueDedupeInt((IntBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    sortOrder
                ),
                ElementType.INT
            );
            case LONG -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                ordering,
                (blockFactory, fieldBlock, sortOrder) -> new MultivalueDedupeLong((LongBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    sortOrder
                ),
                ElementType.LONG
            );
            case DOUBLE -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                ordering,
                (blockFactory, fieldBlock, sortOrder) -> new MultivalueDedupeDouble((DoubleBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    sortOrder
                ),
                ElementType.DOUBLE
            );
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("unsupported type [" + field.dataType() + "]");
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvSort(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvSort::new, field, order);
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, order);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MvSort other = (MvSort) obj;
        return Objects.equals(other.field, field) && Objects.equals(other.order, order);
    }

    @Override
    public void validate(Failures failures) {
        String operation = sourceText();
        failures.add(isFoldable(order, operation, SECOND));
    }

    private record EvaluatorFactory(
        EvalOperator.ExpressionEvaluator.Factory field,
        boolean order,
        TriFunction<BlockFactory, Block, Boolean, Block> sort,
        ElementType dataType
    ) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new MvSort.Evaluator(context.blockFactory(), field.get(context), order, sort, dataType);
        }

        @Override
        public String toString() {
            return "MvSort" + dataType + "[field=" + field + ", order=" + order + "]";
        }
    }

    private static class Evaluator implements EvalOperator.ExpressionEvaluator {
        private final BlockFactory blockFactory;
        private final EvalOperator.ExpressionEvaluator field;
        private final boolean order;
        private final TriFunction<BlockFactory, Block, Boolean, Block> sort;
        private final ElementType dataType;

        protected Evaluator(
            BlockFactory blockFactory,
            EvalOperator.ExpressionEvaluator field,
            boolean order,
            TriFunction<BlockFactory, Block, Boolean, Block> sort,
            ElementType dataType
        ) {
            this.blockFactory = blockFactory;
            this.field = field;
            this.order = order;
            this.sort = sort;
            this.dataType = dataType;
        }

        @Override
        public Block eval(Page page) {
            try (Block fieldBlock = field.eval(page)) {
                return sort.apply(blockFactory, fieldBlock, order);
            }
        }

        @Override
        public String toString() {
            return "MvSort" + dataType + "[field=" + field + ", order=" + order + "]";
        }

        @Override
        public void close() {}
    }
}

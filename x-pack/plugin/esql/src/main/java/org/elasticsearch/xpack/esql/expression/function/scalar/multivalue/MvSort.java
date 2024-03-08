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
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.common.Failures;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
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
public class MvSort extends ScalarFunction implements OptionalArgument, EvaluatorMapper, Validatable {
    private final Expression field, order;

    private static final Literal ASC = new Literal(Source.EMPTY, "ASC", DataTypes.TEXT);

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

        if (order != null) {
            resolution = isString(order, sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolution;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && (order == null || order.foldable());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case BOOLEAN -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                toEvaluator.apply(order),
                (blockFactory, fieldBlock, ascending) -> new MultivalueDedupeBoolean((BooleanBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    ascending
                )
            );
            case BYTES_REF -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                toEvaluator.apply(order),
                (blockFactory, fieldBlock, ascending) -> new MultivalueDedupeBytesRef((BytesRefBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    ascending
                )
            );
            case INT -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                toEvaluator.apply(order),
                (blockFactory, fieldBlock, ascending) -> new MultivalueDedupeInt((IntBlock) fieldBlock).sortToBlock(blockFactory, ascending)
            );
            case LONG -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                toEvaluator.apply(order),
                (blockFactory, fieldBlock, ascending) -> new MultivalueDedupeLong((LongBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    ascending
                )
            );
            case DOUBLE -> new MvSort.EvaluatorFactory(
                toEvaluator.apply(field),
                toEvaluator.apply(order),
                (blockFactory, fieldBlock, ascending) -> new MultivalueDedupeDouble((DoubleBlock) fieldBlock).sortToBlock(
                    blockFactory,
                    ascending
                )
            );
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("unsupported type [" + field.dataType() + "]");
        };
    }

    private record EvaluatorFactory(
        EvalOperator.ExpressionEvaluator.Factory field,
        EvalOperator.ExpressionEvaluator.Factory order,
        TriFunction<BlockFactory, Block, Boolean, Block> sort
    ) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new MvSort.Evaluator(context.blockFactory(), field.get(context), order.get(context), sort);
        }

        @Override
        public String toString() {
            return "MvSort[field=" + field + ", order=" + order + "]";
        }
    }

    private static class Evaluator implements EvalOperator.ExpressionEvaluator {
        private final BlockFactory blockFactory;
        private final EvalOperator.ExpressionEvaluator field;
        private final EvalOperator.ExpressionEvaluator order;
        private final TriFunction<BlockFactory, Block, Boolean, Block> sort;

        protected Evaluator(
            BlockFactory blockFactory,
            EvalOperator.ExpressionEvaluator field,
            EvalOperator.ExpressionEvaluator order,
            TriFunction<BlockFactory, Block, Boolean, Block> sort
        ) {
            this.blockFactory = blockFactory;
            this.field = field;
            this.order = order;
            this.sort = sort;
        }

        @Override
        public Block eval(Page page) {
            try (Block fieldBlock = field.eval(page)) {
                try (BytesRefBlock orderBlock = (BytesRefBlock) order.eval(page)) {
                    boolean ascending = orderBlock.areAllValuesNull();  // if all are nulls, default is ascending
                    if (ascending == false) { // the first not null value decides the order
                        for (int p = 0; p < orderBlock.getPositionCount(); p++) {
                            int count = orderBlock.getValueCount(p);
                            if (count > 0) {
                                BytesRef orderScratch = new BytesRef();
                                BytesRef sortOrder = orderBlock.getBytesRef(orderBlock.getFirstValueIndex(p), orderScratch);
                                ascending = sortOrder.utf8ToString().equalsIgnoreCase("DESC") == false;
                                break;
                            }
                        }
                    }
                    return sort.apply(blockFactory, fieldBlock, ascending);
                }
            }
        }

        @Override
        public String toString() {
            return "MvSort[field=" + field + ", order=" + order + "]";
        }

        @Override
        public void close() {}
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
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
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
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
}

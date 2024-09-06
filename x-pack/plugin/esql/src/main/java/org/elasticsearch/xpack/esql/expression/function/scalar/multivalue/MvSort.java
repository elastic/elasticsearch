/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
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
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBytesRef;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeDouble;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeLong;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;

/**
 * Sorts a multivalued field in lexicographical order.
 */
public class MvSort extends EsqlScalarFunction implements OptionalArgument, Validatable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvSort", MvSort::new);

    private final Expression field, order;

    private static final Literal ASC = new Literal(Source.EMPTY, "ASC", DataType.KEYWORD);
    private static final Literal DESC = new Literal(Source.EMPTY, "DESC", DataType.KEYWORD);

    private static final String INVALID_ORDER_ERROR = "Invalid order value in [{}], expected one of [{}, {}] but got [{}]";

    @FunctionInfo(
        returnType = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
        description = "Sorts a multivalued field in lexicographical order.",
        examples = @Example(file = "ints", tag = "mv_sort")
    )
    public MvSort(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "Multivalue expression. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "order",
            type = { "keyword" },
            description = "Sort order. The valid options are ASC and DESC, the default is ASC.",
            optional = true
        ) Expression order
    ) {
        super(source, order == null ? Arrays.asList(field) : Arrays.asList(field, order));
        this.field = field;
        this.order = order;
    }

    private MvSort(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeOptionalNamedWriteable(order);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression field() {
        return field;
    }

    Expression order() {
        return order;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(field, DataType::isRepresentable, sourceText(), FIRST, "representable");

        if (resolution.unresolved()) {
            return resolution;
        }

        if (order == null) {
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
        boolean ordering = true;
        if (isValidOrder() == false) {
            throw new IllegalArgumentException(
                LoggerMessageFormat.format(
                    null,
                    INVALID_ORDER_ERROR,
                    sourceText(),
                    ASC.value(),
                    DESC.value(),
                    ((BytesRef) order.fold()).utf8ToString()
                )
            );
        }
        if (order != null && order.foldable()) {
            ordering = ((BytesRef) order.fold()).utf8ToString().equalsIgnoreCase((String) ASC.value());
        }

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
    public void validate(Failures failures) {
        if (order == null) {
            return;
        }
        String operation = sourceText();
        failures.add(isFoldable(order, operation, SECOND));
        if (isValidOrder() == false) {
            failures.add(
                Failure.fail(order, INVALID_ORDER_ERROR, sourceText(), ASC.value(), DESC.value(), ((BytesRef) order.fold()).utf8ToString())
            );
        }
    }

    private boolean isValidOrder() {
        boolean isValidOrder = true;
        if (order != null && order.foldable()) {
            Object obj = order.fold();
            String o = null;
            if (obj instanceof BytesRef ob) {
                o = ob.utf8ToString();
            } else if (obj instanceof String os) {
                o = os;
            }
            if (o == null || o.equalsIgnoreCase((String) ASC.value()) == false && o.equalsIgnoreCase((String) DESC.value()) == false) {
                isValidOrder = false;
            }
        }
        return isValidOrder;
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
            return "MvSort" + dataType.pascalCaseName() + "[field=" + field + ", order=" + order + "]";
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
            return "MvSort" + dataType.pascalCaseName() + "[field=" + field + ", order=" + order + "]";
        }

        @Override
        public void close() {}
    }
}

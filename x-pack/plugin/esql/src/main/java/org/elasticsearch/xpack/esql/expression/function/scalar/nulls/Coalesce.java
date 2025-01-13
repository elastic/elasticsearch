/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Function returning the first non-null value.
 */
public class Coalesce extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Coalesce", Coalesce::new);

    private DataType dataType;

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date_nanos",
            "date",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "version" },
        description = "Returns the first of its arguments that is not null. If all arguments are null, it returns `null`.",
        examples = { @Example(file = "null", tag = "coalesce") }
    )
    public Coalesce(
        Source source,
        @Param(
            name = "first",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date_nanos",
                "date",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "Expression to evaluate."
        ) Expression first,
        @Param(
            name = "rest",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date_nanos",
                "date",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "Other expression to evaluate.",
            optional = true
        ) List<Expression> rest
    ) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    private Coalesce(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteableCollection(children().subList(1, children().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        for (int position = 0; position < children().size(); position++) {
            if (dataType == null || dataType == NULL) {
                dataType = children().get(position).dataType().noText();
                continue;
            }
            TypeResolution resolution = TypeResolutions.isType(
                children().get(position),
                t -> t.noText() == dataType,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(position),
                dataType.typeName()
            );
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Nullability nullable() {
        // If any of the children aren't nullable then this isn't.
        for (Expression c : children()) {
            if (c.nullable() == Nullability.FALSE) {
                return Nullability.FALSE;
            }
        }
        /*
         * Otherwise let's call this one "unknown". If we returned TRUE here
         * an optimizer rule would replace this with null if any of our children
         * fold to null. We don't want that at all.
         */
        return Nullability.UNKNOWN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Coalesce(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Coalesce::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<ExpressionEvaluator.Factory> childEvaluators = children().stream().map(toEvaluator::apply).toList();
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new CoalesceEvaluator(
                    context,
                    PlannerUtils.toElementType(dataType()),
                    childEvaluators.stream().map(x -> x.get(context)).toList()
                );
            }

            @Override
            public String toString() {
                return "CoalesceEvaluator[values=" + childEvaluators + ']';
            }
        };
    }

    private record CoalesceEvaluator(DriverContext driverContext, ElementType resultType, List<EvalOperator.ExpressionEvaluator> evaluators)
        implements
            EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            return blockAtATime(page);
        }

        /**
         * Evaluate COALESCE block-at-a-time for as long as we can, then shift to
         * position-at-a-time.
         * <p>
         *     Block-at-a-time evaluation is the "normal" way to run the compute engine,
         *     just calling {@link ExpressionEvaluator#eval}. It's much faster so we try
         *     that first. For each evaluator, we {@linkplain ExpressionEvaluator#eval} and:
         * </p>
         * <ul>
         *     <li>If the {@linkplain Block} doesn't have any nulls we return it. COALESCE done.</li>
         *     <li>If the {@linkplain Block} is only nulls we skip it and try the next evaluator.</li>
         *     <li>If this is the last evaluator we just return it. COALESCE done.</li>
         *     <li>
         *         Otherwise, the {@linkplain Block} has mixed nulls and non-nulls so we drop
         *         into a block-at-a-time evaluator.
         *     </li>
         * </ul>
         */
        private Block blockAtATime(Page page) {
            int lastFullBlockIdx = 0;
            while (true) {
                Block lastFullBlock = evaluators.get(lastFullBlockIdx++).eval(page);
                if (lastFullBlockIdx == evaluators.size() || lastFullBlock.asVector() != null) {
                    return lastFullBlock;
                }
                if (lastFullBlock.areAllValuesNull()) {
                    // Result is all nulls and isn't the last result so we don't need any of it.
                    lastFullBlock.close();
                    continue;
                }
                try {
                    // The result has some nulls and some non-nulls.
                    return positionAtATime(page, lastFullBlockIdx, lastFullBlock);
                } finally {
                    lastFullBlock.close();
                }
            }
        }

        /**
         * Evaluate COALESCE position-at-a-time. We have a block that contains
         * some nulls and some non-nulls and we have some remaining evaluators.
         * For each position we either:
         * <ul>
         *     <li>Take the non-null values from the {@code lastFullBlock}</li>
         *     <li>
         *         Evaluator the remaining evaluators one at a time, keeping
         *         the first non-null value.
         *     </li>
         * </ul>
         * <p>
         *     It's important that we're evaluating position-by-position because
         *     the evaluators may produce warnings, and we really don't want those
         *     warnings to sneak into the output if they are for values that
         *     aren't needed. This lazy evaluation is not fast. Not at all. But
         *     it's very important not to leak the warnings.
         * </p>
         */
        private Block positionAtATime(Page page, int firstToEvaluate, Block lastFullBlock) {
            int positionCount = page.getPositionCount();
            try (Block.Builder result = resultType.newBlockBuilder(positionCount, driverContext.blockFactory())) {
                position: for (int p = 0; p < positionCount; p++) {
                    if (lastFullBlock.isNull(p) == false) {
                        result.copyFrom(lastFullBlock, p, p + 1);
                        continue;
                    }
                    int[] positions = new int[] { p };
                    Page limited = new Page(
                        1,
                        IntStream.range(0, page.getBlockCount()).mapToObj(b -> page.getBlock(b).filter(positions)).toArray(Block[]::new)
                    );
                    try (Releasable ignored = limited::releaseBlocks) {
                        for (int e = firstToEvaluate; e < evaluators.size(); e++) {
                            try (Block block = evaluators.get(e).eval(limited)) {
                                if (false == block.isNull(0)) {
                                    result.copyFrom(block, 0, 1);
                                    continue position;
                                }
                            }
                        }
                        result.appendNull();
                    }
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "CoalesceEvaluator[values=" + evaluators + ']';
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(() -> Releasables.close(evaluators));
        }
    }
}

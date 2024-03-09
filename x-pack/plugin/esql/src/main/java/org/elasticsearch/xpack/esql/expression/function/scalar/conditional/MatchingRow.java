/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.Expressions.name;

public class MatchingRow extends EsqlScalarFunction implements OptionalArgument {
    public record Match(Expression toMatch, Expression values) {}

    private final Match[] matches;

    public MatchingRow(Source source, Expression firstToMatch, Expression firstValues, List<Expression> remainingMatches) {
        super(source, Stream.concat(Stream.of(firstToMatch, firstValues), remainingMatches.stream()).toList());
        if (remainingMatches.size() % 2 == 1) {
            throw new QlIllegalArgumentException("[MATCHING_ROW] needs pairs of matching rows");
        }
        matches = new Match[remainingMatches.size() / 2 + 1];
        matches[0] = new Match(firstToMatch, firstValues);
        for (int i = 1; i < matches.length; i++) {
            matches[i] = new Match(remainingMatches.get(2 * i - 2), remainingMatches.get(2 * i - 1));
        }
    }

    public Match[] matches() {
        return matches;
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        for (Match m : matches) {
            if (m.values.foldable() == false) {
                return new TypeResolution(
                    format(
                        null,
                        "values position arguments of [{}] must be constants, found value [{}] type [{}]",
                        source(),
                        name(m.values),
                        m.values.dataType()
                    )
                );
            }
            if (m.toMatch.dataType() != m.values.dataType()) {
                return new TypeResolution(
                    format(
                        null,
                        "to_match position arguments of [{}] must match the type of their "
                            + "values position arguments, found value [{}] type [{}] which didn't match [{}] type [{}]",
                        source(),
                        name(m.toMatch),
                        m.toMatch.dataType(),
                        name(m.values),
                        m.values.dataType()
                    )
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MatchingRow(source(), newChildren.get(0), newChildren.get(1), newChildren.subList(2, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            MatchingRow::new,
            matches[0].toMatch,
            matches[0].values,
            Arrays.stream(matches).skip(1).flatMap(m -> Stream.of(m.toMatch, m.values)).toList()
        );
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        if (matches.length != 1) {
            throw new UnsupportedOperationException("NOCOMMIT");
        }
        // NOCOMMIT track me
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);

        Block[] values = new Block[matches.length];
        BlockHash hash = null;
        try {
            List<HashAggregationOperator.GroupSpec> groups = new ArrayList<>(matches.length);
            EvalOperator.ExpressionEvaluator.Factory[] toMatchFactories = new EvalOperator.ExpressionEvaluator.Factory[matches.length];
            for (int i = 0; i < values.length; i++) {
                Match m = matches[i];
                ElementType elementType = PlannerUtils.toElementType(m.values.dataType());
                values[i] = switch (elementType) {
                    case BYTES_REF -> ValueAt.valuesAsBytesRefBlock(blockFactory, m.values);
                    case INT -> ValueAt.valuesAsIntBlock(blockFactory, m.values);
                    default -> throw new UnsupportedOperationException("NOCOMMIT");
                };
                groups.add(new HashAggregationOperator.GroupSpec(i, elementType));
                toMatchFactories[i] = toEvaluator.apply(matches[i].toMatch);
            }
            hash = BlockHash.build(groups, blockFactory, Integer.MAX_VALUE, false);
            hash.add(new Page(values), new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    // NOCOMMIT make sure no dupes in input data.
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    // NOCOMMIT make sure no dupes in input data.
                }
            });
            Factory factory = new Factory(toMatchFactories, hash);
            hash = null;
            return factory;
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(values), hash);
        }
    }

    record Factory(EvalOperator.ExpressionEvaluator.Factory[] toMatch, BlockHash hash) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            EvalOperator.ExpressionEvaluator[] evaluators = new EvalOperator.ExpressionEvaluator[toMatch.length];
            for (int i = 0; i < evaluators.length; i++) {
                evaluators[i] = toMatch[i].get(context);
            }
            return new Evaluator(evaluators, hash);
        }

        @Override
        public String toString() {
            return "MatchingRow[" + hash + "]";
        }
    }

    record Evaluator(EvalOperator.ExpressionEvaluator[] toMatch, BlockHash hash) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            Block[] blocks = new Block[toMatch.length];
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = toMatch[i].eval(page);
                }
                return hash.lookup(new Page(blocks));
            } finally {
                Releasables.closeExpectNoException(blocks);
            }
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return "MatchingRow[" + hash + "]";
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.tree;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.LiteralTests;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.InPipe;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.NodeSubclassTests;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRanks;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentiles;
import org.elasticsearch.xpack.sql.expression.function.grouping.Histogram;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentDateTime;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Iif;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;

/**
 * Looks for all subclasses of {@link Node} and verifies that they
 * implement {@link Node#info()} and
 * {@link Node#replaceChildren(List)} sanely. It'd be better if
 * each subclass had its own test case that verified those methods
 * and any other interesting things that that they do but we're a
 * long way from that and this gets the job done for now.
 * <p>
 * This test attempts to use reflection to create believeable nodes
 * and manipulate them in believeable ways with as little knowledge
 * of the actual subclasses as possible. This is problematic because
 * it is possible, for example, for nodes to stackoverflow because
 * they <strong>can</strong> contain themselves. So this class
 * <strong>does</strong> have some {@link Node}-subclass-specific
 * knowledge. As little as I could get away with though.
 * <p>
 * When there are actual tests for a subclass of {@linkplain Node}
 * then this class will do two things:
 * <ul>
 * <li>Skip running any tests for that subclass entirely.
 * <li>Delegate to that test to build nodes of that type when a
 * node of that type is called for.
 * </ul>
 */
public class SqlNodeSubclassTests<T extends B, B extends Node<B>> extends NodeSubclassTests<T, B> {

    private static final List<Class<?>> CLASSES_WITH_MIN_TWO_CHILDREN = asList(
        Percentile.class,
        Percentiles.class,
        PercentileRanks.class,
        Iif.class,
        IfConditional.class,
        IfNull.class,
        In.class,
        InPipe.class,
        org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In.class
    );

    public SqlNodeSubclassTests(Class<T> subclass) {
        super(subclass);
    }

    @Override
    protected boolean hasAtLeastTwoChildren(Class<? extends Node<?>> toBuildClass) {
        return CLASSES_WITH_MIN_TWO_CHILDREN.stream().anyMatch(toBuildClass::equals);
    }

    @Override
    protected Object makeCompoundAgg() throws Exception {
        return makeNode(CompoundNumericAggregate.class);
    }

    @Override
    protected Object makeEnclosedAgg() {
        return makeArg(Avg.class);
    }

    @Override
    protected Object pluggableMakeArg(Class<? extends Node<?>> toBuildClass, Class<?> argClass) {
        if (toBuildClass == Histogram.class) {
            if (argClass == Expression.class) {
                return LiteralTests.randomLiteral();
            }
        } else if (toBuildClass == CurrentDateTime.class) {
            if (argClass == Expression.class) {
                return new Literal(SourceTests.randomSource(), randomInt(9), INTEGER);
            }
        }

        return null;
    }
}

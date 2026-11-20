/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * A constant collected below a node holds above it only where the node passes the column's values through: a LEFT
 * {@link HashJoin} fills its build side's columns with null for a probe row without a match, and a {@link UnionAll}'s
 * columns carry every branch's values.
 */
public class RuleUtilsFoldableRefsTests extends ESTestCase {

    public void testLeftHashJoinBuildSideIsNotFoldable() {
        Attribute probeKey = new ReferenceAttribute(Source.EMPTY, "probe_key", DataType.INTEGER);
        Attribute buildKey = new ReferenceAttribute(Source.EMPTY, "build_key", DataType.INTEGER);
        Alias constant = new Alias(Source.EMPTY, "c", new Literal(Source.EMPTY, 1, DataType.INTEGER));
        LogicalPlan probe = relation(probeKey);
        LogicalPlan build = new Eval(Source.EMPTY, relation(buildKey), List.of(constant));
        List<Attribute> added = List.of(constant.toAttribute());

        LogicalPlan inner = new HashJoin(Source.EMPTY, probe, build, List.of(probeKey), List.of(buildKey), added, false, JoinTypes.INNER);
        assertThat(foldable(inner).get(constant.toAttribute()), notNullValue());

        LogicalPlan left = new HashJoin(Source.EMPTY, probe, build, List.of(probeKey), List.of(buildKey), added, false, JoinTypes.LEFT);
        assertThat(foldable(left).get(constant.toAttribute()), nullValue());
    }

    public void testUnionColumnIsNotFoldable() {
        Attribute key = new ReferenceAttribute(Source.EMPTY, "key", DataType.INTEGER);
        Alias one = new Alias(Source.EMPTY, "c", new Literal(Source.EMPTY, 1, DataType.INTEGER));
        Alias two = new Alias(Source.EMPTY, "c", new Literal(Source.EMPTY, 2, DataType.INTEGER));
        LogicalPlan first = new Eval(Source.EMPTY, relation(key), List.of(one));
        LogicalPlan second = new Eval(Source.EMPTY, relation(key), List.of(two));
        assertThat(foldable(first).get(one.toAttribute()), notNullValue());

        // The union's column shares its id with the first branch's.
        LogicalPlan union = new UnionAll(Source.EMPTY, List.of(first, second), List.of(key, one.toAttribute()));
        assertThat(foldable(union).get(one.toAttribute()), nullValue());
    }

    private static LogicalPlan relation(Attribute attribute) {
        return new LocalRelation(Source.EMPTY, List.of(attribute), EmptyLocalSupplier.EMPTY);
    }

    private static AttributeMap<Expression> foldable(LogicalPlan plan) {
        return RuleUtils.foldableReferencesSkipMVGroupings(plan, unboundLogicalOptimizerContext());
    }
}

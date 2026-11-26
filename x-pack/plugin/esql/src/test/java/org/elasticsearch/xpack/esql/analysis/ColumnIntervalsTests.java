/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ColumnIntervalsTests extends ESTestCase {

    public void testFormat() {
        assertThat(ColumnIntervals.format(new Rounding.Interval(1, "hour")), equalTo("1 hour"));
        assertThat(ColumnIntervals.format(new Rounding.Interval(90, "minute")), equalTo("90 minutes"));
        assertThat(ColumnIntervals.format(new Rounding.Interval(3, "month")), equalTo("3 months"));
    }

    public void testDoesNotFoldConstantsThatAreNotBucketArguments() {
        Expression embedding = new TextEmbedding(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "hi"),
            Literal.keyword(Source.EMPTY, "id"),
            null
        );
        assertTrue(embedding.foldable());
        LogicalPlan plan = new Eval(
            Source.EMPTY,
            new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY),
            List.of(new Alias(Source.EMPTY, "e", embedding))
        );
        assertThat(ColumnIntervals.collect(plan, FoldContext.small()), equalTo(Map.of()));
    }

    public void testDoesNotInlineConstantsThatAreNotBucketArguments() {
        // Each alias reads the previous one twice, so inlining them would double the expression at every step.
        List<Alias> aliases = new ArrayList<>();
        Expression previous = Literal.integer(Source.EMPTY, 1);
        for (int i = 0; i < 64; i++) {
            Alias alias = new Alias(Source.EMPTY, "a" + i, new Add(Source.EMPTY, previous, previous, EsqlTestUtils.TEST_CFG));
            aliases.add(alias);
            previous = alias.toAttribute();
        }
        LogicalPlan plan = new Eval(Source.EMPTY, new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY), aliases);
        assertThat(ColumnIntervals.collect(plan, FoldContext.small()), equalTo(Map.of()));
    }
}

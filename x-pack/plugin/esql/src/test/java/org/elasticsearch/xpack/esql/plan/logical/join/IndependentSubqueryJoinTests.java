/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class IndependentSubqueryJoinTests extends ESTestCase {

    public void testMainScopeTraversalStopsAtIndependentSubqueryRightSide() {
        ScopePlans plans = scopePlans();

        List<LogicalPlan> visited = new ArrayList<>();
        IndependentSubqueryJoin.forEachInMainScope(plans.join(), visited::add);
        assertThat(visited, contains(plans.join(), plans.leftLimit(), plans.left()));
        assertTrue(IndependentSubqueryJoin.anyMatchInMainScope(plans.join(), plan -> plan == plans.leftLimit()));
        assertFalse(IndependentSubqueryJoin.anyMatchInMainScope(plans.join(), plan -> plan == plans.rightLimit()));
        assertThat(
            IndependentSubqueryJoin.collectFirstChildrenInMainScope(plans.join(), Limit.class::isInstance),
            contains(plans.leftLimit())
        );

        visited.clear();
        IndependentSubqueryJoin.forEachUpInMainScope(plans.join(), visited::add);
        assertThat(visited, contains(plans.left(), plans.leftLimit(), plans.join()));

        visited.clear();
        assertTrue(IndependentSubqueryJoin.forEachInMainScopeUntil(plans.join(), plan -> {
            visited.add(plan);
            return plan == plans.leftLimit();
        }));
        assertThat(visited, contains(plans.join(), plans.leftLimit()));
    }

    public void testMainScopeTransformsDoNotRewriteIndependentSubqueryRightSide() {
        ScopePlans plans = scopePlans();
        LogicalPlan transformedUp = IndependentSubqueryJoin.transformUpInMainScope(
            plans.join(),
            plan -> plan == plans.leftLimit() ? limit(plans.left(), 20) : plan
        );
        SemiJoin upJoin = (SemiJoin) transformedUp;
        assertThat(((Literal) ((Limit) upJoin.left()).limit()).value(), equalTo(20));
        assertSame(plans.rightLimit(), upJoin.right());

        LogicalPlan transformedDown = IndependentSubqueryJoin.transformDownInMainScope(
            plans.join(),
            plan -> plan == plans.leftLimit() ? limit(plans.left(), 30) : plan
        );
        SemiJoin downJoin = (SemiJoin) transformedDown;
        assertThat(((Literal) ((Limit) downJoin.left()).limit()).value(), equalTo(30));
        assertSame(plans.rightLimit(), downJoin.right());

        List<LogicalPlan> visited = new ArrayList<>();
        LogicalPlan skipped = IndependentSubqueryJoin.transformDownInMainScopeSkipBranch(plans.join(), (plan, skipBranch) -> {
            visited.add(plan);
            if (plan == plans.leftLimit()) {
                skipBranch.set(true);
            }
            return plan;
        });
        assertSame(plans.join(), skipped);
        assertThat(visited, contains(plans.join(), plans.leftLimit()));
    }

    private static ScopePlans scopePlans() {
        ReferenceAttribute leftKey = new ReferenceAttribute(Source.EMPTY, "left", DataType.INTEGER);
        ReferenceAttribute rightKey = new ReferenceAttribute(Source.EMPTY, "right", DataType.INTEGER);
        LocalRelation left = relation(leftKey);
        LocalRelation right = relation(rightKey);
        Limit leftLimit = limit(left, 10);
        Limit rightLimit = limit(right, 10);
        return new ScopePlans(
            left,
            leftLimit,
            rightLimit,
            new SemiJoin(Source.EMPTY, leftLimit, rightLimit, List.of(leftKey), List.of(rightKey))
        );
    }

    private static LocalRelation relation(Attribute attribute) {
        return new LocalRelation(Source.EMPTY, List.of(attribute), LocalSupplier.of(new Page(0)));
    }

    private static Limit limit(LogicalPlan child, int value) {
        return new Limit(Source.EMPTY, new Literal(Source.EMPTY, value, DataType.INTEGER), child);
    }

    private record ScopePlans(LocalRelation left, Limit leftLimit, Limit rightLimit, SemiJoin join) {}
}

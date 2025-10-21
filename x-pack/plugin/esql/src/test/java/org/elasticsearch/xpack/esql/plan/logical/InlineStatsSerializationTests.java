/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class InlineStatsSerializationTests extends AbstractLogicalPlanSerializationTests<InlineStats> {
    @Override
    protected InlineStats createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = randomFieldAttributes(0, 5, false).stream().map(a -> (NamedExpression) a).toList();
        return new InlineStats(source, new Aggregate(source, child, groupings, aggregates));
    }

    @Override
    protected InlineStats mutateInstance(InlineStats instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.aggregate().groupings();
        List<? extends NamedExpression> aggregates = instance.aggregate().aggregates();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 2 -> aggregates = randomValueOtherThan(
                aggregates,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (NamedExpression) a).toList()
            );
        }
        Aggregate agg = new Aggregate(instance.source(), child, groupings, aggregates);
        return new InlineStats(instance.source(), agg);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AggregateSerializationTests extends AbstractLogicalPlanSerializationTests<Aggregate> {
    @Override
    protected Aggregate createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Aggregate.AggregateType aggregateType = randomFrom(Aggregate.AggregateType.values());
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = randomAggregates();
        return new Aggregate(source, child, aggregateType, groupings, aggregates);
    }

    public static List<? extends NamedExpression> randomAggregates() {
        int size = between(1, 5);
        List<NamedExpression> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Expression agg = switch (between(0, 5)) {
                case 0 -> new Max(randomSource(), FieldAttributeTests.createFieldAttribute(1, true));
                case 1 -> new Min(randomSource(), FieldAttributeTests.createFieldAttribute(1, true));
                case 2 -> new Count(randomSource(), FieldAttributeTests.createFieldAttribute(1, true));
                case 3 -> new Top(
                    randomSource(),
                    FieldAttributeTests.createFieldAttribute(1, true),
                    new Literal(randomSource(), between(1, 5), DataType.INTEGER),
                    new Literal(randomSource(), randomFrom("ASC", "DESC"), DataType.KEYWORD)
                );
                case 4 -> new Values(randomSource(), FieldAttributeTests.createFieldAttribute(1, true));
                case 5 -> new Sum(randomSource(), FieldAttributeTests.createFieldAttribute(1, true));
                default -> throw new IllegalArgumentException();
            };
            result.add(new Alias(randomSource(), randomAlphaOfLength(5), agg));
        }
        return result;
    }

    @Override
    protected Aggregate mutateInstance(Aggregate instance) throws IOException {
        LogicalPlan child = instance.child();
        Aggregate.AggregateType aggregateType = instance.aggregateType();
        List<Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> aggregateType = randomValueOtherThan(aggregateType, () -> randomFrom(Aggregate.AggregateType.values()));
            case 2 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 3 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
        }
        return new Aggregate(instance.source(), child, aggregateType, groupings, aggregates);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

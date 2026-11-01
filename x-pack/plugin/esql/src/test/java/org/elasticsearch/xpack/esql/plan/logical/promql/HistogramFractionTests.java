/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests.randomChild;

public class HistogramFractionTests extends AbstractNodeTestCase<HistogramFraction, LogicalPlan> {

    public static HistogramFraction randomHistogramFraction() {
        Source source = randomSource();
        return new HistogramFraction(
            source,
            randomChild(0),
            org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction.PROMQL_DEFINITION,
            List.of(randomBound(source), randomBound(source))
        );
    }

    private static Expression randomBound(Source source) {
        return Literal.fromDouble(source, randomDouble());
    }

    @Override
    protected HistogramFraction randomInstance() {
        return randomHistogramFraction();
    }

    @Override
    protected HistogramFraction mutate(HistogramFraction instance) {
        Supplier<HistogramFraction> option = randomFrom(
            List.of(
                () -> new HistogramFraction(
                    instance.source(),
                    randomValueOtherThan(instance.child(), () -> randomChild(0)),
                    instance.definition(),
                    instance.parameters()
                ),
                () -> new HistogramFraction(
                    instance.source(),
                    instance.child(),
                    instance.definition(),
                    List.of(randomValueOtherThan(instance.lower(), () -> randomBound(instance.source())), instance.upper())
                ),
                () -> new HistogramFraction(
                    instance.source(),
                    instance.child(),
                    instance.definition(),
                    List.of(instance.lower(), randomValueOtherThan(instance.upper(), () -> randomBound(instance.source())))
                )
            )
        );
        return option.get();
    }

    @Override
    protected HistogramFraction copy(HistogramFraction instance) {
        return new HistogramFraction(instance.source(), instance.child(), instance.definition(), instance.parameters());
    }

    @Override
    public void testTransform() {
        HistogramFraction node = randomHistogramFraction();
        List<Expression> newParameters = List.of(randomValueOtherThan(node.lower(), () -> randomBound(node.source())), node.upper());
        assertEquals(
            new HistogramFraction(node.source(), node.child(), node.definition(), newParameters),
            node.transformPropertiesOnly(Object.class, p -> Objects.equals(p, node.parameters()) ? newParameters : p)
        );

        PromqlFunctionDefinition newDefinition = randomValueOtherThan(node.definition(), () -> Rate.PROMQL_DEFINITION);
        assertEquals(
            new HistogramFraction(node.source(), node.child(), newDefinition, node.parameters()),
            node.transformPropertiesOnly(Object.class, p -> Objects.equals(p, node.definition()) ? newDefinition : p)
        );
    }

    @Override
    public void testReplaceChildren() {
        HistogramFraction node = randomHistogramFraction();
        LogicalPlan newChild = randomValueOtherThan(node.child(), () -> randomChild(0));
        assertEquals(new HistogramFraction(node.source(), newChild, node.definition(), node.parameters()), node.replaceChild(newChild));
    }
}

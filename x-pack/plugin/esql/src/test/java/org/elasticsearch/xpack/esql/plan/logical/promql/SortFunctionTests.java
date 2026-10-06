/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests.randomEsRelation;
import static org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests.randomLocalRelation;

/**
 * Needed to override the reflective tests in {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests}
 * for {@link SortFunction}.
 */
public class SortFunctionTests extends AbstractNodeTestCase<SortFunction, LogicalPlan> {

    public static SortFunction randomSortFunction() {
        return new SortFunction(randomSource(), randomChildWithOutput(), randomDefinition(), List.of());
    }

    private static LogicalPlan randomChildWithOutput() {
        return randomBoolean() ? randomEsRelation() : randomLocalRelation();
    }

    private static PromqlFunctionDefinition randomDefinition() {
        return randomBoolean() ? PromqlBuiltinFunctionDefinitions.SORT : PromqlBuiltinFunctionDefinitions.SORT_DESC;
    }

    private static PromqlFunctionDefinition otherDefinition(PromqlFunctionDefinition current) {
        return current == PromqlBuiltinFunctionDefinitions.SORT
            ? PromqlBuiltinFunctionDefinitions.SORT_DESC
            : PromqlBuiltinFunctionDefinitions.SORT;
    }

    @Override
    protected SortFunction randomInstance() {
        return randomSortFunction();
    }

    @Override
    protected SortFunction mutate(SortFunction instance) {
        Supplier<SortFunction> option = randomFrom(
            List.of(
                () -> instance.replaceChild(randomValueOtherThan(instance.child(), SortFunctionTests::randomChildWithOutput)),
                () -> (SortFunction) instance.transformPropertiesOnly(
                    Object.class,
                    p -> Objects.equals(p, instance.definition()) ? otherDefinition(instance.definition()) : p
                )
            )
        );
        return option.get();
    }

    @Override
    protected SortFunction copy(SortFunction instance) {
        return instance.replaceChild(instance.child());
    }

    @Override
    public void testTransform() {
        SortFunction node = randomSortFunction();

        PromqlFunctionDefinition newDefinition = otherDefinition(node.definition());
        SortFunction transformed = (SortFunction) node.transformPropertiesOnly(
            Object.class,
            p -> Objects.equals(p, node.definition()) ? newDefinition : p
        );
        assertEquals(node.source(), transformed.source());
        assertEquals(node.child(), transformed.child());
        assertEquals(newDefinition, transformed.definition());
        assertEquals(node.parameters(), transformed.parameters());
    }

    @Override
    public void testReplaceChildren() {
        SortFunction node = randomSortFunction();
        LogicalPlan newChild = randomValueOtherThan(node.child(), SortFunctionTests::randomChildWithOutput);

        SortFunction replaced = node.replaceChild(newChild);
        assertEquals(node.source(), replaced.source());
        assertEquals(newChild, replaced.child());
        assertEquals(node.definition(), replaced.definition());
        assertEquals(node.parameters(), replaced.parameters());
    }
}

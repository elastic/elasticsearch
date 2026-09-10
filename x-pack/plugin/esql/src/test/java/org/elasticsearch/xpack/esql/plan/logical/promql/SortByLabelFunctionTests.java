/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests.randomEsRelation;
import static org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests.randomLocalRelation;

/**
 * Needed to override the reflective tests in {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests}
 * for {@link SortByLabelFunction}.
 */
public class SortByLabelFunctionTests extends AbstractNodeTestCase<SortByLabelFunction, LogicalPlan> {

    public static SortByLabelFunction randomSortByLabelFunction() {
        Source source = randomSource();
        return new SortByLabelFunction(
            source,
            randomChildWithOutput(),
            randomDefinition(),
            randomParameters(source),
            randomSortLabels(source)
        );
    }

    private static LogicalPlan randomChildWithOutput() {
        return randomBoolean() ? randomEsRelation() : randomLocalRelation();
    }

    private static PromqlFunctionDefinition randomDefinition() {
        return randomBoolean() ? PromqlBuiltinFunctionDefinitions.SORT_BY_LABEL : PromqlBuiltinFunctionDefinitions.SORT_BY_LABEL_DESC;
    }

    private static PromqlFunctionDefinition otherDefinition(PromqlFunctionDefinition current) {
        return current == PromqlBuiltinFunctionDefinitions.SORT_BY_LABEL
            ? PromqlBuiltinFunctionDefinitions.SORT_BY_LABEL_DESC
            : PromqlBuiltinFunctionDefinitions.SORT_BY_LABEL;
    }

    private static List<Expression> randomParameters(Source source) {
        int count = between(1, 3);
        List<Expression> parameters = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            parameters.add(Literal.keyword(source, randomAlphaOfLength(5)));
        }
        return parameters;
    }

    private static List<Attribute> randomSortLabels(Source source) {
        int count = between(1, 3);
        List<Attribute> labels = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            labels.add(new UnresolvedAttribute(source, randomAlphaOfLength(5)));
        }
        return labels;
    }

    @Override
    protected SortByLabelFunction randomInstance() {
        return randomSortByLabelFunction();
    }

    @Override
    protected SortByLabelFunction mutate(SortByLabelFunction instance) {
        Supplier<SortByLabelFunction> option = randomFrom(
            List.of(
                () -> instance.replaceChild(randomValueOtherThan(instance.child(), SortByLabelFunctionTests::randomChildWithOutput)),
                () -> (SortByLabelFunction) instance.transformPropertiesOnly(
                    Object.class,
                    p -> Objects.equals(p, instance.definition()) ? otherDefinition(instance.definition()) : p
                )
            )
        );
        return option.get();
    }

    @Override
    protected SortByLabelFunction copy(SortByLabelFunction instance) {
        return instance.replaceChild(instance.child());
    }

    @Override
    public void testTransform() {
        SortByLabelFunction node = randomSortByLabelFunction();

        PromqlFunctionDefinition newDefinition = otherDefinition(node.definition());
        SortByLabelFunction transformed = (SortByLabelFunction) node.transformPropertiesOnly(
            Object.class,
            p -> Objects.equals(p, node.definition()) ? newDefinition : p
        );
        assertEquals(node.source(), transformed.source());
        assertEquals(node.child(), transformed.child());
        assertEquals(newDefinition, transformed.definition());
        assertEquals(node.parameters(), transformed.parameters());
        assertEquals(node.sortLabels(), transformed.sortLabels());
    }

    @Override
    public void testReplaceChildren() {
        SortByLabelFunction node = randomSortByLabelFunction();
        LogicalPlan newChild = randomValueOtherThan(node.child(), SortByLabelFunctionTests::randomChildWithOutput);
        SortByLabelFunction replaced = node.replaceChild(newChild);
        assertEquals(newChild, replaced.child());
        assertEquals(node.definition(), replaced.definition());
        assertEquals(node.parameters(), replaced.parameters());
        assertEquals(node.sortLabels(), replaced.sortLabels());
    }
}

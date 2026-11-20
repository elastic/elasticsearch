/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicateSerializationTests.analyzedPredicate;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class NestedPredicateTests extends ESTestCase {
    private static final String QUERY = "FROM ne | WHERE NESTED_ANY(CLAMP(items.qty, 1, 5) > 2) | LIMIT 10";

    public void testBodyIsTheOnlyChild() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.children(), equalTo(List.of(predicate.body())));
    }

    public void testExpressionRewritesReachTheBody() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.body().anyMatch(SurrogateExpression.class::isInstance), is(true));

        Expression rewritten = predicate.transformDown(Expression.class, SubstituteSurrogateExpressions::rule);

        assertThat(rewritten, instanceOf(NestedPredicate.class));
        NestedPredicate rewrittenPredicate = (NestedPredicate) rewritten;
        assertThat(rewrittenPredicate.body().anyMatch(SurrogateExpression.class::isInstance), is(false));
        assertThat(rewrittenPredicate.body(), not(equalTo(predicate.body())));
        assertThat(rewrittenPredicate.path(), equalTo(predicate.path()));
        assertThat(rewrittenPredicate.inputs(), equalTo(predicate.inputs()));
        assertThat(rewrittenPredicate.relativePaths(), equalTo(predicate.relativePaths()));
        assertThat(rewrittenPredicate.configuration(), equalTo(predicate.configuration()));
    }

    public void testInputsAreNotReferencesOfTheParentRow() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.body().references().isEmpty(), is(false));
        assertThat(predicate.references().isEmpty(), is(true));
    }

    private static final String OUTER_QUERY = "FROM ne | WHERE NESTED_ANY(items.sku == CONCAT(id, \"x\")) | LIMIT 10";

    public void testOuterExpressionsAreChildrenAfterTheBody() {
        NestedPredicate predicate = analyzedPredicate(OUTER_QUERY);
        assertThat(predicate.outers(), hasSize(1));
        assertThat(predicate.children().getFirst(), equalTo(predicate.body()));
        assertThat(predicate.children().subList(1, predicate.children().size()), equalTo(predicate.outers()));
    }

    public void testReferencesAreTheParentColumnsOfTheOuterExpressions() {
        NestedPredicate predicate = analyzedPredicate(OUTER_QUERY);
        assertThat(predicate.references().stream().map(attribute -> attribute.name()).toList(), equalTo(List.of("id")));
    }

    public void testAnOuterExpressionFoldedToAConstantBecomesPartOfTheBody() {
        NestedPredicate predicate = analyzedPredicate(OUTER_QUERY);
        List<Expression> children = new ArrayList<>(predicate.children());
        children.set(1, new Literal(Source.EMPTY, new BytesRef("ax"), DataType.KEYWORD));

        NestedPredicate folded = predicate.replaceChildren(children);

        assertThat(folded.outers(), is(empty()));
        assertThat(folded.outerInputs(), is(empty()));
        assertThat(folded.body().anyMatch(e -> e instanceof Literal literal && new BytesRef("ax").equals(literal.value())), is(true));
        assertThat(folded.references().isEmpty(), is(true));
    }

    public void testAnOuterExpressionThatIsNotConstantIsKept() {
        NestedPredicate predicate = analyzedPredicate(OUTER_QUERY);
        NestedPredicate same = predicate.replaceChildren(predicate.children());
        assertThat(same, equalTo(predicate));
    }

    public void testAChildPredicateDoesNotExistWhenTheBodyReadsAnOuterValue() {
        assertThat(analyzedPredicate(OUTER_QUERY).childPredicate(), is(nullValue()));
    }
}

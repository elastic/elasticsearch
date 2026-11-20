/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;

import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicateSerializationTests.analyzedPredicate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
}

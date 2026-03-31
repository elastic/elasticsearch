/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.util.List;

public class ReplaceAggregateNestedExpressionWithEvalTests extends ESTestCase {

    // Regression for unresolved `step` reaching this rule: canonicalization must not
    // call dataType() on unresolved attributes.
    public void testUnresolvedGroupingAliasDoesNotThrowDuringCanonicalization() {
        Source source = Source.EMPTY;
        Alias stepAlias = new Alias(source, "step", new UnresolvedAttribute(source, "step"));
        Aggregate aggregate = new Aggregate(
            source,
            new StubRelation(source, List.of()),
            List.of(stepAlias),
            List.of(stepAlias.toAttribute())
        );

        Aggregate rewritten = (Aggregate) new ReplaceAggregateNestedExpressionWithEval().apply(aggregate);
        assertTrue(rewritten.child() instanceof Eval);
    }
}

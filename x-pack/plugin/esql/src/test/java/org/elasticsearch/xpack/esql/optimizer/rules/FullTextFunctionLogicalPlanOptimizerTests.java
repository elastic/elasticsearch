/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;

import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

public class FullTextFunctionLogicalPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    public void testFullTextFunctionQueryArgFoldedFromConcat() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = optimizedPlan(String.format(Locale.ROOT, """
            from test
            | where %s(last_name, concat("Do", "e"))
            """, functionName));

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        FieldAttribute lastName = as(fullTextFunction.field(), FieldAttribute.class);
        assertEquals("last_name", lastName.name());
        Literal queryLiteral = as(fullTextFunction.query(), Literal.class);
        assertEquals(new BytesRef("Doe"), queryLiteral.value());
    }

    public void testFullTextFunctionQueryArgPropagatedFromEval() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = optimizedPlan(String.format(Locale.ROOT, """
            from test
            | eval q = "Doe"
            | where %s(last_name, q)
            """, functionName));

        // Eval is retained in the plan even after propagation
        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        FieldAttribute lastName = as(fullTextFunction.field(), FieldAttribute.class);
        assertEquals("last_name", lastName.name());
        Literal queryLiteral = as(fullTextFunction.query(), Literal.class);
        assertEquals(new BytesRef("Doe"), queryLiteral.value());
    }

    public void testQueryStringFunctionQueryArgFoldedFromConcat() {
        String functionName = randomFrom("qstr", "kql");
        var plan = optimizedPlan(String.format(Locale.ROOT, """
            from test
            | where %s(concat("last_name:", "Doe"))
            """, functionName));

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), FullTextFunction.class);
        Literal queryLiteral = as(fullTextFunction.query(), Literal.class);
        assertEquals(new BytesRef("last_name:Doe"), queryLiteral.value());
    }

    public void testFullTextFunctionQueryArgNotFoldable() {
        String functionName = randomFrom("match", "match_phrase");
        failPlan(
            String.format(Locale.ROOT, "from test | where %s(last_name, first_name)", functionName),
            "Query must be a constant string in"
        );
    }

    public void testQueryStringFunctionQueryArgNotFoldable() {
        String functionName = randomFrom("qstr", "kql");
        failPlan(String.format(Locale.ROOT, "from test | where %s(last_name)", functionName), "Query must be a constant string in");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedGroupings;
import org.elasticsearch.xpack.esql.plan.logical.Eval;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/** {@code NESTED(<aggregate> BY <keys>)} parses its keys into one trailing argument, and BY is rejected anywhere else. */
public class NestedByParserTests extends AbstractStatementParserTests {

    public void testKeysBecomeTheLastArgument() {
        assumeTrue("NESTED BY is snapshot-only", Build.current().isSnapshot());
        Eval eval = as(query("row a = 1 | eval x = nested(count(*) by b, c.d)"), Eval.class);
        UnresolvedFunction nested = as(as(eval.fields().getFirst(), Alias.class).child(), UnresolvedFunction.class);
        assertThat(nested.name(), equalTo("nested"));
        assertThat(nested.children().size(), equalTo(2));
        assertThat(nested.children().getFirst(), instanceOf(UnresolvedFunction.class));
        NestedGroupings keys = as(nested.children().get(1), NestedGroupings.class);
        assertThat(keys.children().stream().map(k -> as(k, UnresolvedAttribute.class).name()).toList(), contains("b", "c.d"));
    }

    public void testByOutsideNestedIsRejected() {
        assumeTrue("NESTED BY is snapshot-only", Build.current().isSnapshot());
        expectError("row a = 1 | eval x = mv_count(a by b)", "BY is only allowed inside NESTED");
    }

    public void testByInsideNestedJsonIsRejected() {
        assumeTrue("NESTED BY is snapshot-only", Build.current().isSnapshot());
        expectError("row a = 1 | eval x = nested_json(a by b)", "BY is only allowed inside NESTED");
    }

    public void testByInsideNestedAnyIsRejected() {
        assumeTrue("NESTED BY is snapshot-only", Build.current().isSnapshot());
        expectError("row a = 1 | eval x = nested_any(a by b)", "BY is only allowed inside NESTED");
    }
}

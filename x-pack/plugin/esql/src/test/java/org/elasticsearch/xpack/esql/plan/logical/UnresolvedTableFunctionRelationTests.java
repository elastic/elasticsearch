/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionArgument;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * What this leaf is allowed to say about itself. A map argument can carry a credential, and both
 * {@code nodeProperties()} and {@code toString()} reach {@code EXPLAIN} output, so neither may render one.
 * The sibling leaf {@code UnresolvedExternalRelation} pins the same property for the same reason.
 */
public class UnresolvedTableFunctionRelationTests extends ESTestCase {

    private static final String SECRET = "AKIAIOSFODNN7EXAMPLE";

    private static UnresolvedTableFunctionRelation leafCarryingASecret() {
        Source source = Source.EMPTY;
        return new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(
                source,
                "somefunc",
                List.of(
                    new TableFunctionArgument.Name(source, "a_name"),
                    new TableFunctionArgument.NamedValues(source, Map.of("access_key", SECRET))
                )
            ),
            List.of()
        );
    }

    public void testNodePropertiesCarryTheNameAndNothingElse() {
        UnresolvedTableFunctionRelation leaf = leafCarryingASecret();

        assertThat(leaf.nodeProperties().size(), equalTo(1));
        assertThat(leaf.nodeProperties().get(0), equalTo("somefunc"));
        assertThat(leaf.nodeProperties().toString(), not(containsString(SECRET)));
    }

    public void testToStringDoesNotRenderTheArguments() {
        assertThat(leafCarryingASecret().toString(), not(containsString(SECRET)));
    }

    /** It still has to be identifiable, or an EXPLAIN of a query with two calls reads the same for both. */
    public void testToStringNamesTheFunction() {
        assertThat(leafCarryingASecret().toString(), containsString("SOMEFUNC(...)"));
    }
}

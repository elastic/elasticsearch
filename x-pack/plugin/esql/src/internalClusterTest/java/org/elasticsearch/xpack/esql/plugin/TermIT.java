/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.plugin.QueryStringIT.createAndPopulateIndex;
import static org.hamcrest.CoreMatchers.containsString;

public class TermIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex(this::ensureYellow);
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());
        return super.run(request);
    }

    public void testSimpleTermQuery() throws Exception {
        var query = """
            FROM test
            | WHERE term(content,"dog")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testTermWithinEval() {
        var query = """
            FROM test
            | EVAL term_query = term(title,"fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[Term] function is only supported in WHERE and STATS commands"));
    }

    public void testMultipleTerm() {
        var query = """
            FROM test
            | WHERE term(content,"fox") AND term(content,"brown")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(2), List.of(4), List.of(5)));
        }
    }

    public void testNotWhereTerm() {
        var query = """
            FROM test
            | WHERE NOT term(content,"brown")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(3)));
        }
    }

    public void testTermWithLookupJoin() {
        var query = """
            FROM test
            | LOOKUP JOIN test_lookup ON id
            | WHERE id > 0 AND TERM(lookup_content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString(
                "line 3:25: [Term] function cannot operate on [lookup_content], supplied by an index [test_lookup] "
                    + "in non-STANDARD mode [lookup]"
            )
        );
    }
}

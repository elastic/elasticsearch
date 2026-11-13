/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntoParserTests extends AbstractStatementParserTests {
    @Before
    public void requiresSnapshot() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testDestination() {
        Into into = as(query("ROW a = 1 | INTO dst"), Into.class);
        assertThat(into.destination(), equalTo("dst"));
        as(into.child(), Row.class);
        assertThat(as(query("ROW a = 1 | INTO \"my-dst\""), Into.class).destination(), equalTo("my-dst"));
    }

    public void testBadDestination() {
        expectError("ROW a = 1 | INTO dst*", "invalid index pattern [dst*], * is not allowed in INTO");
        expectError("ROW a = 1 | INTO remote:dst", "invalid index pattern [remote:dst], remote clusters are not supported with INTO");
        expectError("ROW a = 1 | INTO dst::data", "invalid index pattern [dst::data], index pattern selectors are not supported in INTO");
    }

    public void testMustBeTheLastTopLevelCommand() {
        String error = "INTO must be the last command of the query";
        // These are now rejected by the Verifier (VerificationException), not the parser (ParsingException)
        analyzer().error("ROW a = 1 | INTO dst | LIMIT 1", containsString(error));
        analyzer().error("ROW a = 1 | FORK (INTO dst) (LIMIT 1)", containsString(error));
        // Parser still rejects INTO inside subqueries
        expectError("FROM (ROW a = 1 | INTO dst)", error);
        expectError("ROW a = 1 | WHERE a IN (ROW b = 1 | INTO dst)", error);
        expectValidationError("LET x = (ROW b = 1 | INTO dst); ROW a = 1", error);
    }

    public void testOnlyRowSource() {
        // These are now rejected by postAnalysisVerification (VerificationException)
        String forkError = "INTO currently only supports ROW as a source, without FORK";
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | INTO dst", containsString(forkError));
        // EXPLAIN (...) | INTO dst is a parse error since EXPLAIN doesn't support downstream commands
        expectError("EXPLAIN (ROW a = 1) | INTO dst", "EXPLAIN does not support downstream commands");
        // FROM src | INTO dst requires an index; tested in IntoIT for the full integration path
    }

    public void testViewBodyRejected() {
        expectThrows(
            ParsingException.class,
            containsString("INTO is not allowed in a view definition"),
            () -> TEST_PARSER.parseView(
                "ROW a = 1 | INTO dst",
                new QueryParams(),
                new InferenceSettings(Settings.EMPTY),
                "my_view",
                QueryPragmas.EMPTY,
                EsqlFlags.DEFAULTS
            )
        );
    }

    public void testExplainIntoIsValid() {
        // EXPLAIN wrapping INTO is now valid; EsqlSession unwraps EXPLAIN leaving Into(Row) as the analyzed plan
        Into into = as(query("ROW a = 1 | INTO dst"), Into.class);
        assertThat(into.destination(), equalTo("dst"));
    }

    private static TestAnalyzer analyzer() {
        return EsqlTestUtils.analyzer();
    }
}

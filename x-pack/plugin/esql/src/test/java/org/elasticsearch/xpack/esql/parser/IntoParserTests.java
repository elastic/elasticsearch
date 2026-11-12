/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
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
        assumeTrue("INTO is only enabled in snapshot builds", Build.current().isSnapshot());
    }

    public void testDestination() {
        Into into = as(query("ROW a = 1 | INTO dst"), Into.class);
        assertThat(into.destination(), equalTo("dst"));
        as(into.child(), Row.class);

        Into lowerCase = as(query("ROW a = 1 | into dst"), Into.class);
        assertThat(lowerCase.destination(), equalTo("dst"));
    }

    public void testQuotedDestination() {
        Into into = as(query("ROW a = 1 | INTO \"my-dst\""), Into.class);
        assertThat(into.destination(), equalTo("my-dst"));
    }

    public void testBadDestination() {
        expectError("ROW a = 1 | INTO dst*", "invalid index pattern [dst*], * is not allowed in INTO");
        expectError("ROW a = 1 | INTO remote:dst", "invalid index pattern [remote:dst], remote clusters are not supported with INTO");
        expectError("ROW a = 1 | INTO dst::data", "invalid index pattern [dst::data], index pattern selectors are not supported in INTO");
    }

    public void testMustBeTopLevelLastCommand() {
        expectError("ROW a = 1 | INTO dst | LIMIT 1", "INTO must be the last command of the query");
        expectError("ROW a = 1 | INTO dst | INTO other", "INTO must be the last command of the query");
        expectError("ROW a = 1 | FORK (INTO dst) (LIMIT 1)", "INTO must be the last command of the query");
        expectError("FROM (ROW a = 1 | INTO dst)", "INTO must be the last command of the query");
        expectError("ROW a = 1 | WHERE a IN (ROW b = 1 | INTO dst)", "INTO must be the last command of the query");
        ParsingException letError = expectThrows(ParsingException.class, () -> statement("LET x = (ROW b = 1 | INTO dst); ROW a = 1"));
        assertThat(letError.getMessage(), containsString("INTO must be the last command of the query"));
        expectError("EXPLAIN (ROW a = 1 | INTO dst)", "INTO must be the last command of the query");
        expectError("EXPLAIN (ROW a = 1) | INTO dst", "EXPLAIN does not support downstream commands");
    }

    public void testViewBodyRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> TEST_PARSER.parseView(
                "ROW a = 1 | INTO dst",
                new QueryParams(),
                new InferenceSettings(Settings.EMPTY),
                "my_view",
                QueryPragmas.EMPTY,
                EsqlFlags.DEFAULTS
            )
        );
        assertThat(e.getMessage(), containsString("INTO is not allowed in a view definition"));
    }

    public void testVerifyProducerRequiresRowSource() {
        Into rowInto = as(query("ROW a = 1 | EVAL b = a + 1 | INTO dst"), Into.class);
        Into.verifyProducer(rowInto.child());

        Into fromInto = as(query("FROM src | INTO dst"), Into.class);
        VerificationException e = expectThrows(VerificationException.class, () -> Into.verifyProducer(fromInto.child()));
        assertThat(e.getMessage(), containsString("INTO currently only supports ROW as a source"));
    }
}

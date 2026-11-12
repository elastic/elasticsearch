/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.settings.Settings;
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
        expectError("ROW a = 1 | INTO dst | LIMIT 1", error);
        expectError("ROW a = 1 | FORK (INTO dst) (LIMIT 1)", error);
        expectError("FROM (ROW a = 1 | INTO dst)", error);
        expectError("ROW a = 1 | WHERE a IN (ROW b = 1 | INTO dst)", error);
        expectError("EXPLAIN (ROW a = 1 | INTO dst)", error);
        expectValidationError("LET x = (ROW b = 1 | INTO dst); ROW a = 1", error);
    }

    public void testOnlyRowSource() {
        String error = "INTO currently only supports ROW as a source, without FORK";
        expectError("FROM src | INTO dst", error);
        expectError("ROW a = 1 | FORK (WHERE true) (WHERE true) | INTO dst", error);
        expectError("EXPLAIN (ROW a = 1) | INTO dst", error);
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
}

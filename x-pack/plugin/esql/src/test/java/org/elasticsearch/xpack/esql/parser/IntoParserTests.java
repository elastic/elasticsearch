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
import org.elasticsearch.xpack.esql.into.IntoOptions;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
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

    public void testMustBeLastCommandInSubqueries() {
        String error = "INTO must be the last command of the query";
        expectError("FROM (ROW a = 1 | INTO dst)", error);
        expectError("FROM (ROW a = 1 | INTO dst | LIMIT 1)", error);
        expectError("ROW a = 1 | WHERE a IN (ROW b = 1 | INTO dst)", error);
        expectValidationError("LET x = (ROW b = 1 | INTO dst | LIMIT 1); ROW a = 1", error);
        expectValidationError("LET x = (ROW b = 1 | INTO dst); ROW a = 1", error);
        expectError("EXPLAIN (ROW a = 1) | INTO dst", "EXPLAIN does not support downstream commands");
    }

    public void testViewBodyRejected() {
        expectThrows(
            ParsingException.class,
            "INTO is not allowed in a view definition",
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

    public void testEmptyOptions() {
        Into into = as(query("ROW a = 1 | INTO dst WITH {}"), Into.class);
        assertThat(into.destination(), equalTo("dst"));
    }

    public void testUnknownOptionRejected() {
        expectError("ROW a = 1 | INTO dst WITH {\"foo\": 1}", "Invalid option [foo] in INTO, expected one of [refresh]");
        expectError("ROW a = 1 | INTO dst WITH {\"foo\": 1, \"bar\": 2}", "Invalid options [foo, bar] in INTO, expected one of [refresh]");
    }

    public void testRefreshDefaultsToTrue() {
        assertThat(as(query("ROW a = 1 | INTO dst"), Into.class).options(), equalTo(IntoOptions.DEFAULT));
        assertThat(as(query("ROW a = 1 | INTO dst WITH {}"), Into.class).options().refresh(), equalTo(true));
    }

    public void testRefresh() {
        boolean refresh = randomBoolean();
        assertThat(as(query("ROW a = 1 | INTO dst WITH {\"refresh\": " + refresh + "}"), Into.class).options().refresh(), equalTo(refresh));
    }

    public void testOptionNamesAreCaseInsensitive() {
        assertThat(as(query("ROW a = 1 | INTO dst WITH {\"REFRESH\": false}"), Into.class).options().refresh(), equalTo(false));
        assertThat(as(query("ROW a = 1 | into dst with {\"Refresh\": false}"), Into.class).options().refresh(), equalTo(false));
    }

    public void testRefreshFromParameter() {
        Into into = as(
            query("ROW a = 1 | INTO dst WITH {\"refresh\": ?r}", new QueryParams(List.of(paramAsConstant("r", false)))),
            Into.class
        );
        assertThat(into.options().refresh(), equalTo(false));
    }

    public void testRefreshMustBeBoolean() {
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": \"no\"}", "Option [refresh] must be a boolean literal");
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": 1}", "Option [refresh] must be a boolean literal");
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": null}", "Option [refresh] must be a boolean literal");
    }

    public void testOptionsDifferingOnlyInCaseRejected() {
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": true, \"REFRESH\": false}", "Duplicate option [refresh] in INTO");
    }

    public void testExplainInto() {
        Explain explain = as(query("EXPLAIN (ROW a = 1 | INTO dst)"), Explain.class);
        assertThat(as(explain.query(), Into.class).destination(), equalTo("dst"));
    }
}

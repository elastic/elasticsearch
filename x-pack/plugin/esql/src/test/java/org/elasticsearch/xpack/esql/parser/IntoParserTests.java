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
import static org.elasticsearch.xpack.esql.core.util.TestUtils.randomCasing;
import static org.hamcrest.Matchers.equalTo;

public class IntoParserTests extends AbstractStatementParserTests {
    @Before
    public void requiresSnapshot() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testDestination() {
        Into into = as(query("ROW a = 1 | INTO dst"), Into.class);
        assertThat(into.destination(), equalTo("dst"));
        assertThat(into.options(), equalTo(IntoOptions.DEFAULT));
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
        assertThat(into.options(), equalTo(IntoOptions.DEFAULT));
    }

    public void testMultipleUnknownOptionsRejected() {
        expectError(
            "ROW a = 1 | INTO dst WITH {\"foo\": 1, \"bar\": 2}",
            "Invalid options [foo, bar] in INTO, expected one of [refresh, keep_going]"
        );
    }

    public void testRefresh() {
        assertTrue(as(query("ROW a = 1 | INTO dst WITH {\"refresh\": true}"), Into.class).options().refresh());
        assertFalse(as(query("ROW a = 1 | INTO dst WITH {\"refresh\": false}"), Into.class).options().refresh());
    }

    public void testOptionNamesAreCaseInsensitive() {
        String query = "ROW a = 1 | INTO dst WITH {\"" + randomCasing("refresh") + "\": false}";
        assertFalse(as(query(query), Into.class).options().refresh());
    }

    public void testKeywordsAreCaseInsensitiveWithOptions() {
        String query = "ROW a = 1 | " + randomCasing("into") + " dst " + randomCasing("with") + " {\"refresh\": false}";
        assertFalse(as(query(query), Into.class).options().refresh());
    }

    public void testRefreshFromParameter() {
        Into into = as(
            query("ROW a = 1 | INTO dst WITH {\"refresh\": ?r}", new QueryParams(List.of(paramAsConstant("r", false)))),
            Into.class
        );
        assertFalse(into.options().refresh());
    }

    public void testRefreshMustBeBoolean() {
        String error = "Option [refresh] must be a boolean literal";
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": \"no\"}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": 1}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": null}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": [true, false]}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": [true]}", error);
    }

    public void testOptionsDifferingOnlyInCaseRejected() {
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": true, \"REFRESH\": false}", "Duplicate option [refresh] in INTO");
    }

    /**
     * The map builder only remembers the names of entries with a literal value, so a name that first had a map as its value can come
     * again. INTO checks the entries it is given, so that the invalid first value cannot be hidden by the second.
     */
    public void testOptionRepeatedAfterAMapValueRejected() {
        String error = "Duplicate option [refresh] in INTO";
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": {}, \"refresh\": false}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"refresh\": {\"x\": 1}, \"REFRESH\": true}", error);
    }

    public void testKeepGoingDefaultsToFalse() {
        assertFalse(as(query("ROW a = 1 | INTO dst"), Into.class).options().keepGoing());
        assertFalse(as(query("ROW a = 1 | INTO dst WITH {\"keep_going\": false}"), Into.class).options().keepGoing());
    }

    public void testKeepGoing() {
        assertTrue(as(query("ROW a = 1 | INTO dst WITH {\"keep_going\": true}"), Into.class).options().keepGoing());
    }

    public void testKeepGoingNameIsCaseInsensitive() {
        assertTrue(as(query("ROW a = 1 | INTO dst WITH {\"KEEP_Going\": true}"), Into.class).options().keepGoing());
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": true, \"KEEP_GOING\": false}", "Duplicate option [keep_going] in INTO");
    }

    public void testKeepGoingFromParameter() {
        Into into = as(
            query("ROW a = 1 | INTO dst WITH {\"keep_going\": ?kg}", new QueryParams(List.of(paramAsConstant("kg", true)))),
            Into.class
        );
        assertTrue(into.options().keepGoing());
    }

    public void testKeepGoingMustBeBoolean() {
        String error = "Option [keep_going] must be a boolean literal";
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": \"yes\"}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": 1}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": null}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": [true, false]}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": {\"x\": true}}", error);
    }

    public void testDuplicateOptionRejected() {
        expectError(
            "ROW a = 1 | INTO dst WITH {\"foo\": 1, \"foo\": 2}",
            "Duplicated named parameters with the same name [foo] is not supported"
        );
    }

    public void testRefreshAndKeepGoingTogether() {
        Into into = as(query("ROW a = 1 | INTO dst WITH {\"refresh\": false, \"keep_going\": true}"), Into.class);
        assertThat(into.options(), equalTo(new IntoOptions(false, true)));
    }

    public void testExplainInto() {
        Explain explain = as(query("EXPLAIN (ROW a = 1 | INTO dst)"), Explain.class);
        assertThat(as(explain.query(), Into.class).destination(), equalTo("dst"));
    }

    public void testExplainIntoWithOptions() {
        Explain explain = as(query("EXPLAIN (ROW a = 1 | INTO dst WITH {\"refresh\": false})"), Explain.class);
        assertFalse(as(explain.query(), Into.class).options().refresh());
    }

    public void testKeywordDestination() {
        assertThat(as(query("ROW a = 1 | INTO \"with\""), Into.class).destination(), equalTo("with"));
    }
}

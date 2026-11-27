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
import org.elasticsearch.xpack.esql.into.IntoOptions.Collisions;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
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
    public void requiresIntoCommand() {
        assumeTrue("INTO command required", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
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
            "ROW a = 1 | INTO dst WITH {\"Foo\": 1, \"BAR\": 2}",
            "Invalid options [Foo, BAR] in INTO, expected one of [refresh, keep_going, collisions]"
        );
    }

    public void testRefresh() {
        assertTrue(as(query("ROW a = 1 | INTO dst WITH {\"refresh\": true}"), Into.class).options().refresh());
        assertFalse(as(query("ROW a = 1 | INTO dst WITH {\"refresh\": false}"), Into.class).options().refresh());
    }

    public void testOptionNamesAndCollisionsValuesAreCaseInsensitive() {
        String query = "ROW a = 1 | INTO dst WITH {\""
            + randomCasing("refresh")
            + "\": false, \""
            + randomCasing("keep_going")
            + "\": true, \""
            + randomCasing("collisions")
            + "\": \""
            + randomCasing("replace")
            + "\"}";
        assertThat(
            intoOptions(query),
            equalTo(IntoOptions.builder().refresh(false).keepGoing(true).collisions(Collisions.REPLACE).build())
        );
    }

    public void testOptionsFromParameters() {
        LogicalPlan plan = query(
            "ROW a = 1 | INTO dst WITH {\"refresh\": ?r, \"keep_going\": ?kg, \"collisions\": ?c}",
            new QueryParams(List.of(paramAsConstant("r", false), paramAsConstant("kg", true), paramAsConstant("c", "replace")))
        );
        assertThat(
            as(plan, Into.class).options(),
            equalTo(IntoOptions.builder().refresh(false).keepGoing(true).collisions(Collisions.REPLACE).build())
        );
    }

    public void testKeywordsAreCaseInsensitiveWithOptions() {
        String query = "ROW a = 1 | " + randomCasing("into") + " dst " + randomCasing("with") + " {\"refresh\": false}";
        assertFalse(as(query(query), Into.class).options().refresh());
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
    }

    public void testExplicitKeepGoingFalse() {
        assertFalse(as(query("ROW a = 1 | INTO dst WITH {\"keep_going\": false}"), Into.class).options().keepGoing());
    }

    public void testKeepGoing() {
        String query = "ROW a = 1 | INTO dst WITH {\"" + randomCasing("keep_going") + "\": true}";
        assertTrue(as(query(query), Into.class).options().keepGoing());
    }

    public void testCaseFoldedDuplicateRejected() {
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": true, \"KEEP_GOING\": false}", "Duplicate option [keep_going] in INTO");
    }

    public void testKeepGoingMustBeBoolean() {
        String error = "Option [keep_going] must be a boolean literal";
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": \"yes\"}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": 1}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": null}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": [true, false]}", error);
        expectError("ROW a = 1 | INTO dst WITH {\"keep_going\": {\"x\": true}}", error);
    }

    public void testAllOptionsTogether() {
        Into into = as(query("ROW a = 1 | INTO dst WITH {\"refresh\": false, \"keep_going\": true, \"collisions\": \"keep\"}"), Into.class);
        assertThat(into.options(), equalTo(IntoOptions.builder().refresh(false).keepGoing(true).collisions(Collisions.KEEP).build()));
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

    public void testCollisionsDefaultsToAbort() {
        assertThat(IntoOptions.DEFAULT.collisions(), equalTo(Collisions.ABORT));
    }

    public void testCollisionsInvalidValue() {
        expectError(
            "ROW a = 1 | INTO dst WITH {\"collisions\": \"overwrite\"}",
            "Invalid value [overwrite] for option [collisions] in INTO, expected one of [abort, keep, replace]"
        );
    }

    private IntoOptions intoOptions(String query) {
        return as(query(query), Into.class).options();
    }
}

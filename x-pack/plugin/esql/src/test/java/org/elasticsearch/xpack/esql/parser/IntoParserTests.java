/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.into.IntoOptions;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** How {@code INTO destination [option=value, ...]} parses, and what it refuses. */
public class IntoParserTests extends AbstractStatementParserTests {

    @Before
    public void requiresSnapshot() {
        assumeTrue("INTO is behind the esql_into feature flag", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testDestination() {
        Into into = as(query("FROM src | INTO dst"), Into.class);
        assertThat(into.destination(), equalTo("dst"));
        assertThat(into.options(), equalTo(IntoOptions.DEFAULT));
        assertThat(as(into.child(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("src"));
    }

    public void testQuotedDestination() {
        Into into = as(query("FROM src | INTO \"my-dst\""), Into.class);
        assertThat(into.destination(), equalTo("my-dst"));
    }

    /** The command and its options are case-insensitive, and the values are typed. */
    public void testOptions() {
        Into into = as(query("FROM src | into dst WITH { \"COLLISIONS\": \"Keep\", \"Refresh\": false }"), Into.class);
        assertThat(into.options(), equalTo(new IntoOptions(IntoOptions.Collisions.KEEP, false, false, false)));

        into = as(query("FROM src | INTO dst WITH {\"collisions\": \"replace\", \"keep_going\": true}"), Into.class);
        assertThat(into.options(), equalTo(new IntoOptions(IntoOptions.Collisions.REPLACE, true, true, false)));

        into = as(query("FROM src | INTO dst WITH {\"approximate\": true}"), Into.class);
        assertThat(into.options(), equalTo(new IntoOptions(IntoOptions.Collisions.ABORT, true, false, true)));

        into = as(query("FROM src | INTO dst WITH {\"refresh\": true}"), Into.class);
        assertThat("refresh is on by default", into.options(), equalTo(IntoOptions.DEFAULT));
        assertThat(as(query("FROM src | INTO dst"), Into.class).options(), equalTo(IntoOptions.DEFAULT));
        assertThat(as(query("FROM src | INTO dst WITH {}"), Into.class).options(), equalTo(IntoOptions.DEFAULT));
    }

    /** A parameter stands for its value, with the type it was given. */
    public void testOptionsFromParameters() {
        QueryParams params = new QueryParams(
            List.of(
                new QueryParam("r", false, DataType.BOOLEAN, ParserUtils.ParamClassification.VALUE),
                new QueryParam("c", BytesRefs.toBytesRef("keep"), DataType.KEYWORD, ParserUtils.ParamClassification.VALUE)
            )
        );
        Into into = as(query("FROM src | INTO dst WITH {\"refresh\": ?r, \"collisions\": ?c}", params), Into.class);
        assertThat(into.options(), equalTo(new IntoOptions(IntoOptions.Collisions.KEEP, false, false, false)));
    }

    public void testBadOptions() {
        expectError(
            "FROM src | INTO dst WITH {\"mode\": \"keep\"}",
            "unknown option [mode] for INTO, expected one of [collisions, refresh, keep_going, approximate]"
        );
        expectError(
            "FROM src | INTO dst WITH {\"collisions\": \"merge\"}",
            "invalid value [merge] for option [collisions], expected one of [\"abort\", \"replace\", \"keep\"]"
        );
        expectError(
            "FROM src | INTO dst WITH {\"refresh\": \"true\"}",
            "invalid value [true] for option [refresh], expected a boolean, true or false"
        );
        expectError("FROM src | INTO dst WITH {\"refresh\": true, \"REFRESH\": false}", "duplicate option [refresh] for INTO");
        expectError(
            "FROM src | INTO dst WITH {\"collisions\": {\"x\": 1}}",
            "option [collisions] for INTO must be a literal or a parameter"
        );
        expectError("FROM src | INTO dst WITH {\"collisions\": 1 + 1}", "mismatched input");
        expectError("FROM src | INTO dst collisions=keep", "token recognition error at: '='");
        expectError("FROM src | INTO dst WITH", "mismatched input");
    }

    public void testBadDestination() {
        expectError("FROM src | INTO dst*", "invalid index pattern [dst*], * is not allowed in INTO");
        expectError("FROM src | INTO remote:dst", "invalid index pattern [remote:dst], remote clusters are not supported with INTO");
        expectError("FROM src | INTO dst::data", "invalid index pattern [dst::data], index pattern selectors are not supported in INTO");
    }

    /** INTO ends the query: nothing may follow it, and it cannot sit inside a subquery, a FORK branch or EXPLAIN. */
    public void testMustBeTheLastCommand() {
        expectError("FROM src | INTO dst | LIMIT 1", "INTO must be the last command of the query");
        expectError("FROM src | INTO dst | INTO other", "INTO must be the last command of the query");
        expectError("FROM src | FORK (INTO dst) (LIMIT 1)", "INTO must be the last command of the query");
        expectError("FROM (FROM src | INTO dst)", "INTO must be the last command of the query");
        expectError("EXPLAIN (FROM src | INTO dst)", "EXPLAIN does not support INTO");
    }

    /** A sort the destination cannot keep is refused rather than attempted over the whole result. */
    public void testUnlimitedSortIsRefused() {
        Into into = as(query("FROM src | SORT host | INTO dst"), Into.class);
        VerificationException e = expectThrows(VerificationException.class, () -> Into.verifyProducer(into.child(), into.destination()));
        assertThat(e.getMessage(), containsString("[dst]: SORT has no effect on what INTO writes; remove it, or add an explicit LIMIT"));
    }

    /** With a limit the sort is bounded, so it stands. */
    public void testLimitedSortIsAccepted() {
        Into into = as(query("FROM src | SORT host | LIMIT 10 | INTO dst"), Into.class);
        Into.verifyProducer(into.child(), into.destination());
        as(into.child(), Limit.class);

        Into withSortBelow = as(query("FROM src | SORT host | LIMIT 10 | EVAL x = 1 | INTO dst"), Into.class);
        Into.verifyProducer(withSortBelow.child(), withSortBelow.destination());
    }
}

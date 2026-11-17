/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class SubqueryJoinParserTests extends AbstractStatementParserTests {

    public void testLeftOuterJoinSubquery() {
        assumeDevVersion();
        List<Attribute> key = List.of(attribute("key"));
        assertQuery(
            "FROM left | LEFT OUTER JOIN (FROM right | KEEP key) ON key",
            new SubqueryJoin(
                EMPTY,
                relation("left"),
                new Keep(EMPTY, relation("right"), key),
                new JoinConfig(JoinTypes.LEFT, key, key, null)
            )
        );
    }

    public void testLeftOuterJoinSubqueryTargets() {
        assumeDevVersion();
        for (String target : List.of(
            "(ROW key = 1)",
            "(TS metrics | KEEP key)",
            "(FROM (FROM nested) | KEEP key)",
            "(FROM right /* nested comment */ | WHERE key > 1 | KEEP key)",
            "(FROM right | LOOKUP JOIN nested ON key)",
            "(FROM right | LEFT OUTER JOIN (FROM nested) ON key)"
        )) {
            var join = as(query("FROM left | LEFT OUTER JOIN " + target + " ON key"), SubqueryJoin.class);
            assertThat(target, join.config().type(), equalTo(JoinTypes.LEFT));
        }
    }

    public void testMultipleJoinFields() {
        assumeDevVersion();
        var join = as(query("FROM left | LEFT OUTER JOIN (FROM right) ON a, b"), SubqueryJoin.class);
        assertThat(join.config().leftFields().stream().map(Attribute::name).toList(), contains("a", "b"));
        assertThat(join.config().rightFields().stream().map(Attribute::name).toList(), contains("a", "b"));
        assertNull(join.config().joinOnConditions());
    }

    public void testIndexPatternTarget() {
        assumeDevVersion();
        for (String target : List.of("right", "right*", "\"right\"", "remote:right")) {
            expectError("FROM left | LEFT OUTER JOIN " + target + " ON key", "LEFT OUTER JOIN requires a subquery, found [");
        }
    }

    public void testExpressionJoinConditions() {
        assumeDevVersion();
        for (String condition : List.of("a1 == a2", "a1 == a2 AND b1 == b2 AND c1 > c2", "a1 == a2 OR b1 == b2", "key, a1 == a2")) {
            expectError(
                "FROM left | LEFT OUTER JOIN (FROM right) ON " + condition,
                "JOIN on a subquery only supports a comma separated list of fields in ON"
            );
        }
    }

    public void testInvalidJoinFields() {
        assumeDevVersion();
        expectError("FROM left | LEFT OUTER JOIN (FROM right) ON key, key", "found multiple instances of [key]");
        expectError("FROM left | LEFT OUTER JOIN (FROM right) ON 1", "JOIN on a subquery only supports a comma separated list of fields");
    }

    public void testInnerJoin() {
        assumeDevVersion();
        for (String target : List.of("(FROM right | KEEP key)", "right")) {
            expectError("FROM left | INNER JOIN " + target + " ON key", "only LOOKUP JOIN available, INNER JOIN unsupported");
        }
    }

    public void testOuterKeyword() {
        assumeDevVersion();
        expectError("FROM left | LEFT fake JOIN (FROM right) ON key", "no viable alternative at input 'LEFT fake'");
        expectError("FROM left | INNER OUTER JOIN (FROM right) ON key", "extraneous input 'OUTER' expecting 'join'");
    }

    public void testLookupJoinRejectsSubqueryTarget() {
        expectError("FROM main | LOOKUP JOIN (FROM sub) ON key", "mismatched input '('");
    }

    public void testTimeSeriesSubqueryDoesNotMakeStatsTimeSeries() {
        assumeDevVersion();
        LogicalPlan plan = query("FROM left | LEFT OUTER JOIN (TS metrics | KEEP key) ON key | STATS c = COUNT(*)");
        assertThat(plan.getClass(), equalTo(Aggregate.class));
    }

    public void testSubqueryStatsDoesNotHideTimeSeriesStats() {
        assumeDevVersion();
        LogicalPlan plan = query("TS metrics | LEFT OUTER JOIN (FROM right | STATS c = COUNT(*) BY key) ON key | STATS m = MAX(c)");
        as(plan, TimeSeriesAggregate.class);
    }

    public void testRemoteEnrichDoesNotChangeSubqueryLookupJoin() {
        assumeDevVersion();
        LogicalPlan plan = query("""
            FROM left
            | LOOKUP JOIN outer_lookup ON key
            | LEFT OUTER JOIN (FROM right | LOOKUP JOIN inner_lookup ON key) ON key
            | ENRICH _remote:policy ON key""");
        var enrich = as(plan, Enrich.class);
        var join = as(enrich.child(), SubqueryJoin.class);
        assertThat(as(join.left(), LookupJoin.class).executesOn(), equalTo(ExecuteLocation.REMOTE));
        assertThat(as(join.right(), LookupJoin.class).executesOn(), equalTo(ExecuteLocation.ANY));
    }

    private static void assumeDevVersion() {
        assumeTrue("requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
    }
}

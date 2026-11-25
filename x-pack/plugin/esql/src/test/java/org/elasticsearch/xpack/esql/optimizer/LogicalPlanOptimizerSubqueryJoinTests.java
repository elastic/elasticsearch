/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.junit.Before;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/**
 * Logical optimization of {@code LEFT OUTER JOIN} on a subquery that is rejected. The plans of queries that optimize are captured in
 * {@link LogicalPlanOptimizerSubqueryJoinGoldenTests}.
 */
public class LogicalPlanOptimizerSubqueryJoinTests extends AbstractLogicalPlanOptimizerTests {

    private static final String JOIN_LANGUAGES = """
        FROM test
        | EVAL language_code = languages
        | LEFT OUTER JOIN (FROM languages) ON language_code
        """;

    public LogicalPlanOptimizerSubqueryJoinTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void checkGrammar() {
        assumeTrue("LEFT OUTER JOIN requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
    }

    /**
     * The join copies the subquery's columns with {@code Block#lookup}, which some block types do not implement. The check runs after
     * optimization, so a column that nothing reads after the join is pruned instead of rejected (see
     * {@link LogicalPlanOptimizerSubqueryJoinGoldenTests#testPayloadTypeWithoutLookupIsPruned}).
     */
    public void testPayloadTypesWithoutLookup() {
        String query = """
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (ROW language_code = 1, m = TO_AGGREGATE_METRIC_DOUBLE(1)) ON language_code
            """;
        VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(query));
        assertThat(
            e.getMessage(),
            containsString("JOIN subquery cannot return column [m] of type [AGGREGATE_METRIC_DOUBLE]; remove it with KEEP or DROP")
        );
    }

    /**
     * An index search runs against the left side's documents, which the join's output no longer carries. A search that the optimizer
     * can't push below the join, because the same condition reads a subquery column, is rejected.
     */
    public void testIndexSearchAboveJoinIsRejected() {
        Map<String, String> searches = Map.of("MATCH(first_name, \"Anna\")", "[MATCH] function", "first_name : \"Anna\"", "[:] operator");
        for (Map.Entry<String, String> search : searches.entrySet()) {
            String query = JOIN_LANGUAGES + "| WHERE " + search.getKey() + " OR language_name IS NOT NULL\n";
            VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(query));
            assertThat(
                e.getMessage(),
                containsString(search.getValue() + " cannot be used after a JOIN on a subquery when it targets an indexed field")
            );
        }
    }

    /**
     * A filter can also stay above the join because of a command between them, here {@code INLINE STATS}, although it only reads
     * the left side's columns.
     */
    public void testIndexSearchAboveInlineStatsAfterJoinIsRejected() {
        VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(JOIN_LANGUAGES + """
            | INLINE STATS c = COUNT(*) BY gender
            | WHERE MATCH(first_name, "Anna")
            """));
        assertThat(
            e.getMessage(),
            containsString("line 5:9: [MATCH] function cannot be used after a JOIN on a subquery when it targets an indexed field")
        );
    }

    /**
     * The aggregation of a {@code STATS} after the join runs on the coordinator, so a search in the filter of an aggregate function
     * is rejected too.
     */
    public void testIndexSearchInAggregateFilterAfterJoinIsRejected() {
        VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(JOIN_LANGUAGES + """
            | STATS n = COUNT(*), anna = COUNT(*) WHERE MATCH(first_name, "Anna")
            """));
        assertThat(
            e.getMessage(),
            containsString("[MATCH] function cannot be used after a JOIN on a subquery when it targets an indexed field")
        );
    }

    /**
     * The aggregation of an {@code INLINE STATS} after the join runs on the coordinator too, although it reads its input through a
     * stub rather than from the join.
     */
    public void testIndexSearchInInlineStatsAggregateFilterAfterJoinIsRejected() {
        VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(JOIN_LANGUAGES + """
            | INLINE STATS anna = COUNT(*) WHERE MATCH(first_name, "Anna")
            """));
        assertThat(
            e.getMessage(),
            containsString("line 4:38: [MATCH] function cannot be used after a JOIN on a subquery when it targets an indexed field")
        );
    }

    /**
     * {@code SCORE} evaluates its search where it stands, so after the join it would search on the coordinator.
     */
    public void testIndexSearchInScoreAfterJoinIsRejected() {
        VerificationException e = expectThrows(VerificationException.class, () -> optimizedJoinPlan(JOIN_LANGUAGES + """
            | EVAL s = SCORE(MATCH(first_name, "Anna"))
            """));
        assertThat(
            e.getMessage(),
            containsString("line 4:18: [MATCH] function cannot be used after a JOIN on a subquery when it targets an indexed field")
        );
    }

    /**
     * The optimizer replaces the join with a hash join, which is the only join on a subquery the mapper can plan.
     */
    public void testMapperRejectsJoinThatWasNotReplaced() {
        LogicalPlan plan = analyzer().addEmployees("test").addLanguages().query(JOIN_LANGUAGES);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new Mapper().map(new Versioned<>(plan, minimumVersion)));
        assertThat(e.getMessage(), containsString("JOIN on a subquery must be replaced with a hash join before mapping"));
    }

    private LogicalPlan optimizedJoinPlan(String query) {
        return optimize(analyzer().addEmployees("test").addLanguages().query(query));
    }
}

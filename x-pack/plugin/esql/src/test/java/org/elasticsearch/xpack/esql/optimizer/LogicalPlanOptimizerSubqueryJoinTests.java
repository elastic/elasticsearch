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

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/**
 * Logical optimization of {@code LEFT OUTER JOIN} on a subquery that is rejected. The session rejects the join before optimization
 * until it can run, so these tests optimize the analyzed plan directly. The plans of queries that optimize are captured in
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

    public void testMapperRejectsJoin() {
        LogicalPlan plan = optimizedJoinPlan(JOIN_LANGUAGES);
        VerificationException e = expectThrows(VerificationException.class, () -> new Mapper().map(new Versioned<>(plan, minimumVersion)));
        assertThat(e.getMessage(), containsString("line 3:3: JOIN on a subquery is not supported yet"));
    }

    private LogicalPlan optimizedJoinPlan(String query) {
        return optimize(analyzer().addEmployees("test").addLanguages().query(query));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.junit.BeforeClass;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class PushDownAndCombineLimitByGoldenTests extends GoldenTestCase {

    @BeforeClass
    public static void checkLimitByCapability() {}

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    /**
     * We are marking abbrev as missing in the shard here, so a synthetic \_EvalExec[[null[KEYWORD] AS abbrev] will be introduced by
     * the ENRICH in the local physical plan, reusing the NameId. We cannot let the LimitByExec past EvalExec
     *
     * {@code
     *  \_LimitByExec[1[INTEGER],[abbrev{f}#0],70]
     *    \_EvalExec[[null[KEYWORD] AS abbrev#0]]
     * }
     */
    public void testLimitByNotPushedPastEval() {
        runGoldenTest(
            """
                FROM *
                | ENRICH languages on street
                | KEEP abbrev, integer, year
                | LIMIT 1 BY abbrev
                | SORT abbrev
                | LIMIT 5
                """,
            EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION),
            new EsqlTestUtils.TestConfigurableSearchStats().exclude(EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS, "abbrev")
        );
    }

    public void testLimitByNotPushedPastDissect() {
        runGoldenTest("""
            FROM web_logs
            | DISSECT uri "/%{path}/%{file}"
            | LIMIT 2 BY path, domain
            """, STAGES, STATS);
    }

    public void testLimitPushedPastDissect() {
        runGoldenTest("""
            FROM web_logs
            | DISSECT uri "/%{path}/%{file}"
            | LIMIT 2 BY domain
            """, STAGES, STATS);
    }

    public void testLimitByNotPushedPastUriParts() {
        runGoldenTest("""
            FROM web_logs
            | SORT uri
            | URI_PARTS p = uri
            | LIMIT 1 BY p.domain, uri
            """, STAGES, STATS);
    }

    public void testLimitPushedPastUriParts() {
        runGoldenTest("""
            FROM web_logs
            | SORT uri
            | URI_PARTS p = uri
            | LIMIT 1 BY uri
            """, STAGES, STATS);
    }

    public void testLimitByNotPushedPastRerank() {
        runGoldenTest("""
            FROM books
            | RERANK "war and peace" ON title WITH { "inference_id" : "reranking-inference-id" }
            | LIMIT 3 BY _score, author
            """, STAGES, STATS);
    }

    public void testLimitPushedPastRerank() {
        runGoldenTest("""
            FROM books
            | RERANK "war and peace" ON title WITH { "inference_id" : "reranking-inference-id" }
            | LIMIT 3 BY author
            """, STAGES, STATS);
    }

    /**
     * A LEFT JOIN (LOOKUP JOIN) can increase the number of rows, so we duplicate the LimitBy: keep the original above
     * and add a copy on the left (first) grandchild. The inner LimitBy is then pushed below the Eval by
     * {@link PushDownAndCombineLimitBy} because emp_no does not depend on the Eval.
     */
    public void testLimitByOriginalFieldDuplicated() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 5 BY emp_no
            """, STAGES, STATS);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references a field introduced by a LEFT JOIN (from the right side)
     * must not be duplicated below the join, because the field would not exist there.
     */
    public void testLimitByFieldIntroducedInTheJoinNotDuplicated() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 5 BY language_name
            """, STAGES, STATS);
    }

    /**
     * We cannot duplicate the LIMIT BY if we limit by a shadowed non-join field
     */
    public void testLimitByShadowedNonJoinFieldNotDuplicated() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | EVAL language_name = 2*salary
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 5 BY language_name
            """, STAGES, STATS);
    }

    /**
     * We duplicate the LIMIT BY if we limit by a shadowed join field
     */
    public void testLimitByShadowedJoinFieldDuplicated() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | EVAL language_name = 2*salary
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 5 BY language_code
            """, STAGES, STATS);
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class PushDownAndCombineLimitByGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public PushDownAndCombineLimitByGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

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
                FROM airport_city_boundaries, addresses, all_types, books
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

    /**
     * A LIMIT BY grouping on a source field is pushed past both RENAME (Project) and EVAL.
     */
    public void testLimitByPushedPastRenameAndEval() {
        runGoldenTest("""
            FROM employees
            | EVAL doubled = salary * 2
            | RENAME doubled AS x
            | LIMIT 5 BY languages
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY grouping on a field that is a renamed computed value must not be pushed past the RENAME.
     * The field {@code x} traces back to an EVAL-introduced attribute; it does not exist before the Project.
     */
    public void testLimitByNotPushedPastRenameOfEvalField() {
        runGoldenTest("""
            FROM employees
            | EVAL doubled = salary * 2
            | RENAME doubled AS x
            | LIMIT 5 BY x
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY whose grouping references a field introduced by an EVAL must not be pushed below the EVAL.
     */
    public void testLimitByNotPushedPastEvalWhenGroupingByEvalField() {
        runGoldenTest("""
            FROM employees
            | EVAL x = salary * 2
            | LIMIT 5 BY x
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY whose grouping references only source fields is pushed below an EVAL.
     */
    public void testLimitByPushedPastEval() {
        runGoldenTest("""
            FROM employees
            | EVAL x = salary * 2
            | LIMIT 5 BY languages
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY above a Filter is not pushed past it: pushing would change which rows are eligible,
     * so the rule correctly leaves LimitBy above Filter.
     */
    public void testLimitByNotPushedPastFilter() {
        runGoldenTest("""
            FROM employees
            | WHERE salary > 50000
            | LIMIT 5 BY languages
            """, STAGES, STATS);
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

    public void testLimitByNotPushedPastGrok() {
        runGoldenTest("""
            FROM web_logs
            | GROK uri "/%{WORD:path}/%{WORD:file}"
            | LIMIT 2 BY path, domain
            """, STAGES, STATS);
    }

    public void testLimitPushedPastGrok() {
        runGoldenTest("""
            FROM web_logs
            | GROK uri "/%{WORD:path}/%{WORD:file}"
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

    public void testLimitByNotPushedPastUserAgent() {
        runGoldenTest("""
            FROM web_logs
            | USER_AGENT ua = user_agent
            | LIMIT 1 BY ua.name, domain
            """, STAGES, STATS);
    }

    public void testLimitByPushedPastUserAgent() {
        runGoldenTest("""
            FROM web_logs
            | USER_AGENT ua = user_agent
            | LIMIT 1 BY domain
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

    public void testLimitByNotPushedPastCompletion() {
        runGoldenTest("""
            FROM books
            | COMPLETION result = title WITH { "inference_id": "completion-inference-id" }
            | LIMIT 3 BY result, author
            """, STAGES, STATS);
    }

    public void testLimitByPushedPastCompletion() {
        runGoldenTest("""
            FROM books
            | COMPLETION result = title WITH { "inference_id": "completion-inference-id" }
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
     * LIMIT BY groups by the MV_EXPAND target, so it must stay above the expand and must not be
     * duplicated below it. See https://github.com/elastic/elasticsearch/issues/148513
     */
    public void testLimitByNotDuplicatedPastMvExpandWhenGroupingByExpandTarget() {
        runGoldenTest("""
            ROW x = 1
            | MV_EXPAND x
            | LIMIT 1 BY x
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

    /**
     * MV_EXPAND can increase the number of rows, so we duplicate the LimitBy: keep the original above and add a copy below.
     */
    public void testLimitByDuplicatedPastMvExpand() {
        runGoldenTest("""
            FROM employees
            | MV_EXPAND first_name
            | LIMIT 5 BY emp_no
            """, STAGES, STATS);
    }

    /**
     * A grouped LIMIT (LIMIT BY) above a Fork must not be pushed into the fork branches.
     */
    public void testLimitByNotPushedIntoForkBranches() {
        runGoldenTest("""
            FROM employees
            | FORK (WHERE emp_no > 100) (WHERE emp_no < 10)
            | LIMIT 5 BY emp_no
            """, STAGES, STATS);
    }

    /**
     * Three LIMIT BY nodes with the same grouping: the minimum limit value wins, leaving only one node.
     */
    public void testLimitByPruneIdenticalLimits() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY emp_no
            | LIMIT 2 BY emp_no
            | LIMIT 1 BY emp_no
            """, STAGES, STATS);
    }

    /**
     * Two LIMIT BY nodes with different groupings must both be preserved.
     */
    public void testLimitByKeepDifferentGroupings() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY emp_no
            | LIMIT 1 BY first_name
            """, STAGES, STATS);
    }

    /**
     * A plain LIMIT separating two LIMIT BY nodes with the same grouping prevents combining them.
     */
    public void testLimitByNotCombinedWhenSeparatedByPlainLimit() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY emp_no
            | LIMIT 2
            | LIMIT 2 BY emp_no
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY above a TopN (SORT + LIMIT) must not be combined with the TopN.
     */
    public void testLimitByNotCombinedWithTopN() {
        runGoldenTest("""
            FROM employees
            | SORT emp_no
            | LIMIT 1000
            | LIMIT 2 BY languages
            | STATS c = COUNT(*) BY languages
            | SORT languages ASC NULLS LAST
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY whose grouping references a field introduced by a local ENRICH must not be pushed below the ENRICH.
     */
    public void testLimitByNotPushedBelowLocalEnrichWhenGroupingReferencesEnrichField() {
        runGoldenTest("""
            FROM employees
            | ENRICH languages ON first_name
            | LIMIT 5 BY language_name
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY whose grouping references a field introduced by a remote ENRICH must not be pushed or duplicated below the ENRICH.
     */
    public void testLimitByNotPushedBelowRemoteEnrichWhenGroupingReferencesEnrichField() {
        runGoldenTest("""
            FROM employees
            | ENRICH _remote:languages_remote ON first_name
            | LIMIT 5 BY language_name
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY whose grouping references both a source field and an enrich field must not be pushed below the local ENRICH,
     * because the enrich field is unavailable below it.
     */
    public void testLimitByNotPushedBelowLocalEnrichWhenSomeGroupingReferencesEnrichField() {
        runGoldenTest("""
            FROM employees
            | ENRICH languages ON first_name
            | LIMIT 5 BY emp_no, language_name
            """, STAGES, STATS);
    }

    /**
     * A LIMIT BY whose grouping references only source fields is pushed below a local ENRICH.
     */
    public void testLimitByPushedBelowLocalEnrichWhenGroupingOnSourceField() {
        runGoldenTest("""
            FROM employees
            | ENRICH languages ON first_name
            | LIMIT 5 BY emp_no
            """, STAGES, STATS);
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}

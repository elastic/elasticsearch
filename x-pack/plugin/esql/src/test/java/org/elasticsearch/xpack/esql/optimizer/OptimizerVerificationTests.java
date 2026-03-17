/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OptimizerVerificationTests extends AbstractLogicalPlanOptimizerTests {

    private String error(LogicalPlan plan) {
        Throwable e = expectThrows(
            VerificationException.class,
            "Expected error for plan [" + plan + "] but no error was raised",
            () -> optimize(plan)
        );
        assertThat(e, instanceOf(VerificationException.class));

        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));

        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    public void testRemoteEnrichAfterCoordinatorOnlyPlans() {
        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json")
            .addAnalysisTestsLookupResolutions()
            .addAnalysisTestsInferenceResolution()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(Enrich.Mode.COORDINATOR, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");

        String err;

        // Remote enrich is ok after limit
        optimize(testAnalyzer.query("""
            FROM test
            | LIMIT 10
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            """));

        // Remote enrich is ok after topn
        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | SORT languages
            | ENRICH _remote:languages ON language_code
            """));
        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | SORT languages
            | LIMIT 2
            | ENRICH _remote:languages ON language_code
            """));

        // Remote enrich is ok before pipeline breakers
        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | LIMIT 10
            """));

        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            """));

        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            | LIMIT 10
            """));

        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | SORT language_name
            """));

        err = error(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | STATS count(*) BY language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(err, containsString("4:3: ENRICH with remote policy can't be executed after [STATS count(*) BY language_code]@3:3"));

        if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
            err = error(testAnalyzer.query("""
                FROM test
                | EVAL language_code = languages
                | INLINE STATS count(*) BY language_code
                | ENRICH _remote:languages ON language_code
                """));
            assertThat(
                err,
                containsString("4:3: ENRICH with remote policy can't be executed after [INLINE STATS count(*) BY language_code]@3:3")
            );
        }

        err = error(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | STATS count(*) BY language_code
            | EVAL x = 1
            | MV_EXPAND language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(err, containsString("6:3: ENRICH with remote policy can't be executed after [STATS count(*) BY language_code]@3:3"));

        // Coordinator after remote is OK
        optimize(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | ENRICH _coordinator:languages ON language_code
            """));

        err = error(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString("4:3: ENRICH with remote policy can't be executed after [ENRICH _coordinator:languages ON language_code]@3:3")
        );

        err = error(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages ON language_code
            | EVAL x = 1
            | MV_EXPAND language_name
            | DISSECT language_name "%{foo}"
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString("7:3: ENRICH with remote policy can't be executed after [ENRICH _coordinator:languages ON language_code]@3:3")
        );

        err = error(testAnalyzer.query("""
            FROM test
            | FORK (WHERE languages == 1) (WHERE languages == 2)
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString(
                "4:3: ENRICH with remote policy can't be executed after [FORK (WHERE languages == 1) (WHERE languages == 2)]@2:3"
            )
        );

        err = error(testAnalyzer.query("""
            FROM test
            | COMPLETION language_code = CONCAT("some prompt: ", first_name) WITH { "inference_id" : "completion-inference-id" }
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString(
                "ENRICH with remote policy can't be executed after "
                    + "[COMPLETION language_code = CONCAT(\"some prompt: \", first_name) "
                    + "WITH { \"inference_id\" : \"completion-inference-id\" }]@2:3"
            )
        );

        err = error(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | RERANK "test" ON first_name WITH { "inference_id" : "reranking-inference-id" }
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString(
                "ENRICH with remote policy can't be executed after "
                    + "[RERANK \"test\" ON first_name WITH { \"inference_id\" : \"reranking-inference-id\" }]@3:3"
            )
        );

        err = error(testAnalyzer.query("""
            FROM test
            | CHANGE_POINT salary ON languages
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(err, containsString("4:3: ENRICH with remote policy can't be executed after [CHANGE_POINT salary ON languages]@2:3"));
    }

    /**
     * The validation should not trigger for remote enrich after a lookup join. Lookup joins can be executed anywhere.
     */
    public void testRemoteEnrichAfterLookupJoin() {
        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json")
            .addAnalysisTestsLookupResolutions()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");

        String lookupCommand = randomBoolean() ? "LOOKUP JOIN test_lookup ON languages" : "LOOKUP JOIN languages_lookup ON language_code";

        optimize(testAnalyzer.query(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | %s
            """, lookupCommand)));

        optimize(testAnalyzer.query(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | ENRICH _remote:languages ON language_code
            """, lookupCommand)));

        optimize(testAnalyzer.query(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | ENRICH _remote:languages ON language_code
            | %s
            """, lookupCommand, lookupCommand)));

        optimize(testAnalyzer.query(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | EVAL x = 1
            | MV_EXPAND language_code
            | ENRICH _remote:languages ON language_code
            """, lookupCommand)));
    }

    public void testRemoteLookupJoinWithPipelineBreaker() {
        var testAnalyzer = analyzer().addIndex("test,remote:test", "mapping-default.json")
            .addAnalysisTestsLookupResolutions()
            .addAnalysisTestsEnrichResolution();
        assertEquals(
            "1:92: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@1:25",
            error(
                testAnalyzer.query(
                    "FROM test,remote:test | STATS c = COUNT(*) by languages "
                        + "| EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"
                )
            )
        );

        assertEquals(
            "1:72: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@1:25",
            error(
                testAnalyzer.query(
                    "FROM test,remote:test | SORT emp_no | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"
                )
            )
        );
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 1:25: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                + "add another SORT after the LOOKUP JOIN if order is required"
        );

        assertEquals(
            "1:68: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@1:25",
            testAnalyzer.stripErrorPrefix(true)
                .error("FROM test,remote:test | LIMIT 2 | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code")
        );

        assertEquals(
            "1:96: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@1:58",
            error(
                testAnalyzer.query(
                    "FROM test,remote:test | EVAL language_code = languages | ENRICH _coordinator:languages_coord "
                        + "| LOOKUP JOIN languages_lookup ON language_code"
                )
            )
        );

        optimize(
            testAnalyzer.query(
                "FROM test,remote:test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code | LIMIT 2"
            )
        );
    }

    public void testRemoteEnrichAfterLookupJoinWithPipelineBreakerCCS() {
        var testAnalyzer = analyzer().addIndex("test,remote:test", "mapping-default.json")
            .addAnalysisTestsLookupResolutions()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(
                Enrich.Mode.COORDINATOR,
                MATCH_TYPE,
                "languages_coord",
                "language_code",
                "languages_idx",
                "mapping-languages.json"
            );

        String err = error(testAnalyzer.query("""
            FROM test,remote:test
            | STATS c = COUNT(*) by languages
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@2:3")
        );

        err = error(testAnalyzer.query("""
            FROM test,remote:test
            | SORT emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(err, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@2:3"));
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 2:3: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                + "add another SORT after the LOOKUP JOIN if order is required"
        );

        err = testAnalyzer.stripErrorPrefix(true).error("""
            FROM test,remote:test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """);
        assertThat(err, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

        err = error(testAnalyzer.query("""
            FROM test,remote:test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages_coord
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@3:3")
        );
    }

    public void testRemoteEnrichAfterLookupJoinWithPipelineBreaker() {
        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json")
            .addAnalysisTestsLookupResolutions()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(
                Enrich.Mode.COORDINATOR,
                MATCH_TYPE,
                "languages_coord",
                "language_code",
                "languages_idx",
                "mapping-languages.json"
            );

        String err = error(testAnalyzer.query("""
            FROM test
            | STATS c = COUNT(*) by languages
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@2:3")
        );

        err = error(testAnalyzer.query("""
            FROM test
            | SORT emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(err, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@2:3"));
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 2:3: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                + "add another SORT after the LOOKUP JOIN if order is required"
        );

        err = testAnalyzer.stripErrorPrefix(true).error("""
            FROM test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """);
        assertThat(err, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

        err = error(testAnalyzer.query("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages_coord
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """));
        assertThat(
            err,
            containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@3:3")
        );
    }

    public void testDanglingOrderByInInlineStats() {
        assumeTrue("INLINE STATS must be enabled", INLINE_STATS.isEnabled());
        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json")
            .addAnalysisTestsLookupResolutions()
            .addAnalysisTestsEnrichResolution();

        var err = error(testAnalyzer.query("""
            FROM test
            | SORT languages
            | MV_EXPAND languages
            | INLINE STATS count(*) BY languages
            | INLINE STATS s = sum(salary) BY first_name
            """));

        assertThat(err, is("""
            2:3: Unbounded SORT not supported yet [SORT languages] please add a LIMIT
            line 4:3: INLINE STATS [INLINE STATS count(*) BY languages] cannot yet have an unbounded SORT [SORT languages] before\
             it : either move the SORT after it, or add a LIMIT before the SORT
            line 5:3: INLINE STATS [INLINE STATS s = sum(salary) BY first_name] cannot yet have an unbounded SORT [SORT languages]\
             before it : either move the SORT after it, or add a LIMIT before the SORT"""));
    }
}

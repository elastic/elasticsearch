/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OptimizerVerificationTests extends AbstractLogicalPlanOptimizerTests {

    public void testRemoteEnrichAfterCoordinatorOnlyPlans() {
        var testAnalyzer = analyzer().addDefaultIndex()
            .addLanguagesLookup()
            .addTestLookup()
            .addAnalysisTestsInferenceResolution()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(Enrich.Mode.COORDINATOR, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");

        // Remote enrich is ok after limit
        testAnalyzer.plans("""
            FROM test
            | LIMIT 10
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            """).coordinatorLogicalOptimized();

        // Remote enrich is ok after topn
        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | SORT languages
            | ENRICH _remote:languages ON language_code
            """).coordinatorLogicalOptimized();
        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | SORT languages
            | LIMIT 2
            | ENRICH _remote:languages ON language_code
            """).coordinatorLogicalOptimized();

        // Remote enrich is ok before pipeline breakers
        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | LIMIT 10
            """).coordinatorLogicalOptimized();

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            """).coordinatorLogicalOptimized();

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            | LIMIT 10
            """).coordinatorLogicalOptimized();

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | SORT language_name
            """).coordinatorLogicalOptimized();

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | STATS count(*) BY language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: ENRICH with remote policy can't be executed after [STATS count(*) BY language_code]@3:3")
            );

        if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
            testAnalyzer.plans("""
                FROM test
                | EVAL language_code = languages
                | INLINE STATS count(*) BY language_code
                | ENRICH _remote:languages ON language_code
                """)
                .coordinatorLogicalPlanOptimizationError(
                    containsString("4:3: ENRICH with remote policy can't be executed after [INLINE STATS count(*) BY language_code]@3:3")
                );
        }

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | STATS count(*) BY language_code
            | EVAL x = 1
            | MV_EXPAND language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("6:3: ENRICH with remote policy can't be executed after [STATS count(*) BY language_code]@3:3")
            );

        // Coordinator after remote is OK
        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | ENRICH _coordinator:languages ON language_code
            """).coordinatorLogicalOptimized();

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString(
                    "4:3: ENRICH with remote policy can't be executed after [ENRICH _coordinator:languages ON language_code]@3:3"
                )
            );

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages ON language_code
            | EVAL x = 1
            | MV_EXPAND language_name
            | DISSECT language_name "%{foo}"
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString(
                    "7:3: ENRICH with remote policy can't be executed after [ENRICH _coordinator:languages ON language_code]@3:3"
                )
            );

        testAnalyzer.plans("""
            FROM test
            | FORK (WHERE languages == 1) (WHERE languages == 2)
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString(
                    "4:3: ENRICH with remote policy can't be executed after [FORK (WHERE languages == 1) (WHERE languages == 2)]@2:3"
                )
            );

        testAnalyzer.plans("""
            FROM test
            | COMPLETION language_code = CONCAT("some prompt: ", first_name) WITH { "inference_id" : "completion-inference-id" }
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString(
                    "ENRICH with remote policy can't be executed after "
                        + "[COMPLETION language_code = CONCAT(\"some prompt: \", first_name) "
                        + "WITH { \"inference_id\" : \"completion-inference-id\" }]@2:3"
                )
            );

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | RERANK "test" ON first_name WITH { "inference_id" : "reranking-inference-id" }
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString(
                    "ENRICH with remote policy can't be executed after "
                        + "[RERANK \"test\" ON first_name WITH { \"inference_id\" : \"reranking-inference-id\" }]@3:3"
                )
            );

        testAnalyzer.plans("""
            FROM test
            | CHANGE_POINT salary ON languages
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: ENRICH with remote policy can't be executed after [CHANGE_POINT salary ON languages]@2:3")
            );
    }

    /**
     * The validation should not trigger for remote enrich after a lookup join. Lookup joins can be executed anywhere.
     */
    public void testRemoteEnrichAfterLookupJoin() {
        var testAnalyzer = analyzer().addDefaultIndex()
            .addLanguagesLookup()
            .addTestLookup()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");

        String lookupCommand = randomBoolean() ? "LOOKUP JOIN test_lookup ON languages" : "LOOKUP JOIN languages_lookup ON language_code";

        testAnalyzer.plans(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | %s
            """, lookupCommand)).coordinatorLogicalOptimized();

        testAnalyzer.plans(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | ENRICH _remote:languages ON language_code
            """, lookupCommand)).coordinatorLogicalOptimized();

        testAnalyzer.plans(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | ENRICH _remote:languages ON language_code
            | %s
            """, lookupCommand, lookupCommand)).coordinatorLogicalOptimized();

        testAnalyzer.plans(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | EVAL x = 1
            | MV_EXPAND language_code
            | ENRICH _remote:languages ON language_code
            """, lookupCommand)).coordinatorLogicalOptimized();
    }

    public void testRemoteLookupJoinWithPipelineBreaker() {
        var testAnalyzer = analyzer().addIndex("test,remote:test", "mapping-default.json")
            .addLanguagesLookup()
            .addTestLookup()
            .addAnalysisTestsEnrichResolution();
        testAnalyzer.plans(
            "FROM test,remote:test | STATS c = COUNT(*) by languages "
                + "| EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"
        )
            .coordinatorLogicalPlanOptimizationError(
                equalTo("1:92: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@1:25")
            );

        testAnalyzer.plans(
            "FROM test,remote:test | SORT emp_no | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"
        )
            .coordinatorLogicalPlanOptimizationError(
                equalTo("1:72: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@1:25")
            );
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 1:25: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                + "add another SORT after the LOOKUP JOIN if order is required"
        );

        testAnalyzer.stripErrorPrefix(true)
            .error(
                "FROM test,remote:test | LIMIT 2 | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
                equalTo("1:68: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@1:25")
            );

        testAnalyzer.plans(
            "FROM test,remote:test | EVAL language_code = languages | ENRICH _coordinator:languages_coord "
                + "| LOOKUP JOIN languages_lookup ON language_code"
        )
            .coordinatorLogicalPlanOptimizationError(
                equalTo("1:96: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@1:58")
            );

        testAnalyzer.plans(
            "FROM test,remote:test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code | LIMIT 2"
        ).coordinatorPhysicalPlanOptimized();
    }

    public void testRemoteEnrichAfterLookupJoinWithPipelineBreakerCCS() {
        var testAnalyzer = analyzer().addIndex("test,remote:test", "mapping-default.json")
            .addLanguagesLookup()
            .addTestLookup()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(
                Enrich.Mode.COORDINATOR,
                MATCH_TYPE,
                "languages_coord",
                "language_code",
                "languages_idx",
                "mapping-languages.json"
            );

        testAnalyzer.plans("""
            FROM test,remote:test
            | STATS c = COUNT(*) by languages
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@2:3")
            );

        testAnalyzer.plans("""
            FROM test,remote:test
            | SORT emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@2:3")
            );
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 2:3: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                + "add another SORT after the LOOKUP JOIN if order is required"
        );

        testAnalyzer.stripErrorPrefix(true).error("""
            FROM test,remote:test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

        testAnalyzer.plans("""
            FROM test,remote:test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages_coord
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@3:3")
            );
    }

    public void testRemoteEnrichAfterLookupJoinWithPipelineBreaker() {
        var testAnalyzer = analyzer().addDefaultIndex()
            .addLanguagesLookup()
            .addTestLookup()
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(
                Enrich.Mode.COORDINATOR,
                MATCH_TYPE,
                "languages_coord",
                "language_code",
                "languages_idx",
                "mapping-languages.json"
            );

        testAnalyzer.plans("""
            FROM test
            | STATS c = COUNT(*) by languages
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@2:3")
            );

        testAnalyzer.plans("""
            FROM test
            | SORT emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@2:3")
            );
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 2:3: SORT is followed by a LOOKUP JOIN which does not preserve order; "
                + "add another SORT after the LOOKUP JOIN if order is required"
        );

        testAnalyzer.stripErrorPrefix(true).error("""
            FROM test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

        testAnalyzer.plans("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages_coord
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """)
            .coordinatorLogicalPlanOptimizationError(
                containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@3:3")
            );
    }

    public void testDanglingOrderByMvExpand() {
        var testAnalyzer = analyzer().addDefaultIndex().addLanguagesLookup().addTestLookup().addAnalysisTestsEnrichResolution();

        testAnalyzer.plans("""
            FROM test
            | SORT languages
            | MV_EXPAND languages
            | WHERE languages == 1
            """).coordinatorLogicalPlanOptimizationError(is("""
            2:3: Unbounded SORT not supported yet [SORT languages] please add a LIMIT
            line 3:3: MV_EXPAND [MV_EXPAND languages] cannot yet have an unbounded SORT [SORT languages] before it: either move the SORT \
            after it, or add a LIMIT after the SORT"""));
    }

    public void testDanglingOrderByInInlineStats() {
        assumeTrue("INLINE STATS must be enabled", INLINE_STATS.isEnabled());
        var testAnalyzer = analyzer().addDefaultIndex().addLanguagesLookup().addTestLookup().addAnalysisTestsEnrichResolution();

        testAnalyzer.plans("""
            FROM test
            | SORT languages
            | MV_EXPAND languages
            | INLINE STATS count(*) BY languages
            | INLINE STATS s = sum(salary) BY first_name
            """).coordinatorLogicalPlanOptimizationError(is("""
            2:3: Unbounded SORT not supported yet [SORT languages] please add a LIMIT
            line 3:3: MV_EXPAND [MV_EXPAND languages] cannot yet have an unbounded SORT [SORT languages] before it: either move the \
            SORT after it, or add a LIMIT after the SORT
            line 4:3: INLINE STATS [INLINE STATS count(*) BY languages] cannot yet have an unbounded SORT [SORT languages] before it: \
            either move the SORT after it, or add a LIMIT after the SORT
            line 5:3: INLINE STATS [INLINE STATS s = sum(salary) BY first_name] cannot yet have an unbounded SORT [SORT languages] before \
            it: either move the SORT after it, or add a LIMIT after the SORT"""));
    }
}

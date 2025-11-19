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
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadEnrichPolicyResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OptimizerVerificationTests extends AbstractLogicalPlanOptimizerTests {

    private LogicalPlan plan(String query, Analyzer analyzer) {
        var analyzed = analyzer.analyze(parser.createStatement(query));
        return logicalOptimizer.optimize(analyzed);
    }

    private String error(String query, Analyzer analyzer, Object... params) {
        List<QueryParam> parameters = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                parameters.add(paramAsConstant(null, null));
            } else if (param instanceof String) {
                parameters.add(paramAsConstant(null, param));
            } else if (param instanceof Number) {
                parameters.add(paramAsConstant(null, param));
            } else {
                throw new IllegalArgumentException("VerifierTests don't support params of type " + param.getClass());
            }
        }
        Throwable e = expectThrows(
            VerificationException.class,
            "Expected error for query [" + query + "] but no error was raised",
            () -> plan(query, analyzer)
        );
        assertThat(e, instanceOf(VerificationException.class));

        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));

        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    public void testRemoteEnrichAfterCoordinatorOnlyPlans() {
        EnrichResolution enrichResolution = new EnrichResolution();
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.REMOTE,
            MATCH_TYPE,
            "languages",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.COORDINATOR,
            MATCH_TYPE,
            "languages",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        var analyzer = AnalyzerTestUtils.analyzer(
            loadMapping("mapping-default.json", "test"),
            defaultLookupResolution(),
            enrichResolution,
            TEST_VERIFIER
        );

        String err;

        // Remote enrich is ok after limit
        plan("""
            FROM test
            | LIMIT 10
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            """, analyzer);

        // Remote enrich is ok after topn
        plan("""
            FROM test
            | EVAL language_code = languages
            | SORT languages
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        plan("""
            FROM test
            | EVAL language_code = languages
            | SORT languages
            | LIMIT 2
            | ENRICH _remote:languages ON language_code
            """, analyzer);

        // Remote enrich is ok before pipeline breakers
        plan("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | LIMIT 10
            """, analyzer);

        plan("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            """, analyzer);

        plan("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | STATS count(*) BY language_name
            | LIMIT 10
            """, analyzer);

        plan("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | SORT language_name
            """, analyzer);

        err = error("""
            FROM test
            | EVAL language_code = languages
            | STATS count(*) BY language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(err, containsString("4:3: ENRICH with remote policy can't be executed after [STATS count(*) BY language_code]@3:3"));

        if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
            err = error("""
                FROM test
                | EVAL language_code = languages
                | INLINE STATS count(*) BY language_code
                | ENRICH _remote:languages ON language_code
                """, analyzer);
            assertThat(
                err,
                containsString("4:3: ENRICH with remote policy can't be executed after [INLINE STATS count(*) BY language_code]@3:3")
            );
        }

        err = error("""
            FROM test
            | EVAL language_code = languages
            | STATS count(*) BY language_code
            | EVAL x = 1
            | MV_EXPAND language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(err, containsString("6:3: ENRICH with remote policy can't be executed after [STATS count(*) BY language_code]@3:3"));

        // Coordinator after remote is OK
        plan("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | ENRICH _coordinator:languages ON language_code
            """, analyzer);

        err = error("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages ON language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString("4:3: ENRICH with remote policy can't be executed after [ENRICH _coordinator:languages ON language_code]@3:3")
        );

        err = error("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages ON language_code
            | EVAL x = 1
            | MV_EXPAND language_name
            | DISSECT language_name "%{foo}"
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString("7:3: ENRICH with remote policy can't be executed after [ENRICH _coordinator:languages ON language_code]@3:3")
        );

        err = error("""
            FROM test
            | FORK (WHERE languages == 1) (WHERE languages == 2)
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString(
                "4:3: ENRICH with remote policy can't be executed after [FORK (WHERE languages == 1) (WHERE languages == 2)]@2:3"
            )
        );

        err = error("""
            FROM test
            | COMPLETION language_code = CONCAT("some prompt: ", first_name) WITH { "inference_id" : "completion-inference-id" }
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString(
                "ENRICH with remote policy can't be executed after "
                    + "[COMPLETION language_code = CONCAT(\"some prompt: \", first_name) "
                    + "WITH { \"inference_id\" : \"completion-inference-id\" }]@2:3"
            )
        );

        err = error("""
            FROM test
            | RERANK language_code="test" ON languages WITH { "inference_id" : "reranking-inference-id" }
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString(
                "ENRICH with remote policy can't be executed after "
                    + "[RERANK language_code=\"test\" ON languages WITH { \"inference_id\" : \"reranking-inference-id\" }]@2:3"
            )
        );

        err = error("""
            FROM test
            | CHANGE_POINT salary ON languages
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(err, containsString("4:3: ENRICH with remote policy can't be executed after [CHANGE_POINT salary ON languages]@2:3"));
    }

    /**
     * The validation should not trigger for remote enrich after a lookup join. Lookup joins can be executed anywhere.
     */
    public void testRemoteEnrichAfterLookupJoin() {
        EnrichResolution enrichResolution = new EnrichResolution();
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.REMOTE,
            MATCH_TYPE,
            "languages",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        var analyzer = AnalyzerTestUtils.analyzer(
            loadMapping("mapping-default.json", "test"),
            defaultLookupResolution(),
            enrichResolution,
            TEST_VERIFIER
        );

        String lookupCommand = randomBoolean() ? "LOOKUP JOIN test_lookup ON languages" : "LOOKUP JOIN languages_lookup ON language_code";

        plan(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _remote:languages ON language_code
            | %s
            """, lookupCommand), analyzer);

        plan(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | ENRICH _remote:languages ON language_code
            """, lookupCommand), analyzer);

        plan(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | ENRICH _remote:languages ON language_code
            | %s
            """, lookupCommand, lookupCommand), analyzer);

        plan(Strings.format("""
            FROM test
            | EVAL language_code = languages
            | %s
            | EVAL x = 1
            | MV_EXPAND language_code
            | ENRICH _remote:languages ON language_code
            """, lookupCommand), analyzer);
    }

    public void testRemoteLookupJoinWithPipelineBreaker() {
        var analyzer = AnalyzerTestUtils.analyzer(loadMapping("mapping-default.json", "test,remote:test"));
        assertEquals(
            "1:92: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@1:25",
            error(
                "FROM test,remote:test | STATS c = COUNT(*) by languages "
                    + "| EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
                analyzer
            )
        );

        assertEquals(
            "1:72: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@1:25",
            error(
                "FROM test,remote:test | SORT emp_no | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
                analyzer
            )
        );

        assertEquals(
            "1:68: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@1:25",
            error(
                "FROM test,remote:test | LIMIT 2 | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
                analyzer
            )
        );

        assertEquals(
            "1:96: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@1:58",
            error(
                "FROM test,remote:test | EVAL language_code = languages | ENRICH _coordinator:languages_coord "
                    + "| LOOKUP JOIN languages_lookup ON language_code",
                analyzer
            )
        );

        plan("FROM test,remote:test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code | LIMIT 2", analyzer);

        // Since FORK, RERANK, COMPLETION and CHANGE_POINT are not supported on remote indices, we can't check them here against the remote
        // LOOKUP JOIN
    }

    public void testRemoteEnrichAfterLookupJoinWithPipelineBreaker() {
        EnrichResolution enrichResolution = new EnrichResolution();
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.REMOTE,
            MATCH_TYPE,
            "languages",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.COORDINATOR,
            MATCH_TYPE,
            "languages_coord",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        var analyzer = AnalyzerTestUtils.analyzer(
            loadMapping("mapping-default.json", "test"),
            defaultLookupResolution(),
            enrichResolution,
            TEST_VERIFIER
        );

        String err = error("""
            FROM test
            | STATS c = COUNT(*) by languages
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [STATS c = COUNT(*) by languages]@2:3")
        );

        err = error("""
            FROM test
            | SORT emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(err, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [SORT emp_no]@2:3"));

        err = error("""
            FROM test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(err, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

        err = error("""
            FROM test
            | EVAL language_code = languages
            | ENRICH _coordinator:languages_coord
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, analyzer);
        assertThat(
            err,
            containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:languages_coord]@3:3")
        );
    }

    public void testDanglingOrderByInInlineStats() {
        assumeTrue("INLINE STATS must be enabled", INLINE_STATS.isEnabled());
        var analyzer = AnalyzerTestUtils.analyzer(loadMapping("mapping-default.json", "test"));

        var err = error("""
            FROM test
            | SORT languages
            | INLINE STATS count(*) BY languages
            | INLINE STATS s = sum(salary) BY first_name
            """, analyzer);

        assertThat(err, is("""
            2:3: Unbounded SORT not supported yet [SORT languages] please add a LIMIT
            line 3:3: INLINE STATS [INLINE STATS count(*) BY languages] cannot yet have an unbounded SORT [SORT languages] before\
             it : either move the SORT after it, or add a LIMIT before the SORT
            line 4:3: INLINE STATS [INLINE STATS s = sum(salary) BY first_name] cannot yet have an unbounded SORT [SORT languages]\
             before it : either move the SORT after it, or add a LIMIT before the SORT"""));
    }
}

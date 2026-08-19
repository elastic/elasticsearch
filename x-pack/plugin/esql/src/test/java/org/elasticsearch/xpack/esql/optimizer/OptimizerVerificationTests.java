/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerExternalTests.S3_PATH;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerExternalTests.external;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.EMBEDDING_INFERENCE_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OptimizerVerificationTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * A cast to keyword (`::keyword`) produces a foldable string pattern. {@code 12::keyword} folds to the
     * literal {@code "12"}, which has no wildcards, so {@code ReplaceRegexMatch} rewrites it to an {@code Equals}.
     */
    public void testLikeCastToKeywordFoldsToEquals() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | where first_name like 12::keyword"));
        var filter = as(as(plan, Limit.class).child(), Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        assertEquals("12", BytesRefs.toString(as(equals.right(), Literal.class).value()));
    }

    /**
     * A pattern that folds to an invalid regex must raise a clear pattern error, not an internal failure.
     * {@code concat("(", ".*")} folds to {@code "(.*"} which is not a valid regex.
     */
    public void testRLikeInvalidRegexPatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = defaultAnalyzer().query("from test | where first_name rlike concat(\"(\", \".*\")");
        var e = expectThrows(org.elasticsearch.xpack.esql.parser.ParsingException.class, () -> optimize(plan));
        assertThat(e.getMessage(), containsString("Invalid regex pattern for RLIKE [(.*]"));
    }

    /**
     * A cast of a field to keyword ({@code last_name::keyword}) is not foldable, so it is rejected at
     * post-optimization verification the same way a bare field reference is.
     */
    public void testLikeNonFoldableCastReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name like last_name::keyword"));
        assertThat(err, containsString("[LIKE] pattern must be a constant"));
    }

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
        var testAnalyzer = analyzer().addDefaultIndex()
            .addLanguagesLookup()
            .addTestLookup()
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
        var testAnalyzer = analyzer().addDefaultIndex()
            .addLanguagesLookup()
            .addTestLookup()
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
            .addLanguagesLookup()
            .addTestLookup()
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

        testAnalyzer.stripErrorPrefix(true)
            .error(
                "FROM test,remote:test | LIMIT 2 | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
                equalTo("1:68: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@1:25")
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

        testAnalyzer.stripErrorPrefix(true).error("""
            FROM test,remote:test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

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

        testAnalyzer.stripErrorPrefix(true).error("""
            FROM test
            | LIMIT 2
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH _remote:languages ON language_code
            """, containsString("4:3: LOOKUP JOIN with remote indices can't be executed after [LIMIT 2]@2:3"));

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

    public void testDanglingOrderByMvExpand() {
        var testAnalyzer = analyzer().addDefaultIndex().addLanguagesLookup().addTestLookup().addAnalysisTestsEnrichResolution();

        var err = error(testAnalyzer.query("""
            FROM test
            | SORT languages
            | MV_EXPAND languages
            | WHERE languages == 1
            """));

        assertThat(err, is("""
            2:3: Unbounded SORT not supported yet [SORT languages] please add a LIMIT
            line 3:3: MV_EXPAND [MV_EXPAND languages] cannot yet have an unbounded SORT [SORT languages] before it: either move the SORT \
            after it, or add a LIMIT after the SORT"""));
    }

    public void testDanglingOrderByInInlineStats() {
        assumeTrue("INLINE STATS must be enabled", INLINE_STATS.isEnabled());
        var testAnalyzer = analyzer().addDefaultIndex().addLanguagesLookup().addTestLookup().addAnalysisTestsEnrichResolution();

        var err = error(testAnalyzer.query("""
            FROM test
            | SORT languages
            | MV_EXPAND languages
            | INLINE STATS count(*) BY languages
            | INLINE STATS s = sum(salary) BY first_name
            """));

        assertThat(err, is("""
            2:3: Unbounded SORT not supported yet [SORT languages] please add a LIMIT
            line 3:3: MV_EXPAND [MV_EXPAND languages] cannot yet have an unbounded SORT [SORT languages] before it: either move the \
            SORT after it, or add a LIMIT after the SORT
            line 4:3: INLINE STATS [INLINE STATS count(*) BY languages] cannot yet have an unbounded SORT [SORT languages] before it: \
            either move the SORT after it, or add a LIMIT after the SORT
            line 5:3: INLINE STATS [INLINE STATS s = sum(salary) BY first_name] cannot yet have an unbounded SORT [SORT languages] before \
            it: either move the SORT after it, or add a LIMIT after the SORT"""));
    }

    /**
     * Renaming the sort key between an unbounded SORT and an MV_EXPAND used to make ReorderLimitProjectAndOrderBy lift the OrderBy
     * above the renaming Project, leaving the OrderBy with a dangling reference to the dropped column and tripping the
     * "optimized incorrectly" optimizer verifier.
     * <p>
     * The proper "unbounded SORT" message should be reported instead.
     * <p>
     * Same root cause as the DROP variant in <a href="https://github.com/elastic/elasticsearch/issues/148612">#148612</a>.
     */
    public void testDanglingOrderByInInlineStatsWithRenamedSortKey() {
        assumeTrue("INLINE STATS must be enabled", INLINE_STATS.isEnabled());
        var testAnalyzer = analyzer().addDefaultIndex().addLanguagesLookup().addTestLookup().addAnalysisTestsEnrichResolution();

        var err = error(testAnalyzer.query("""
            ROW a = 1
            | SORT a DESC
            | RENAME a AS b
            | MV_EXPAND b
            | INLINE STATS c = count(*)
            """));

        assertThat(err, is("""
            2:3: Unbounded SORT not supported yet [SORT a DESC] please add a LIMIT
            line 4:3: MV_EXPAND [MV_EXPAND b] cannot yet have an unbounded SORT [SORT a DESC] before it: either move the SORT after \
            it, or add a LIMIT after the SORT
            line 5:3: INLINE STATS [INLINE STATS c = count(*)] cannot yet have an unbounded SORT [SORT a DESC] before it: either move \
            the SORT after it, or add a LIMIT after the SORT"""));
    }

    /**
     * Dropping the sort key with a DROP between an unbounded SORT and an MV_EXPAND used to make ReorderLimitProjectAndOrderBy lift
     * the OrderBy above the dropping Project, tripping the "optimized incorrectly" optimizer verifier.
     * See <a href="https://github.com/elastic/elasticsearch/issues/148612">#148612</a>.
     */
    public void testDanglingOrderByInInlineStatsWithDroppedSortKey() {
        assumeTrue("INLINE STATS must be enabled", INLINE_STATS.isEnabled());
        var testAnalyzer = analyzer().addDefaultIndex().addLanguagesLookup().addTestLookup().addAnalysisTestsEnrichResolution();

        var err = error(testAnalyzer.query("""
            ROW x = "a b"
            | DISSECT x "%{y}"
            | SORT y
            | DROP y
            | MV_EXPAND x
            | INLINE STATS c = count(*)
            """));

        assertThat(err, is("""
            3:3: Unbounded SORT not supported yet [SORT y] please add a LIMIT
            line 5:3: MV_EXPAND [MV_EXPAND x] cannot yet have an unbounded SORT [SORT y] before it: either move the SORT after it, \
            or add a LIMIT after the SORT
            line 6:3: INLINE STATS [INLINE STATS c = count(*)] cannot yet have an unbounded SORT [SORT y] before it: either move \
            the SORT after it, or add a LIMIT after the SORT"""));
    }

    public void testEnrichRemoteRejected() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        var testAnalyzer = external().addEnrichPolicy(
            Enrich.Mode.REMOTE,
            EnrichPolicy.MATCH_TYPE,
            "languages_policy",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        var err = error(testAnalyzer.query("EXTERNAL \"" + S3_PATH + "\"" + """
            | EVAL x = TO_STRING(languages)
            | ENRICH _remote:languages_policy ON x
            """));
        assertThat(err, containsString("ENRICH with remote policy can't be executed after [EXTERNAL"));
        assertThat(
            err,
            containsString("federated data sources execute entirely on the coordinating node and are incompatible with remote ENRICH")
        );
    }

    public void testEmbeddingLiteralValues() {
        assumeTrue("Embedding function must be enabled", EsqlCapabilities.Cap.EMBEDDING_FUNCTION.isEnabled());

        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json").addAnalysisTestsInferenceResolution();

        var err = error(testAnalyzer.query("""
            from test
            | EVAL embedding = EMBEDDING(first_name, "embedding-inference-id")
            """));
        assertThat(err, is("2:20: first argument for [EMBEDDING(first_name, \"embedding-inference-id\")] must be a constant string"));

        err = error(testAnalyzer.query("""
            from test
            | EVAL embedding = EMBEDDING("my text", first_name)
            """));
        assertThat(err, is("2:20: second argument for [EMBEDDING(\"my text\", first_name)] must be a constant string"));
    }

    public void testEmbeddingFunctionInvalidQuery() {
        assumeTrue("Embedding function must be enabled", EsqlCapabilities.Cap.EMBEDDING_FUNCTION.isEnabled());

        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json").addAnalysisTestsInferenceResolution();

        var err = error(testAnalyzer.query("from test | EVAL embedding = EMBEDDING(last_name, ?)", EMBEDDING_INFERENCE_ID));
        assertThat(err, is("1:30: first argument for [EMBEDDING(last_name, ?)] must be a constant string"));
    }

    public void testEmbeddingFunctionInvalidInferenceId() {
        assumeTrue("Embedding function must be enabled", EsqlCapabilities.Cap.EMBEDDING_FUNCTION.isEnabled());

        var testAnalyzer = analyzer().addIndex("test", "mapping-default.json").addAnalysisTestsInferenceResolution();

        var err = error(testAnalyzer.query("from test | EVAL embedding = EMBEDDING(\"query\", last_name)", EMBEDDING_INFERENCE_ID));
        assertThat(err, is("1:30: second argument for [EMBEDDING(\"query\", last_name)] must be a constant string"));
    }

    // Regression for #142026.
    public void testLoadModeUnmappedJoinKeyDoesNotCrashOptimizer() {
        assumeTrue(
            "requires optional_fields_load_with_lookup_join",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_WITH_LOOKUP_JOIN.isEnabled()
        );
        var testAnalyzer = analyzer().addDefaultIndex().addSampleDataLookup();
        var analyzed = testAnalyzer.statement("""
            SET unmapped_fields="load";
            FROM test
            | KEEP message
            | LOOKUP JOIN sample_data_lookup ON message
            """);
        var optimized = optimize(analyzed);
        var message = singleValue(optimized.output().stream().filter(a -> "message".equals(a.name())).toList());
        assertThat("unmapped join key loaded from _source is keyword", message.dataType(), equalTo(DataType.KEYWORD));
    }

    public void testPruneEvalColumnsInForkWithStats() {
        var testAnalyzer = analyzer().addDefaultIndex();

        var err = error(testAnalyzer.query("FROM test | EVAL x = 1 | FORK (SORT x) | STATS y = COUNT(*)"));

        assertThat(err, is("1:32: Unbounded SORT not supported yet [SORT x] please add a LIMIT"));
    }

    public void testPruneEvalColumnsInForkWithStatsAndExpressionSorts() {
        var testAnalyzer = analyzer().addDefaultIndex();

        var err = error(testAnalyzer.query("FROM test | FORK (SORT emp_no + 1) (SORT emp_no - 1) | STATS y = COUNT(*)"));

        assertThat(err, is("""
            1:19: Unbounded SORT not supported yet [SORT emp_no + 1] please add a LIMIT
            line 1:37: Unbounded SORT not supported yet [SORT emp_no - 1] please add a LIMIT"""));
    }

    public void testPruneEvalColumnsInForkWithStatsAndSingleExpressionSort() {
        var testAnalyzer = analyzer().addDefaultIndex();

        var err = error(testAnalyzer.query("FROM test | FORK (SORT emp_no + 1) | STATS y = COUNT(*)"));

        assertThat(err, is("1:19: Unbounded SORT not supported yet [SORT emp_no + 1] please add a LIMIT"));
    }

    // LIKE/RLIKE constant-expression tests: pattern folding happens in the optimizer, not the analyzer.

    /**
     * LIKE with a foldable CONCAT expression is folded by ConstantFolding in the optimizer, then
     * routed through {@code ReplaceRegexMatch}. As for an inline {@code LIKE "Anna*"}, the prefix
     * pattern decomposes into a {@link StartsWith}, keeping both paths consistent.
     */
    public void testLikeConstantExpressionFoldsToStartsWith() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | where first_name like concat(\"Anna\", \"*\")"));
        var filter = as(as(plan, Limit.class).child(), Filter.class);
        StartsWith startsWith = as(filter.condition(), StartsWith.class);
        assertEquals("Anna", BytesRefs.toString(as(startsWith.prefix(), Literal.class).value()));
    }

    /**
     * RLIKE with a foldable CONCAT expression is folded by ConstantFolding in the optimizer into a concrete RLike.
     */
    public void testRLikeConstantExpressionFoldsToRLike() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | where first_name rlike concat(\"Anna\", \".*\")"));
        var filter = as(as(plan, Limit.class).child(), Filter.class);
        RLike rlike = as(filter.condition(), RLike.class);
        assertEquals("Anna.*", rlike.pattern().asJavaRegex());
    }

    /**
     * EVAL propagation: PropagateEvalFoldables folds the literal "Anna*" from the EVAL back into
     * the LIKE pattern before the regex-resolution rule fires. This is the key case that the
     * analyzer-only approach could not handle: a pattern arriving via an EVAL alias should work
     * identically to an inline literal.
     * <p>
     * The Eval stays above the Limit in the optimized plan because x is part of the output
     * (no KEEP to exclude it), and the Filter is pushed below Limit.
     */
    public void testLikeEvalPropagatedConstant() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval x = \"Anna*\" | where first_name like x"));
        // Eval → Limit → Filter → EsRelation (Eval stays because x is in the output)
        var filter = as(as(as(plan, Eval.class).child(), Limit.class).child(), Filter.class);
        StartsWith startsWith = as(filter.condition(), StartsWith.class);
        assertEquals("Anna", BytesRefs.toString(as(startsWith.prefix(), Literal.class).value()));
    }

    /**
     * Same as {@link #testLikeEvalPropagatedConstant} for RLIKE.
     */
    public void testRLikeEvalPropagatedConstant() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval x = \"Anna.*\" | where first_name rlike x"));
        var filter = as(as(as(plan, Eval.class).child(), Limit.class).child(), Filter.class);
        RLike rlike = as(filter.condition(), RLike.class);
        assertEquals("Anna.*", rlike.pattern().asJavaRegex());
    }

    /**
     * EVAL with a CONCAT that folds: the optimizer first evaluates CONCAT, then propagates and
     * converts the DeferredRegexExpression, decomposing the prefix pattern into a {@link StartsWith}.
     */
    public void testLikeEvalPropagatedConcat() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval x = concat(\"Anna\", \"*\") | where first_name like x"));
        var filter = as(as(as(plan, Eval.class).child(), Limit.class).child(), Filter.class);
        StartsWith startsWith = as(filter.condition(), StartsWith.class);
        assertEquals("Anna", BytesRefs.toString(as(startsWith.prefix(), Literal.class).value()));
    }

    /**
     * Same as {@link #testLikeEvalPropagatedConcat} for RLIKE.
     */
    public void testRLikeEvalPropagatedConcat() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval x = concat(\"Anna\", \".*\") | where first_name rlike x"));
        var filter = as(as(as(plan, Eval.class).child(), Limit.class).child(), Filter.class);
        RLike rlike = as(filter.condition(), RLike.class);
        assertEquals("Anna.*", rlike.pattern().asJavaRegex());
    }

    /**
     * Multi-level EVAL chain: PropagateEvalFoldables must propagate through two EVAL steps.
     * First {@code suffix = ".*"} is substituted into {@code p = CONCAT("Eber", suffix)}, making
     * p a foldable CONCAT, which is then substituted into the RLIKE pattern and resolved.
     * CombineEvals merges both EVAL nodes into one, so the plan root is a single merged Eval.
     */
    public void testRLikeMultiLevelEvalChain() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(
            defaultAnalyzer().query("from test | eval suffix = \".*\" | eval p = concat(\"Eber\", suffix) | where first_name rlike p")
        );
        // CombineEvals merges both EVAL nodes into one; plan shape mirrors the single-EVAL case
        var eval = as(plan, Eval.class);
        var filter = as(as(eval.child(), Limit.class).child(), Filter.class);
        RLike rlike = as(filter.condition(), RLike.class);
        assertEquals("Eber.*", rlike.pattern().asJavaRegex());
    }

    /**
     * Same as {@link #testRLikeMultiLevelEvalChain} for LIKE.
     */
    public void testLikeMultiLevelEvalChain() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(
            defaultAnalyzer().query("from test | eval suffix = \"*\" | eval p = concat(\"Eber\", suffix) | where first_name like p")
        );
        var eval = as(plan, Eval.class);
        var filter = as(as(eval.child(), Limit.class).child(), Filter.class);
        StartsWith startsWith = as(filter.condition(), StartsWith.class);
        assertEquals("Eber", BytesRefs.toString(as(startsWith.prefix(), Literal.class).value()));
    }

    /**
     * EVAL with a matchesAll pattern ("*") should produce IsNotNull, same as the inline case.
     * ReplaceDeferredRegex handles this directly after propagation.
     */
    public void testLikeEvalPropagatedMatchesAll() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval p = \"*\" | where first_name like p"));
        var outerEval = as(plan, Eval.class);
        var filter = as(as(outerEval.child(), Limit.class).child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
    }

    /**
     * EVAL with an exactMatch pattern (no wildcards) should produce Equals, same as the inline case.
     */
    public void testLikeEvalPropagatedExactMatch() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval p = \"Eber\" | where first_name like p"));
        var outerEval = as(plan, Eval.class);
        var filter = as(as(outerEval.child(), Limit.class).child(), Filter.class);
        as(filter.condition(), Equals.class);
    }

    /**
     * Non-foldable pattern (a field reference) must be rejected at post-optimization verification
     * with an error indicating the argument must be a constant.
     */
    public void testLikeNonFoldablePatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name like last_name"));
        assertThat(err, containsString("[LIKE] pattern must be a constant, received [last_name]"));
    }

    /**
     * Non-foldable pattern (a field reference) must be rejected at post-optimization verification for RLIKE.
     */
    public void testRLikeNonFoldablePatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name rlike last_name"));
        assertThat(err, containsString("[RLIKE] pattern must be a constant, received [last_name]"));
    }

    /**
     * A pattern that folds to a non-string type (integer) must be rejected at post-optimization
     * verification with a type error.
     */
    public void testLikeIntegerPatternReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name like 12"));
        assertThat(err, containsString("[LIKE] pattern must be a string"));
    }

    /**
     * A foldable arithmetic expression that yields a non-string constant (1 + 2 = 3) is rejected
     * the same way as a bare integer literal.
     */
    public void testLikeFoldedIntegerPatternReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name like (1 + 2)"));
        assertThat(err, containsString("[LIKE] pattern must be a string"));
    }

    /**
     * Same as {@link #testLikeIntegerPatternReportsTypeError} for RLIKE.
     */
    public void testRLikeIntegerPatternReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name rlike 12"));
        assertThat(err, containsString("[RLIKE] pattern must be a string"));
    }

    /**
     * A null-valued keyword pattern (e.g. via {@code null::keyword}) must produce a clear user error,
     * not an internal crash. The pattern passes type and foldability checks but folds to null,
     * so the null guard in {@code postOptimizationVerification} must catch it.
     */
    public void testLikeNullPatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | eval p = null::keyword | where first_name like p"));
        assertThat(err, containsString("[LIKE] pattern must not be null"));
    }

    /**
     * Same as {@link #testLikeNullPatternReportsError} for RLIKE.
     */
    public void testRLikeNullPatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | eval p = null::keyword | where first_name rlike p"));
        assertThat(err, containsString("[RLIKE] pattern must not be null"));
    }

    /**
     * A bare {@code null} literal (DataType.NULL) must be rejected because it is not a string type,
     * not because it is null. This is the simplest null path: {@code WHERE field LIKE null}.
     */
    public void testLikeNullLiteralReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name like null"));
        assertThat(err, containsString("[LIKE] pattern must be a string"));
    }

    /**
     * Same as {@link #testLikeNullLiteralReportsTypeError} for RLIKE.
     */
    public void testRLikeNullLiteralReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name rlike null"));
        assertThat(err, containsString("[RLIKE] pattern must be a string"));
    }

    /**
     * {@code CONCAT(null, "*")} folds to a null KEYWORD value; the null guard in
     * {@code postOptimizationVerification} must catch it and report a clear error.
     */
    public void testLikeConcatNullPropagatesError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name like concat(null, \"*\")"));
        assertThat(err, containsString("[LIKE] pattern must not be null"));
    }

    /**
     * Same as {@link #testLikeConcatNullPropagatesError} for RLIKE.
     */
    public void testRLikeConcatNullPropagatesError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name rlike concat(null, \".*\")"));
        assertThat(err, containsString("[RLIKE] pattern must not be null"));
    }

    /**
     * An untyped {@code null} via EVAL (DataType.NULL) must be rejected because it is not a string,
     * not because it is null. Symmetric to {@link #testLikeNullLiteralReportsTypeError} but via
     * the EVAL propagation path.
     */
    public void testLikeNullEvalReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | eval p = null | where first_name like p"));
        assertThat(err, containsString("[LIKE] pattern must be a string"));
    }

    /**
     * Same as {@link #testLikeNullEvalReportsTypeError} for RLIKE.
     */
    public void testRLikeNullEvalReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | eval p = null | where first_name rlike p"));
        assertThat(err, containsString("[RLIKE] pattern must be a string"));
    }

    /**
     * RLIKE pattern ".*" via EVAL matches every non-null string; ReplaceDeferredRegex
     * detects {@code matchesAll()} and produces {@link IsNotNull} instead of {@link RLike}.
     * Symmetric to {@link #testLikeEvalPropagatedMatchesAll}.
     */
    public void testRLikeEvalPropagatedMatchesAll() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval p = \".*\" | where first_name rlike p"));
        var outerEval = as(plan, Eval.class);
        var filter = as(as(outerEval.child(), Limit.class).child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
    }

    /**
     * RLIKE pattern with no regex metacharacters ("Anna") via EVAL has only one accepted
     * string; ReplaceDeferredRegex detects {@code exactMatch()} and produces {@link Equals}.
     * Symmetric to {@link #testLikeEvalPropagatedExactMatch}.
     */
    public void testRLikeEvalPropagatedExactMatch() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | eval p = \"Anna\" | where first_name rlike p"));
        var outerEval = as(plan, Eval.class);
        var filter = as(as(outerEval.child(), Limit.class).child(), Filter.class);
        as(filter.condition(), Equals.class);
    }

    /**
     * A foldable-but-non-string arithmetic expression used as an RLIKE pattern must be
     * rejected at post-optimization verification. Symmetric to
     * {@link #testLikeFoldedIntegerPatternReportsTypeError}.
     */
    public void testRLikeFoldedIntegerPatternReportsTypeError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name rlike (1 + 2)"));
        assertThat(err, containsString("[RLIKE] pattern must be a string"));
    }

    /**
     * NOT LIKE with a constant expression: the parser wraps the DeferredRegexExpression in
     * Not; ReplaceDeferredRegex descends into it and resolves the inner node normally.
     */
    public void testLikeNotConstantExpression() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | where first_name not like concat(\"Anna\", \"*\")"));
        var filter = as(as(plan, Limit.class).child(), Filter.class);
        Not not = as(filter.condition(), Not.class);
        StartsWith startsWith = as(not.field(), StartsWith.class);
        assertEquals("Anna", BytesRefs.toString(as(startsWith.prefix(), Literal.class).value()));
    }

    /**
     * Same as {@link #testLikeNotConstantExpression} for RLIKE.
     */
    public void testRLikeNotConstantExpression() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | where first_name not rlike concat(\"Anna\", \".*\")"));
        var filter = as(as(plan, Limit.class).child(), Filter.class);
        Not not = as(filter.condition(), Not.class);
        RLike rlike = as(not.field(), RLike.class);
        assertEquals("Anna.*", rlike.pattern().asJavaRegex());
    }

    /**
     * NOT LIKE with a non-foldable pattern (a field reference) must be rejected at
     * post-optimization verification. The {@code Not} wrapper must not prevent the
     * {@code LogicalVerifier} from descending into the inner {@code DeferredRegexExpression}.
     */
    public void testNotLikeNonFoldablePatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name not like last_name"));
        assertThat(err, containsString("[LIKE] pattern must be a constant, received [last_name]"));
    }

    /**
     * Same as {@link #testNotLikeNonFoldablePatternReportsError} for RLIKE.
     */
    public void testNotRLikeNonFoldablePatternReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var err = error(defaultAnalyzer().query("from test | where first_name not rlike last_name"));
        assertThat(err, containsString("[RLIKE] pattern must be a constant, received [last_name]"));
    }

    /**
     * A plain text field (no keyword sub-field, i.e. {@code hasExact()} is false) must be
     * accepted with a constant-expression pattern. {@code isStringAndExact} would have rejected
     * it, producing a VerificationException, while the literal-pattern path via
     * {@code WildcardLike} would accept it via {@code isString}. This test locks in the
     * correct behaviour after the fix to use {@code isString} in
     * {@code DeferredRegexExpression.resolveType()}.
     * {@code gender} in mapping-basic.json is a pure text field with no keyword sub-field.
     */
    public void testLikeConstantExpressionOnTextField() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("from test | where gender like concat(\"M\", \"*\")"));
        var filter = as(as(plan, Limit.class).child(), Filter.class);
        StartsWith startsWith = as(filter.condition(), StartsWith.class);
        assertEquals("M", BytesRefs.toString(as(startsWith.prefix(), Literal.class).value()));
    }

    /**
     * A LIKE pattern that folds to an invalid wildcard escape sequence (e.g. {@code \a}) must raise
     * a clear {@link org.elasticsearch.xpack.esql.parser.ParsingException} rather than leaking an
     * {@code InvalidArgumentException} from the
     * {@link org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern} constructor.
     */
    public void testLikeInvalidWildcardEscapeReportsError() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        // concat("pre", "\\a") folds to "pre\a"; the \a escape is invalid in wildcard syntax
        var plan = defaultAnalyzer().query("from test | where first_name like concat(\"pre\", \"\\\\a\")");
        var e = expectThrows(org.elasticsearch.xpack.esql.parser.ParsingException.class, () -> optimize(plan));
        assertThat(e.getMessage(), containsString("Invalid pattern for LIKE"));
    }

    public void testLikeAlwaysTrue_AsLocalRelation() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(defaultAnalyzer().query("row abc = \"demo\" | eval filter = concat(\"demo\", \"*\") | where abc like filter"));
        // The filter folds to true and is pruned; the Row source becomes a LocalRelation
        assertFalse(plan.anyMatch(p -> p instanceof Filter));
        assertTrue(plan.anyMatch(p -> p instanceof LocalRelation));
    }

    /**
     * Same as {@link #testLikeAlwaysTrue_AsLocalRelation} for RLIKE.
     */
    public void testRLikeAlwaysTrue_AsLocalRelation() {
        assumeTrue("requires like_rlike_constant_expression", EsqlCapabilities.Cap.LIKE_RLIKE_CONSTANT_EXPRESSION.isEnabled());
        var plan = optimize(
            defaultAnalyzer().query("row abc = \"demo\" | eval filter = concat(\"demo\", \".*\") | where abc rlike filter")
        );
        assertFalse(plan.anyMatch(p -> p instanceof Filter));
        assertTrue(plan.anyMatch(p -> p instanceof LocalRelation));
    }

    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/155979 where ip
     * ended up as unresolved in the logical plan optimizations
     */
    public void testOrCidrMatchNotPruned() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 == "127.0.0.1"::ip, TO_STRING(ip0), null)
            """));
    }

    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/155979 where ip
     * disappeared from the plan in the logical optimizations
     */
    public void testOrCidrMatchNotPruned2() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 == "127.0.0.1"::ip, TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """));
    }

    public void testOrCidrMatchWithInNotPruned() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 IN ("127.0.0.1"::ip, "192.168.1.1"::ip), TO_STRING(ip0), null)
            """));
    }

    public void testOrCidrMatchWithInNotPruned2() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 IN ("127.0.0.1"::ip, "192.168.1.1"::ip), TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """));
    }

    public void testOrCidrMatchWithMixedTypeInNotPruned() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 IN ("127.0.0.1"::ip, "192.168.1.1"), TO_STRING(ip0), null)
            """));
    }

    public void testIpInWithoutCidrMatchNotPruned() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(ip0 IN ("127.0.0.1"::ip, "192.168.1.1"::ip), TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """));
    }

    public void testIpEqualityAndInCombinedWithCidrMatchNotPruned() {
        var testAnalyzer = analyzer().addIndex("hosts", "mapping-hosts.json");
        optimize(testAnalyzer.query("""
            FROM hosts
            | EVAL ip = CASE(
                CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 == "127.0.0.1"::ip OR ip0 IN ("192.168.1.1"::ip, "172.16.0.1"::ip),
                TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """));
    }
}

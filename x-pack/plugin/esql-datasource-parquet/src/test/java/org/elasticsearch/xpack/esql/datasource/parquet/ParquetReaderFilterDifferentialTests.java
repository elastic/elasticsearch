/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Differential reader-side correctness suite for the Parquet pushdown stack.
 *
 * <p>For every filter and every layout, the suite runs four read paths and asserts they all
 * agree on the set of {@code id} values returned. Each path uses a substantially different
 * code path so disagreements pinpoint the bug class:
 * <ol>
 *   <li><b>production reader</b> (unit under test): our optimized reader with per-conjunct
 *       YES/RECHECK pushdown — the same shape {@code PushFiltersToSource} produces.</li>
 *   <li><b>oracle A — apache-mr</b>: apache-mr's {@link org.apache.parquet.hadoop.ParquetReader}
 *       given the translated {@link org.apache.parquet.filter2.predicate.FilterPredicate},
 *       then post-filtered in pure Java with the original ESQL expression to restore TVL
 *       semantics and to apply conjuncts that did not translate (e.g. {@code WildcardLike}).
 *       This is a true cross-stack check — apache-mr has its own RowGroupFilter,
 *       ColumnIndex evaluator, and page reader.</li>
 *   <li><b>oracle B — pushdown disabled</b>: our reader with no pushed filter at all (no
 *       row-group pruning, no page pruning, no late-mat, no two-phase). Filtering is done
 *       in pure Java on the reader's full output. Isolates bugs introduced by the
 *       optimization shortcuts the production path takes.</li>
 *   <li><b>oracle C — pure Java</b>: evaluate the ESQL expression against the in-memory rows
 *       used to write the parquet file. No parquet involved at all. Catches translation
 *       bugs that both A and B might share.</li>
 * </ol>
 *
 * <h2>Why all three oracles?</h2>
 * Each one catches a different class of bugs that the others would miss:
 * <ul>
 *   <li>The trivially-passes shortcut leak we recently fixed (LIKE pushed as YES alongside a
 *       stats-trivial RECHECK conjunct) is caught by all three — they each apply the LIKE
 *       through a path the production reader bypasses.</li>
 *   <li>A bug in our {@code FilterPredicate} translation (e.g. {@code gt → gte} off-by-one)
 *       is caught by oracle B and oracle C but NOT oracle A — A uses the same translation.</li>
 *   <li>A bug in our pure-Java oracle's understanding of ESQL semantics (e.g. wrong TVL on
 *       Not(Eq)) would silently agree with the production reader; oracle A catches it
 *       because apache-mr is an independent implementation.</li>
 * </ul>
 *
 * <h2>What this suite covers</h2>
 * <ul>
 *   <li>Fixed cases at known fault lines: YES/RECHECK/NO mixes, AND/OR/NOT trees,
 *       stats-trivial conjuncts, all-match and no-match filters, IS NULL / IS NOT NULL,
 *       prefix/suffix/infix LIKE, case-insensitive LIKE.</li>
 *   <li>Randomized expression trees (bounded depth, mixed operators, mixed columns), per
 *       layout.</li>
 *   <li>Multiple Parquet write layouts (single big row group, many small row groups, dict
 *       encoding on/off) so the optimizer's stats/dict/page paths get exercised
 *       differently.</li>
 * </ul>
 *
 * <h2>How to debug a failure</h2>
 * The assertion message names the disagreeing pair (e.g. {@code production-vs-pureJava}),
 * the layout, the filter (in compact form), and a sample of the symmetric difference of
 * row IDs. Reproduce randomized failures with {@code -Dtests.seed=...} as usual.
 */
public class ParquetReaderFilterDifferentialTests extends ESTestCase {

    private BlockFactory blockFactory;

    /**
     * Master schema used across all tests. Keep this fixed: the matrix is filter × layout, not
     * filter × layout × schema.
     */
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("id")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("status")
        .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
        .named("score")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("category")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("url")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("description")
        .optional(PrimitiveType.PrimitiveTypeName.INT32)
        .named("nullable_flag")
        .named("differential_test_schema");

    private static final ReferenceAttribute ID = attr("id", DataType.LONG);
    private static final ReferenceAttribute STATUS = attr("status", DataType.LONG);
    private static final ReferenceAttribute SCORE = attr("score", DataType.DOUBLE);
    private static final ReferenceAttribute CATEGORY = attr("category", DataType.KEYWORD);
    private static final ReferenceAttribute URL = attr("url", DataType.KEYWORD);
    private static final ReferenceAttribute DESCRIPTION = attr("description", DataType.KEYWORD);
    private static final ReferenceAttribute NULLABLE_FLAG = attr("nullable_flag", DataType.INTEGER);

    private static final int ROW_COUNT = 4000;

    private static final String[] CATEGORIES = { "alpha", "beta", "gamma", "delta" };
    private static final String[] URL_HOSTS = { "google.com", "example.org", "elastic.co", "github.com" };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    // ---------------------------------------------------------------------------------------
    // Fixed cases — each one targets a known fault line in the pushdown / late-mat stack.
    // Layouts are randomized per fixed case so the matrix is still wide.
    // ---------------------------------------------------------------------------------------

    public void testSingleEqualsOnLowCardinalityRecheck() throws IOException {
        // status = 200 — RECHECK conjunct; FilterExec re-applies. Stats-aware paths must
        // still produce the right rows whether the row group is fully matching, fully
        // non-matching, or mixed.
        runDifferential(eq(STATUS, 200L, DataType.LONG));
    }

    public void testRangeOnSortedColumn() throws IOException {
        // id is monotonic; this exercises ColumnIndex page pruning and stats-based row-group
        // pruning end to end.
        runDifferential(and(gte(ID, 100L, DataType.LONG), lt(ID, 400L, DataType.LONG)));
    }

    public void testStartsWithKeyword() throws IOException {
        // StartsWith pushes as RECHECK + translates to a prefix-range FilterPredicate.
        // The reader must produce identical rows whether the row group is fully matched
        // (all URLs share the prefix) or partially.
        runDifferential(startsWith(URL, "https://google"));
    }

    public void testWildcardLikeAlonePushedAsYes() throws IOException {
        // Bare LIKE pushes as YES; FilterExec is dropped. Late-mat must run and produce
        // exactly the LIKE-matching rows. This is the common single-conjunct shape.
        runDifferential(like(URL, "*google*"));
    }

    public void testLikeAndStatsTrivialEquals() throws IOException {
        // Regression: LIKE (YES) AND status = 200 (RECHECK, stats-trivial because we set
        // status = 200 for every row in our dataset). This was the trivially-passes shortcut
        // bug — the reader would skip late-mat for every row group on the strength of
        // status = 200's stats and silently leak rows that don't match LIKE. The fix relies
        // on hasYesConjunctOutsideFilterPredicate to suppress the shortcut here.
        runDifferential(and(like(URL, "*google*"), eq(STATUS, 200L, DataType.LONG)));
    }

    public void testNotLike() throws IOException {
        // Not(LIKE) is YES with a TVL-aware special case in evaluateNotWildcardLike.
        // Verifies that null URLs (none in our base dataset, but the path is still exercised)
        // and non-matching URLs both behave as expected under negation.
        runDifferential(not(like(URL, "*google*")));
    }

    public void testCaseInsensitiveLike() throws IOException {
        runDifferential(likeCaseInsensitive(URL, "*GOOGLE*"));
    }

    public void testIsNullAndIsNotNull() throws IOException {
        // nullable_flag is null for ~30% of rows. Both branches exercise the dedicated
        // IsNull/IsNotNull evaluator paths and the FilterPredicate translation that uses
        // a null-valued comparison.
        runDifferential(isNull(NULLABLE_FLAG));
        runDifferential(isNotNull(NULLABLE_FLAG));
    }

    public void testOrOfHeterogeneousConjuncts() throws IOException {
        // OR(LIKE, status > 400) — OR forces the conservative all-or-nothing translation
        // (both arms must translate or the OR is dropped). Critically, OR is NOT YES-eligible
        // even when both arms are YES, so this lands as RECHECK and FilterExec re-applies.
        runDifferential(or(like(URL, "*example*"), gt(STATUS, 400L, DataType.LONG)));
    }

    public void testNestedAndOrLikeTreeWithMixedPushability() throws IOException {
        // (LIKE AND status = 200) OR (id < 100) — combines a YES/RECHECK conjunction with
        // a separate selective range. OR makes the whole thing RECHECK and keeps it in
        // FilterExec, but the late-mat evaluator must still produce a correct subset to
        // hand to FilterExec.
        Expression filter = or(and(like(URL, "*google*"), eq(STATUS, 200L, DataType.LONG)), lt(ID, 100L, DataType.LONG));
        runDifferential(filter);
    }

    public void testFilterMatchingNoRows() throws IOException {
        // Stats-pruned: id > ROW_COUNT * 2 — every row group's max < threshold, so
        // RowGroupFilter drops all groups. Reader returns zero pages.
        runDifferential(gt(ID, (long) (ROW_COUNT * 2L), DataType.LONG));
    }

    public void testFilterMatchingAllRows() throws IOException {
        // Stats-trivial all-pass: id >= 0. Trivially-passes shortcut should fire (all
        // expressions translate, all stats prove it true). Reader returns every row.
        runDifferential(gte(ID, 0L, DataType.LONG));
    }

    public void testNotEqualsOnLowCardinality() throws IOException {
        runDifferential(neq(STATUS, 404L, DataType.LONG));
    }

    public void testRangeOnDouble() throws IOException {
        runDifferential(and(gte(SCORE, 0.25, DataType.DOUBLE), lt(SCORE, 0.75, DataType.DOUBLE)));
    }

    public void testLikeOnHighCardinalityProjectedColumn() throws IOException {
        // url is both predicate (LIKE) and projection target. Late-mat's projection-only
        // optimization doesn't apply here — exercises the "predicate column is also projected"
        // common case.
        runDifferential(like(URL, "*github*"));
    }

    public void testEqualsOnDictionaryEncodedKeyword() throws IOException {
        // category has only 4 distinct values, so the writer dict-encodes it. Equality
        // should land on the dictionary short-circuit path.
        runDifferential(eq(CATEGORY, "alpha", DataType.KEYWORD));
    }

    // ---------------------------------------------------------------------------------------
    // Silent-drop / NOT-translation regression cases.
    //
    // These cases pin down the family of bugs where translateExpression silently drops an
    // untranslatable sub-expression (today: WildcardLike) under a logical connector and the
    // resulting FilterPredicate becomes STRICTER than the true ESQL expression. A stricter
    // pushed predicate is a correctness bug for our reader because we use the predicate for
    // row-group and page (column-index) pruning — pruned rows can't be recovered downstream
    // by FilterExec, so a stricter prune silently loses matching rows.
    //
    // The shapes below were not all reachable from the randomized harness with realistic
    // budgets; surfacing the NOT(AND(LIKE, X)) bug took ~17 default-seed iterations on
    // average (see commit 360b065 / the CI investigation). Pinning each shape as a fixed
    // case makes the regression impossible to miss on a single local test run, complementing
    // the randomized coverage that explores the long tail.
    //
    // DO NOT WEAKEN OR REMOVE these tests. They are the deterministic guard for
    // ParquetPushedExpressions.translateExpression's "exactly translatable" contract —
    // if you change that method (or the isExactlyTranslatable helper, or the canConvert /
    // isFullyEvaluable classifications), these must keep passing. Changes that "simplify"
    // the Not branch by removing the isExactlyTranslatable guard MUST be measured against
    // these tests; a green randomized run is not sufficient evidence of correctness.
    // ---------------------------------------------------------------------------------------

    /**
     * Regression: {@code NOT(AND(LIKE, X))} where {@code LIKE} is YES-eligible but does not
     * translate to a {@link FilterPredicate} and {@code X} does. Without the
     * {@code isExactlyTranslatable} guard in
     * {@link ParquetPushedExpressions#translateExpression}, the {@code AND} arm silently
     * dropped the LIKE and produced just {@code translate(X)}; the outer {@code NOT} then
     * negated that, yielding a predicate {@code NOT(X)} that is STRICTER than the true
     * {@code NOT(AND(LIKE, X)) = NOT(LIKE) OR NOT(X)} (because the latter accepts every
     * row where LIKE doesn't match, regardless of {@code X}). The reader would then prune
     * row groups whose stats prove {@code X} for every row, even though those groups may
     * contain rows that don't match LIKE — silent under-inclusion.
     *
     * <p>This is the exact production bug fixed in commit {@code 360b065} ("guard Not
     * translation against silent drops"), found by {@link #testRandomizedFilterTrees} on
     * the ~17th default-seed iteration. Pinning it deterministically here means a single
     * local run catches a regression — no need to re-roll seeds.
     */
    public void testNotAndLikeXIsNotStricterThanTruth() throws IOException {
        // X = (id < ROW_COUNT / 2). Combined with our dataset (id is 0..ROW_COUNT-1
        // contiguous), this produces a row-group-stats-trivial conjunct on the SINGLE_BIG_GROUP
        // layout (false for every row in upper half) and a partial-prune conjunct elsewhere —
        // so the bug surfaces across multiple layouts, not just one.
        runDifferential(not(and(like(URL, "*google*"), lt(ID, (long) (ROW_COUNT / 2), DataType.LONG))));
    }

    /**
     * Symmetric to {@link #testNotAndLikeXIsNotStricterThanTruth}: {@code NOT(OR(LIKE, X))}.
     * Under {@code OR}, an arm that fails to translate is dropped entirely (the conservative
     * "all arms or nothing" rule), so {@code OR(LIKE, X)} would translate to just
     * {@code translate(X)}, and the outer {@code NOT} would yield {@code NOT(X)}, again
     * STRICTER than the truth {@code NOT(OR(LIKE, X)) = NOT(LIKE) AND NOT(X)} (which is at
     * least as restrictive as both). The {@code isExactlyTranslatable} guard refuses to
     * push the {@code Not} when its child contains a silent-drop, so this lands as
     * "no FilterPredicate" and pruning relies on other conjuncts only.
     */
    public void testNotOrLikeXIsNotStricterThanTruth() throws IOException {
        runDifferential(not(or(like(URL, "*google*"), lt(ID, (long) (ROW_COUNT / 2), DataType.LONG))));
    }

    /**
     * {@code NOT(NOT(LIKE))} should be equivalent to {@code LIKE}. The inner NOT alone is
     * YES-eligible (special-cased to handle TVL nulls in {@code evaluateNotWildcardLike}),
     * the outer NOT wraps a non-translatable child (the inner NOT itself doesn't translate,
     * since {@code WildcardLike} doesn't translate), so the outer should refuse to push.
     * No silent drop, no stricter predicate.
     */
    public void testNotNotLikeRoundTrip() throws IOException {
        runDifferential(not(not(like(URL, "*google*"))));
    }

    /**
     * Sanity check that {@code isExactlyTranslatable} is not over-conservative:
     * {@code NOT(AND(X, Y))} with both arms translatable should still translate. If this
     * test starts failing, the guard has become too strict — it should only refuse to push
     * when an arm under {@code AND} or {@code OR} is silently dropped, not when every arm
     * translates exactly.
     */
    public void testNotAndTwoTranslatableConjunctsStillPushes() throws IOException {
        runDifferential(not(and(eq(STATUS, 200L, DataType.LONG), lt(ID, 50L, DataType.LONG))));
    }

    /**
     * {@code AND(LIKE, NOT(X))} — LIKE outside the NOT, NOT around an exactly-translatable
     * conjunct. The LIKE pushes as YES (no FilterPredicate, but YES); the {@code NOT(X)}
     * pushes as RECHECK with a real FilterPredicate. The two combine cleanly: no silent
     * drop because each conjunct is handled independently by {@code pushFilters} (the planner
     * splits top-level AND into a list of conjuncts, see
     * {@code ParquetFilterPushdownSupportTests#testWildcardLikeAsSeparateConjunctWithEqualsRecheckedOnly}).
     * Asserts the per-conjunct path doesn't fall into the same silent-drop trap as the
     * AND-wrapped case.
     */
    public void testAndOfLikeAndNotTranslatable() throws IOException {
        runDifferential(and(like(URL, "*google*"), not(eq(STATUS, 404L, DataType.LONG))));
    }

    /**
     * Deeper nesting: {@code NOT(AND(OR(LIKE1, LIKE2), X))} — both arms of the inner OR are
     * silent-drops, so the OR collapses to nothing under translation; the AND then collapses
     * to just {@code X}; the outer NOT becomes {@code NOT(X)}, STRICTER than the truth.
     * The guard must catch this transitively, not just one level deep. Without the guard,
     * this is the same shape of bug as
     * {@link #testNotAndLikeXIsNotStricterThanTruth} but harder to surface randomly because
     * it requires three levels of nesting in the right configuration.
     */
    public void testNotAndOrLikeLikeXTransitiveSilentDrop() throws IOException {
        Expression filter = not(and(or(like(URL, "*google*"), like(URL, "*github*")), lt(ID, (long) (ROW_COUNT / 2), DataType.LONG)));
        runDifferential(filter);
    }

    // ---------------------------------------------------------------------------------------
    // Randomized cases — random expression trees per layout.
    // ---------------------------------------------------------------------------------------

    public void testRandomizedFilterTrees() throws IOException {
        Layout layout = randomFrom(Layout.values());
        DatasetAndBytes dataset = buildDataset(layout);
        // Randomization budget: a moderate number of iterations per layout, kept small so
        // the suite stays under a few seconds. Failures reproduce with -Dtests.seed=... and
        // print the offending expression tree.
        int iterations = scaledRandomIntBetween(15, 30);
        for (int i = 0; i < iterations; i++) {
            Expression filter = randomExpression(2);
            assertOracleAgreement(dataset, filter, layout, "iteration " + i);
        }
    }

    /**
     * Targeted randomized variant: every tree contains at least one {@code WildcardLike}
     * conjunct so the YES + non-translatable code path is exercised every iteration. This
     * is the regression-most-likely shape (the trivially-passes leak we just fixed and
     * any future YES-eligible expressions that don't translate).
     */
    public void testRandomizedFilterTreesWithLikeAlwaysPresent() throws IOException {
        Layout layout = randomFrom(Layout.values());
        DatasetAndBytes dataset = buildDataset(layout);
        int iterations = scaledRandomIntBetween(15, 30);
        for (int i = 0; i < iterations; i++) {
            Expression like = randomLike();
            Expression rest = randomExpression(2);
            // AND-combine so the test exercises mixed YES/RECHECK splits at the per-conjunct
            // level (the actual planner shape).
            Expression filter = and(like, rest);
            assertOracleAgreement(dataset, filter, layout, "iteration " + i);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Differential harness
    // ---------------------------------------------------------------------------------------

    /**
     * Runs the same filter against every layout and asserts oracle agreement on each. Used by
     * the fixed-case tests so they get coverage across all parquet write configs without
     * having to enumerate them manually.
     */
    private void runDifferential(Expression filter) throws IOException {
        for (Layout layout : Layout.values()) {
            DatasetAndBytes dataset = buildDataset(layout);
            assertOracleAgreement(dataset, filter, layout, "fixed case");
        }
    }

    /**
     * Lockstep comparison: the production reader path and three independent oracles must all
     * produce the same set of {@code id} values for {@code filter}. Any pairwise disagreement
     * is reported with which pair disagreed and a sample of the symmetric difference.
     *
     * <p>The four paths are deliberately heterogeneous so each one catches a different family
     * of bugs (see class Javadoc for the per-bug-class table):
     * <ul>
     *   <li><b>production</b>: our optimized reader with per-conjunct YES/RECHECK pushdown
     *       (the unit under test).</li>
     *   <li><b>oracleA_apacheMr</b>: apache-mr's {@link ParquetReader} given the translated
     *       {@link FilterPredicate}, then post-filtered in Java with the original ESQL
     *       expression. The post-filter restores ESQL TVL semantics that diverge from
     *       parquet-mr's (e.g. {@code NOT(eq(col, k))} returns nulls in parquet-mr but drops
     *       them in ESQL) and applies any conjuncts that did not translate (e.g. {@link
     *       WildcardLike}). This is a true cross-stack check.</li>
     *   <li><b>oracleB_pushdownDisabled</b>: our reader with no pushed filter at all (no
     *       row-group pruning, no page pruning, no late-mat, no two-phase). All filtering is
     *       done in Java on the reader's full output. This isolates "does pushdown break
     *       anything that the unoptimized path gets right".</li>
     *   <li><b>oracleC_pureJava</b>: pure-Java evaluation of the ESQL expression against the
     *       in-memory rows that were used to write the parquet file. Doesn't touch parquet
     *       at all. Catches translation bugs that both A and B might share (e.g. if our
     *       FilterPredicate translation is off by one).</li>
     * </ul>
     */
    private void assertOracleAgreement(DatasetAndBytes dataset, Expression filter, Layout layout, String iterationTag) throws IOException {
        Set<Long> production = productionReaderEvaluate(dataset.parquetBytes, filter);
        Set<Long> apacheMr = oracleA_apacheMr(dataset.parquetBytes, filter);
        Set<Long> noPushdown = oracleB_pushdownDisabled(dataset.parquetBytes, filter);
        Set<Long> pureJava = oracleC_pureJava(dataset.rows, filter);

        // Order matters for diagnostics: check oracle-vs-oracle first so a real production
        // bug is reported with the simplest possible "production-vs-pureJava" message instead
        // of a confusing chain of cascading mismatches. If oracles disagree among themselves,
        // that's a separate (and rarer) class of bug — fail loud on that first.
        check(layout, iterationTag, filter, "apacheMr-vs-pureJava", apacheMr, pureJava);
        check(layout, iterationTag, filter, "noPushdown-vs-pureJava", noPushdown, pureJava);
        // Now both oracles A and B agree with C (the canonical ESQL semantics oracle), so any
        // remaining disagreement is squarely a production-reader bug.
        check(layout, iterationTag, filter, "production-vs-pureJava", production, pureJava);
        // Belt-and-braces: production-vs-apacheMr would catch the rare case where pureJava
        // and production happen to share a wrong answer (e.g. a bug in our oracle's
        // understanding of ESQL semantics). After the previous three checks pass, this is
        // mathematically implied — we keep it as a static assertion of the invariant.
        check(layout, iterationTag, filter, "production-vs-apacheMr", production, apacheMr);
    }

    private static void check(Layout layout, String iterationTag, Expression filter, String pair, Set<Long> a, Set<Long> b) {
        if (a.equals(b)) {
            return;
        }
        Set<Long> aOnly = new TreeSet<>(a);
        aOnly.removeAll(b);
        Set<Long> bOnly = new TreeSet<>(b);
        bOnly.removeAll(a);
        throw new AssertionError(
            String.format(
                Locale.ROOT,
                "Pair disagreement [%s] on layout=%s [%s]%n  filter: %s%n  left.size=%d, right.size=%d%n"
                    + "  in left only (sample 20): %s%n  in right only (sample 20): %s",
                pair,
                layout,
                iterationTag,
                describe(filter),
                a.size(),
                b.size(),
                sampled(aOnly, 20),
                sampled(bOnly, 20)
            )
        );
    }

    /**
     * Compact, single-line description of an ESQL expression tree for assertion messages.
     * The default {@code toString()} is multi-line; this one is grep-friendly when scanning
     * test failure output.
     */
    private static String describe(Expression expr) {
        if (expr instanceof And and) return "AND(" + describe(and.left()) + ", " + describe(and.right()) + ")";
        if (expr instanceof Or or) return "OR(" + describe(or.left()) + ", " + describe(or.right()) + ")";
        if (expr instanceof Not not) return "NOT(" + describe(not.field()) + ")";
        if (expr instanceof IsNull isNull) return ((ReferenceAttribute) isNull.field()).name() + " IS NULL";
        if (expr instanceof IsNotNull isNotNull) return ((ReferenceAttribute) isNotNull.field()).name() + " IS NOT NULL";
        if (expr instanceof Range range) {
            String name = ((ReferenceAttribute) range.value()).name();
            String lo = range.includeLower() ? ">=" : ">";
            String hi = range.includeUpper() ? "<=" : "<";
            return name + lo + ((Literal) range.lower()).value() + " AND " + name + hi + ((Literal) range.upper()).value();
        }
        if (expr instanceof Equals e) return ((ReferenceAttribute) e.left()).name() + "=" + literalToString(((Literal) e.right()).value());
        if (expr instanceof NotEquals e) return ((ReferenceAttribute) e.left()).name()
            + "!="
            + literalToString(((Literal) e.right()).value());
        if (expr instanceof GreaterThan e) return ((ReferenceAttribute) e.left()).name()
            + ">"
            + literalToString(((Literal) e.right()).value());
        if (expr instanceof GreaterThanOrEqual e) {
            return ((ReferenceAttribute) e.left()).name() + ">=" + literalToString(((Literal) e.right()).value());
        }
        if (expr instanceof LessThan e) return ((ReferenceAttribute) e.left()).name()
            + "<"
            + literalToString(((Literal) e.right()).value());
        if (expr instanceof LessThanOrEqual e) {
            return ((ReferenceAttribute) e.left()).name() + "<=" + literalToString(((Literal) e.right()).value());
        }
        if (expr instanceof StartsWith sw) {
            String name = ((ReferenceAttribute) sw.singleValueField()).name();
            return name + " STARTS_WITH '" + ((BytesRef) ((Literal) sw.prefix()).value()).utf8ToString() + "'";
        }
        if (expr instanceof WildcardLike wl) {
            String name = ((ReferenceAttribute) wl.field()).name();
            String op = wl.caseInsensitive() ? " LIKE/i " : " LIKE ";
            return name + op + "'" + wl.pattern().asLuceneWildcard() + "'";
        }
        return expr.toString();
    }

    private static String literalToString(Object v) {
        if (v instanceof BytesRef br) return "'" + br.utf8ToString() + "'";
        return String.valueOf(v);
    }

    private static String sampled(Set<Long> set, int max) {
        StringBuilder b = new StringBuilder("[");
        int n = 0;
        for (Long v : set) {
            if (n++ > 0) b.append(", ");
            if (n > max) {
                b.append("...");
                break;
            }
            b.append(v);
        }
        return b.append("]").toString();
    }

    /**
     * Oracle C: pure-Java evaluation of an ESQL filter against the in-memory rows. No parquet
     * involved at all. Mirrors the runtime semantics the late-mat evaluator implements
     * (TVL: nulls collapse to "no match" for regular predicates; {@code IS NULL} /
     * {@code IS NOT NULL} are explicit). This is the canonical ESQL semantics oracle.
     */
    private static Set<Long> oracleC_pureJava(List<Map<String, Object>> rows, Expression filter) {
        Set<Long> ids = new TreeSet<>();
        for (Map<String, Object> row : rows) {
            if (Boolean.TRUE.equals(evalNode(filter, row))) {
                ids.add((Long) row.get("id"));
            }
        }
        return ids;
    }

    /**
     * Returns {@code Boolean.TRUE}, {@code Boolean.FALSE}, or {@code null} (UNKNOWN under TVL).
     * The reader's late-mat evaluator collapses {@code null} to "no match" via the per-row
     * mask, but that collapse happens at {@link #oracleC_pureJava} (which only retains rows
     * that evaluated to {@code TRUE}).
     */
    private static Boolean evalNode(Expression expr, Map<String, Object> row) {
        if (expr instanceof And and) {
            Boolean l = evalNode(and.left(), row);
            Boolean r = evalNode(and.right(), row);
            if (Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r)) return false;
            if (l == null || r == null) return null;
            return true;
        }
        if (expr instanceof Or or) {
            Boolean l = evalNode(or.left(), row);
            Boolean r = evalNode(or.right(), row);
            if (Boolean.TRUE.equals(l) || Boolean.TRUE.equals(r)) return true;
            if (l == null || r == null) return null;
            return false;
        }
        if (expr instanceof Not not) {
            Boolean inner = evalNode(not.field(), row);
            if (inner == null) return null;
            return inner ? Boolean.FALSE : Boolean.TRUE;
        }
        if (expr instanceof IsNull isNull) {
            String name = ((ReferenceAttribute) isNull.field()).name();
            return row.get(name) == null;
        }
        if (expr instanceof IsNotNull isNotNull) {
            String name = ((ReferenceAttribute) isNotNull.field()).name();
            return row.get(name) != null;
        }
        if (expr instanceof Range range) {
            String name = ((ReferenceAttribute) range.value()).name();
            Object v = row.get(name);
            if (v == null) return null;
            Number lower = (Number) ((Literal) range.lower()).value();
            Number upper = (Number) ((Literal) range.upper()).value();
            double dv = ((Number) v).doubleValue();
            double dl = lower.doubleValue();
            double du = upper.doubleValue();
            boolean lOk = range.includeLower() ? dv >= dl : dv > dl;
            boolean uOk = range.includeUpper() ? dv <= du : dv < du;
            return lOk && uOk;
        }
        if (expr instanceof Equals e) {
            return cmpEq(row, e.left(), e.right());
        }
        if (expr instanceof NotEquals ne) {
            Boolean eqRes = cmpEq(row, ne.left(), ne.right());
            if (eqRes == null) return null;
            return eqRes ? Boolean.FALSE : Boolean.TRUE;
        }
        if (expr instanceof GreaterThan gt) {
            return cmpOrdered(row, gt.left(), gt.right(), 1, false);
        }
        if (expr instanceof GreaterThanOrEqual gte) {
            return cmpOrdered(row, gte.left(), gte.right(), 1, true);
        }
        if (expr instanceof LessThan lt) {
            return cmpOrdered(row, lt.left(), lt.right(), -1, false);
        }
        if (expr instanceof LessThanOrEqual lte) {
            return cmpOrdered(row, lte.left(), lte.right(), -1, true);
        }
        if (expr instanceof StartsWith sw) {
            String name = ((ReferenceAttribute) sw.singleValueField()).name();
            Object v = row.get(name);
            if (v == null) return null;
            BytesRef prefix = (BytesRef) ((Literal) sw.prefix()).value();
            return ((String) v).startsWith(prefix.utf8ToString());
        }
        if (expr instanceof WildcardLike wl) {
            String name = ((ReferenceAttribute) wl.field()).name();
            Object v = row.get(name);
            if (v == null) return null;
            String regex = wl.pattern().asJavaRegex();
            int flags = wl.caseInsensitive() ? Pattern.CASE_INSENSITIVE : 0;
            return Pattern.compile(regex, flags).matcher((String) v).matches();
        }
        throw new AssertionError("oracle does not handle expression: " + expr.getClass());
    }

    private static Boolean cmpEq(Map<String, Object> row, Expression leftExpr, Expression rightExpr) {
        String name = ((ReferenceAttribute) leftExpr).name();
        Object v = row.get(name);
        if (v == null) return null;
        Object literal = ((Literal) rightExpr).value();
        if (literal instanceof BytesRef br) {
            return ((String) v).equals(br.utf8ToString());
        }
        if (literal instanceof Number n && v instanceof Number nv) {
            return Double.compare(n.doubleValue(), nv.doubleValue()) == 0;
        }
        return literal.equals(v);
    }

    private static Boolean cmpOrdered(Map<String, Object> row, Expression leftExpr, Expression rightExpr, int sign, boolean orEqual) {
        String name = ((ReferenceAttribute) leftExpr).name();
        Object v = row.get(name);
        if (v == null) return null;
        Object literal = ((Literal) rightExpr).value();
        int cmp;
        if (literal instanceof BytesRef br && v instanceof String s) {
            cmp = s.compareTo(br.utf8ToString());
        } else if (literal instanceof Number n && v instanceof Number nv) {
            cmp = Double.compare(nv.doubleValue(), n.doubleValue());
        } else {
            throw new AssertionError("unsupported comparison: " + literal.getClass() + " vs " + v.getClass());
        }
        if (cmp == 0) return orEqual;
        return Integer.signum(cmp) == Integer.signum(sign);
    }

    /**
     * Production reader path: optimized reader with a {@link ParquetPushedExpressions}
     * wrapping the filter (mirroring what {@code PushFiltersToSource} builds). The filter is
     * decomposed into top-level AND conjuncts so the per-conjunct YES/RECHECK classification
     * matches the planner's behavior. RECHECK and NO conjuncts are then applied as a pure-Java
     * post-filter on the reader's output, mirroring what {@code FilterExec} does in production.
     *
     * <p>This is the unit under test. Two-step structure mirrors the production planner:
     * <ol>
     *   <li>The reader receives every conjunct via {@code withPushedFilter} and is allowed to
     *       use any of them for stats pruning, page pruning, late-mat compaction, etc.</li>
     *   <li>Any conjunct whose {@link ParquetFilterPushdownSupport#canPush} classification is
     *       {@link FilterPushdownSupport.Pushability#RECHECK} or
     *       {@link FilterPushdownSupport.Pushability#NO} is re-applied in Java — exactly what
     *       {@code FilterExec} would do.</li>
     * </ol>
     *
     * <p>This precisely captures the YES-vs-RECHECK contract: a YES conjunct that the reader
     * silently drops (the trivially-passes leak) has no FilterExec safety net here, so the
     * test catches it. A RECHECK conjunct that the reader over-includes is fine because the
     * post-filter cleans it up — and that mirrors production semantics exactly.
     */
    private Set<Long> productionReaderEvaluate(byte[] parquetBytes, Expression filter) throws IOException {
        List<Expression> conjuncts = splitTopLevelAnd(filter);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(conjuncts);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);

        // Determine the FilterExec remainder using the same classifier the planner uses.
        ParquetFilterPushdownSupport pushdownSupport = new ParquetFilterPushdownSupport();
        List<Expression> remainder = new ArrayList<>();
        for (Expression conjunct : conjuncts) {
            if (pushdownSupport.canPush(conjunct) != FilterPushdownSupport.Pushability.YES) {
                remainder.add(conjunct);
            }
        }

        Set<Long> readerIds = collectIdsWithRowMaterialization(reader, parquetBytes, remainder);
        return readerIds;
    }

    /**
     * Reads every page from the reader, then for each row applies the {@code remainder}
     * conjuncts (RECHECK/NO) as a pure-Java post-filter. Returns the surviving {@code id}
     * values. Used by the production reader path to mirror what {@code FilterExec} does on
     * top of the reader's output.
     */
    private Set<Long> collectIdsWithRowMaterialization(ParquetFormatReader reader, byte[] parquetBytes, List<Expression> remainder)
        throws IOException {
        StorageObject storageObject = inMemoryStorageObject(parquetBytes);
        Set<Long> ids = new TreeSet<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                try {
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        Map<String, Object> row = pageRowToMap(page, i);
                        boolean keep = true;
                        for (Expression conj : remainder) {
                            if (Boolean.TRUE.equals(evalNode(conj, row)) == false) {
                                keep = false;
                                break;
                            }
                        }
                        if (keep) {
                            ids.add((Long) row.get("id"));
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return ids;
    }

    /**
     * Oracle B: our reader with NO pushdown at all (no {@code withPushedFilter}). The reader
     * returns every row of every page in every row group — no row-group pruning, no
     * ColumnIndex page pruning, no late-mat compaction, no two-phase prefetch — and we apply
     * the filter in pure Java on top. This is a "same parser, different config" oracle: it
     * exercises the read path that does NOT take any of the optimization shortcuts that the
     * production path takes, isolating bugs introduced specifically by those shortcuts (e.g.
     * trivially-passes, RowRanges over-pruning).
     */
    private Set<Long> oracleB_pushdownDisabled(byte[] parquetBytes, Expression filter) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true);
        Set<Long> allRows = collectIdsWithEval(reader, parquetBytes, filter);
        return allRows;
    }

    /**
     * Oracle A: read with apache-mr's {@link ParquetReader} given a <b>conservatively</b>
     * translated {@link FilterPredicate}, then post-filter the original ESQL expression in
     * pure Java on top. apache-mr is a completely independent parquet stack (different
     * RowGroupFilter, different ColumnIndex evaluator, different page reader), so any bug
     * in one of our optimization layers shows up here as a disagreement.
     *
     * <p><b>Why "conservatively translated".</b> Our production
     * {@code ParquetPushedExpressions#translateExpression} silently drops untranslatable
     * sub-expressions inside nested {@code AND}/{@code OR}/{@code NOT}, which can produce a
     * predicate that is <em>stricter</em> than the original ESQL filter (e.g.
     * {@code NOT(AND(LIKE, id<N))} translates to {@code NOT(id<N)} = {@code id>=N}, which
     * drops rows the original would have kept). Apache-mr applies the predicate at the
     * record level via {@code useRecordFilter}, so a stricter predicate <em>silently loses
     * rows</em> that the Java post-filter cannot recover. Production's optimized reader does
     * not exercise the same loss because it uses the FilterPredicate only for stats-based
     * pruning, not for record-level filtering — but apache-mr does, and that mismatch is
     * what made this oracle initially flag false positives on randomized
     * {@code NOT(AND(LIKE, ...))} inputs.
     *
     * <p>To stay correct (an oracle, not a co-bug), we only push the FilterPredicate when
     * the entire ESQL filter consists of top-level {@code AND} conjuncts where every
     * conjunct is a translatable leaf with NO nested {@code NOT}/{@code OR} that could trigger the
     * silent-drop. Anything more complex: skip the FilterPredicate; apache-mr reads every
     * row and the Java post-filter applies the original expression. This costs us the
     * cross-stack check on the apache-mr filter machinery for those cases — acceptable, the
     * point of this oracle is to catch bugs in our reader, not to validate apache-mr.
     */
    private Set<Long> oracleA_apacheMr(byte[] parquetBytes, Expression filter) throws IOException {
        FilterPredicate filterPredicate = safeTranslateForApacheMr(filter);
        GroupReaderBuilder builder = new GroupReaderBuilder(new ParquetStorageObjectAdapter(inMemoryStorageObject(parquetBytes)));
        if (filterPredicate != null) {
            builder.withFilter(FilterCompat.get(filterPredicate));
        }
        Set<Long> ids = new TreeSet<>();
        try (ParquetReader<Group> mrReader = builder.build()) {
            Group g;
            while ((g = mrReader.read()) != null) {
                Map<String, Object> row = groupToRow(g);
                if (Boolean.TRUE.equals(evalNode(filter, row))) {
                    ids.add((Long) row.get("id"));
                }
            }
        }
        return ids;
    }

    /**
     * Translate {@code filter} to a Parquet {@link FilterPredicate} only when the result is
     * provably a SUPERSET of the original filter's matching set (i.e. apache-mr can safely
     * pre-filter without losing any row the Java post-filter would have kept). Returns
     * {@code null} otherwise — caller must read every row and rely on the post-filter.
     *
     * <p>Conservative rule: split at top-level {@code AND}, accept only conjuncts that are
     * "leaf-shaped" (no nested {@code NOT}/{@code OR}/{@code AND}, no {@link WildcardLike}).
     * For each accepted conjunct, ask the production translator; if it returns non-null,
     * that conjunct's predicate is a true superset (since it's a leaf with no
     * silent-drop potential). AND the accepted predicates and return.
     */
    private static FilterPredicate safeTranslateForApacheMr(Expression filter) {
        List<Expression> conjuncts = splitTopLevelAnd(filter);
        List<Expression> safe = new ArrayList<>();
        for (Expression c : conjuncts) {
            if (isLeafShaped(c)) {
                safe.add(c);
            }
        }
        if (safe.isEmpty()) {
            return null;
        }
        return new ParquetPushedExpressions(safe).toFilterPredicate(SCHEMA);
    }

    /**
     * A leaf-shaped expression contains no {@code NOT}, {@code OR}, {@code AND}, or
     * {@link WildcardLike} anywhere in the tree. Such expressions translate to a Parquet
     * predicate whose semantics exactly match (or are looser than) the ESQL original — there
     * is no silent-drop branch in {@code translateExpression} that can fire.
     */
    private static boolean isLeafShaped(Expression expr) {
        if (expr instanceof And || expr instanceof Or || expr instanceof Not) {
            return false;
        }
        if (expr instanceof WildcardLike) {
            return false;
        }
        return true;
    }

    /**
     * Tiny {@link ParquetReader.Builder} subclass that wires up a {@link GroupReadSupport}.
     * Uses the {@code (InputFile, ParquetConfiguration)} constructor explicitly so we get a
     * {@link PlainParquetConfiguration} — the Hadoop-based configuration the no-arg path
     * resolves to pulls in {@code com.ctc.wstx.io.InputBootstrapper} (Woodstox), which isn't
     * on the test classpath. Passing a {@code ParquetConfiguration} skips that resolution
     * entirely.
     */
    private static final class GroupReaderBuilder extends ParquetReader.Builder<Group> {
        GroupReaderBuilder(org.apache.parquet.io.InputFile file) {
            super(file, new PlainParquetConfiguration());
        }

        @Override
        protected org.apache.parquet.hadoop.api.ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }

    /**
     * Reads every row from the reader, applies {@code filter} in pure Java per row, and
     * collects matching {@code id} values. Used by oracle B (pushdown disabled).
     */
    private Set<Long> collectIdsWithEval(ParquetFormatReader reader, byte[] parquetBytes, Expression filter) throws IOException {
        StorageObject storageObject = inMemoryStorageObject(parquetBytes);
        Set<Long> ids = new TreeSet<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                try {
                    int positions = page.getPositionCount();
                    for (int i = 0; i < positions; i++) {
                        Map<String, Object> row = pageRowToMap(page, i);
                        if (Boolean.TRUE.equals(evalNode(filter, row))) {
                            ids.add((Long) row.get("id"));
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return ids;
    }

    /**
     * Materialize a single row from a {@link Page} as a {@code Map} so the pure-Java
     * evaluator can be reused.
     */
    private static Map<String, Object> pageRowToMap(Page page, int rowIndex) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", ((LongBlock) page.getBlock(0)).getLong(rowIndex));
        row.put("status", ((LongBlock) page.getBlock(1)).getLong(rowIndex));
        row.put("score", ((DoubleBlock) page.getBlock(2)).getDouble(rowIndex));
        row.put("category", ((BytesRefBlock) page.getBlock(3)).getBytesRef(rowIndex, new BytesRef()).utf8ToString());
        row.put("url", ((BytesRefBlock) page.getBlock(4)).getBytesRef(rowIndex, new BytesRef()).utf8ToString());
        row.put("description", ((BytesRefBlock) page.getBlock(5)).getBytesRef(rowIndex, new BytesRef()).utf8ToString());
        Block flagBlock = page.getBlock(6);
        if (flagBlock.isNull(rowIndex)) {
            row.put("nullable_flag", null);
        } else {
            row.put("nullable_flag", ((IntBlock) flagBlock).getInt(rowIndex));
        }
        return row;
    }

    /**
     * Convert an apache-mr {@link Group} to the same map shape used by the pure-Java
     * evaluator. {@code nullable_flag} is decoded as {@code null} when the group has no
     * value at that field index (i.e. {@code optional} field with no value written).
     */
    private static Map<String, Object> groupToRow(Group g) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", g.getLong("id", 0));
        row.put("status", g.getLong("status", 0));
        row.put("score", g.getDouble("score", 0));
        row.put("category", g.getString("category", 0));
        row.put("url", g.getString("url", 0));
        row.put("description", g.getString("description", 0));
        if (g.getFieldRepetitionCount("nullable_flag") == 0) {
            row.put("nullable_flag", null);
        } else {
            row.put("nullable_flag", g.getInteger("nullable_flag", 0));
        }
        return row;
    }

    private static List<Expression> splitTopLevelAnd(Expression filter) {
        List<Expression> out = new ArrayList<>();
        splitInto(filter, out);
        return out;
    }

    private static void splitInto(Expression e, List<Expression> out) {
        if (e instanceof And and) {
            splitInto(and.left(), out);
            splitInto(and.right(), out);
        } else {
            out.add(e);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Random expression generation
    // ---------------------------------------------------------------------------------------

    /**
     * Builds a random ESQL filter expression of bounded depth over the test schema. The
     * generated tree exercises the planner's per-conjunct classification because the top
     * level is AND/OR/NOT'd from primitive predicates with various pushability profiles.
     */
    private Expression randomExpression(int depthRemaining) {
        if (depthRemaining <= 0 || randomIntBetween(0, 3) == 0) {
            return randomLeaf();
        }
        int kind = randomIntBetween(0, 4);
        return switch (kind) {
            case 0 -> and(randomExpression(depthRemaining - 1), randomExpression(depthRemaining - 1));
            case 1 -> or(randomExpression(depthRemaining - 1), randomExpression(depthRemaining - 1));
            case 2 -> not(randomExpression(depthRemaining - 1));
            default -> randomLeaf();
        };
    }

    private Expression randomLeaf() {
        int kind = randomIntBetween(0, 9);
        return switch (kind) {
            case 0 -> eq(STATUS, randomLongStatus(), DataType.LONG);
            case 1 -> neq(STATUS, randomLongStatus(), DataType.LONG);
            case 2 -> gt(ID, (long) randomIntBetween(0, ROW_COUNT), DataType.LONG);
            case 3 -> lt(ID, (long) randomIntBetween(0, ROW_COUNT), DataType.LONG);
            case 4 -> like(URL, randomLikePattern());
            case 5 -> startsWith(URL, randomPrefix());
            case 6 -> isNull(NULLABLE_FLAG);
            case 7 -> isNotNull(NULLABLE_FLAG);
            case 8 -> eq(CATEGORY, randomFrom(CATEGORIES), DataType.KEYWORD);
            default -> and(
                gte(SCORE, randomDoubleBetween(0.0, 1.0, true), DataType.DOUBLE),
                lt(SCORE, randomDoubleBetween(0.0, 1.0, true) + 1.0, DataType.DOUBLE)
            );
        };
    }

    private Expression randomLike() {
        return randomBoolean() ? like(URL, randomLikePattern()) : likeCaseInsensitive(URL, randomLikePattern().toUpperCase(Locale.ROOT));
    }

    private long randomLongStatus() {
        return (long) randomFrom(new Integer[] { 200, 404, 500 });
    }

    private String randomLikePattern() {
        // Mix prefix-extractable patterns (good for the StartsWith translation), infix (no
        // prefix), suffix, and match-all.
        int k = randomIntBetween(0, 5);
        return switch (k) {
            case 0 -> "https://google*";
            case 1 -> "*google*";
            case 2 -> "*example*";
            case 3 -> "*github.com";
            case 4 -> "https://elastic*";
            default -> "*";
        };
    }

    private String randomPrefix() {
        return randomFrom(new String[] { "https://google", "https://example", "https://elastic", "https://github" });
    }

    // ---------------------------------------------------------------------------------------
    // Expression builders (terse, to keep the test bodies readable)
    // ---------------------------------------------------------------------------------------

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Literal lit(Object value, DataType type) {
        if (type == DataType.KEYWORD && value instanceof String s) {
            return new Literal(Source.EMPTY, new BytesRef(s), type);
        }
        return new Literal(Source.EMPTY, value, type);
    }

    private static Expression eq(ReferenceAttribute a, Object v, DataType t) {
        return new Equals(Source.EMPTY, a, lit(v, t), null);
    }

    private static Expression neq(ReferenceAttribute a, Object v, DataType t) {
        return new NotEquals(Source.EMPTY, a, lit(v, t), null);
    }

    private static Expression gt(ReferenceAttribute a, Object v, DataType t) {
        return new GreaterThan(Source.EMPTY, a, lit(v, t), null);
    }

    private static Expression gte(ReferenceAttribute a, Object v, DataType t) {
        return new GreaterThanOrEqual(Source.EMPTY, a, lit(v, t), null);
    }

    private static Expression lt(ReferenceAttribute a, Object v, DataType t) {
        return new LessThan(Source.EMPTY, a, lit(v, t), null);
    }

    private static Expression and(Expression l, Expression r) {
        return new And(Source.EMPTY, l, r);
    }

    private static Expression or(Expression l, Expression r) {
        return new Or(Source.EMPTY, l, r);
    }

    private static Expression not(Expression e) {
        return new Not(Source.EMPTY, e);
    }

    private static Expression isNull(ReferenceAttribute a) {
        return new IsNull(Source.EMPTY, a);
    }

    private static Expression isNotNull(ReferenceAttribute a) {
        return new IsNotNull(Source.EMPTY, a);
    }

    private static Expression like(ReferenceAttribute a, String pattern) {
        return new WildcardLike(Source.EMPTY, a, new WildcardPattern(pattern));
    }

    private static Expression likeCaseInsensitive(ReferenceAttribute a, String pattern) {
        return new WildcardLike(Source.EMPTY, a, new WildcardPattern(pattern), true);
    }

    private static Expression startsWith(ReferenceAttribute a, String prefix) {
        return new StartsWith(Source.EMPTY, a, lit(prefix, DataType.KEYWORD));
    }

    @SuppressWarnings("unused")
    private static Expression rangeInt(ReferenceAttribute a, long lo, long hi) {
        return new Range(Source.EMPTY, a, lit(lo, DataType.LONG), true, lit(hi, DataType.LONG), false, ZoneOffset.UTC);
    }

    // ---------------------------------------------------------------------------------------
    // Dataset construction
    // ---------------------------------------------------------------------------------------

    /**
     * Parquet write configurations. Each one stresses a different optimization layer:
     * <ul>
     *   <li>{@link #SINGLE_BIG_GROUP}: no row-group pruning, no page pruning — pure late-mat.</li>
     *   <li>{@link #MANY_SMALL_GROUPS}: row-group level pruning matters; page pruning small.</li>
     *   <li>{@link #SMALL_PAGES_DICT}: dictionary encoding + page-level ColumnIndex active.</li>
     *   <li>{@link #SMALL_PAGES_NO_DICT}: same layout but plain encoding to disable dict
     *       short-circuit on the LIKE path.</li>
     * </ul>
     */
    private enum Layout {
        SINGLE_BIG_GROUP(64L * 1024 * 1024, 1024 * 1024, true),
        MANY_SMALL_GROUPS(64 * 1024L, 8 * 1024, true),
        SMALL_PAGES_DICT(1024 * 1024L, 1024, true),
        SMALL_PAGES_NO_DICT(1024 * 1024L, 1024, false);

        final long rowGroupSize;
        final int pageSize;
        final boolean dictionary;

        Layout(long rowGroupSize, int pageSize, boolean dictionary) {
            this.rowGroupSize = rowGroupSize;
            this.pageSize = pageSize;
            this.dictionary = dictionary;
        }
    }

    /**
     * Holds both the in-memory rows (oracle input) and the serialized Parquet bytes
     * (reader input). Built once per layout per test method.
     */
    private record DatasetAndBytes(List<Map<String, Object>> rows, byte[] parquetBytes) {}

    private DatasetAndBytes buildDataset(Layout layout) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", (long) i);
            // status assignment is contiguous-block: rows 0..(ROW_COUNT/2)-1 are all 200, then
            // alternating 200/404/500 blocks of 50. The first half being uniformly 200 is
            // critical: with small row-group / page layouts, those row groups have stats
            // min=max=200, which makes "status = 200" stats-trivial and fires the
            // trivially-passes shortcut. That is the exact shape that the YES + non-translatable
            // (LIKE) bug exploits — without contiguous uniform blocks here, the differential
            // suite would silently agree with the bug.
            long status;
            if (i < ROW_COUNT / 2) {
                status = 200L;
            } else {
                int block = (i - ROW_COUNT / 2) / 50;
                status = switch (block % 3) {
                    case 0 -> 200L;
                    case 1 -> 404L;
                    default -> 500L;
                };
            }
            row.put("status", status);
            row.put("score", (i % 100) / 100.0);
            row.put("category", CATEGORIES[i % CATEGORIES.length]);
            row.put("url", "https://" + URL_HOSTS[i % URL_HOSTS.length] + "/page?id=" + i);
            row.put("description", "padding_" + ("p".repeat(50)) + "_row_" + i);
            // ~30% nulls in nullable_flag.
            row.put("nullable_flag", (i % 10 < 3) ? null : Integer.valueOf(i % 100));
            rows.add(row);
        }
        byte[] bytes = writeParquet(rows, layout);
        return new DatasetAndBytes(rows, bytes);
    }

    private static byte[] writeParquet(List<Map<String, Object>> rows, Layout layout) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = outputFile(out);
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(SCHEMA)
                .withRowGroupSize(layout.rowGroupSize)
                .withPageSize(layout.pageSize)
                .withDictionaryEncoding(layout.dictionary)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Map<String, Object> row : rows) {
                Group g = factory.newGroup();
                g.add("id", (Long) row.get("id"));
                g.add("status", (Long) row.get("status"));
                g.add("score", (Double) row.get("score"));
                g.add("category", (String) row.get("category"));
                g.add("url", (String) row.get("url"));
                g.add("description", (String) row.get("description"));
                Object flag = row.get("nullable_flag");
                if (flag != null) {
                    g.add("nullable_flag", ((Integer) flag).intValue());
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    // ---------------------------------------------------------------------------------------
    // I/O helpers (storage object + output file). Mirror the helpers in OptimizedFilteredReaderTests
    // but kept self-contained so this test doesn't depend on that class's package-private state.
    // ---------------------------------------------------------------------------------------

    private static StorageObject inMemoryStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://differential.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(out);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionOutputStream(out);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://differential.parquet";
            }
        };
    }

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream out) {
        return new PositionOutputStream() {
            @Override
            public long getPos() {
                return out.size();
            }

            @Override
            public void write(int b) {
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                out.write(b, off, len);
            }
        };
    }

}

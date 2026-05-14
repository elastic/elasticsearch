/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.ImmediateLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link SemiJoin} static methods: inlineData, firstSubPlan, newMainPlan.
 */
public class SemiJoinTests extends ESTestCase {

    private static final int HASH_JOIN_THRESHOLD = PlannerSettings.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getDefault(Settings.EMPTY);
    private static final BlockFactory BLOCK_FACTORY = TestBlockFactory.getNonBreakingInstance();

    // -- inlineData tests --

    public void testInlineDataProducesFilterWithIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        // 3 distinct values so the dedup-first filter path produces an IN list of size 3.
        Block[] blocks = new Block[] { intBlock(1, 2, 3) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(inlined, Filter.class);
        var inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.value(), equalTo(leftField));
        assertThat(inExpr.list().size(), equalTo(3));
    }

    /**
     * BlockHash dedup runs before the threshold check, so a subquery with many duplicates of a
     * single value collapses to a 1-literal IN list even when its raw position count exceeds the
     * threshold. This is the core of the "dedup-first" optimization.
     */
    public void testInlineDataDuplicatesAboveRawThresholdStillUsesFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        // Many duplicates of one value: raw count > threshold, dedup count == 1.
        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(inlined, Filter.class);
        var inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.list().size(), equalTo(1));
    }

    public void testInlineDataAntiJoinProducesNotIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, 2) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(inlined, Filter.class);
        var not = as(filter.condition(), Not.class);
        as(not.field(), In.class);
    }

    public void testInlineDataEmptyResult() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        LocalRelation emptyResult = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(0)));

        // SEMI with empty result -> FALSE
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);
        var inlined = as(SemiJoin.inlineData(semiJoin, emptyResult, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.FALSE));

        // ANTI with empty result -> TRUE
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);
        inlined = as(SemiJoin.inlineData(antiJoin, emptyResult, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.TRUE));
    }

    // -- hash join threshold tests --

    public void testInlineDataSemiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        // Distinct values so dedup count exceeds the threshold and the hash-join branch is taken.
        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { distinctIntBlock(count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    public void testInlineDataAntiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { distinctIntBlock(count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(antiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    public void testInlineDataAtThresholdStillUsesFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        // Distinct values: dedup count == threshold, still uses filter path.
        Block[] blocks = new Block[] { distinctIntBlock(HASH_JOIN_THRESHOLD) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        var filter = as(inlined, Filter.class);
        var inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.list().size(), equalTo(HASH_JOIN_THRESHOLD));
    }

    // -- inlineAsHashJoin dedup correctness tests --
    //
    // These exercise the BlockHash-based dedup path introduced to replace the LinkedHashSet+Java-object
    // approach. We force the hash-join branch by passing hashJoinThreshold=0 and inspect the resulting
    // LocalRelation page directly.

    /**
     * SQL {@code x IN (a, b, NULL)} ≡ {@code x IN (a, b)} once {@code WHERE} drops NULLs.
     * {@code inlineAsHashJoin} therefore strips NULLs from the dedup right side before building
     * the LEFT join, so the runtime BlockHash lookup never even sees a NULL group on the right.
     * The left-side {@code IsNotNull(leftField)} filter (asserted separately) is the
     * complementary piece that drops NULL-keyed left rows.
     */
    public void testInlineAsHashJoinDropsNullFromSemiJoinDedupOutput() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        // 4 positions: 10, null, 20, null -> non-null filter to {10, 20} -> dedup to {10, 20}
        Block keyBlock = intBlockWithNulls(new int[] { 10, 0, 20, 0 }, new boolean[] { false, true, false, true });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.INTEGER);
        assertThat(dedup.getPositionCount(), equalTo(2));

        IntBlock keys = dedup.getBlock(0);
        for (int i = 0; i < keys.getPositionCount(); i++) {
            assertFalse("dedup right side must not contain nulls", keys.isNull(i));
        }
        assertThat(intValuesOf(dedup, 0), equalTo(new LinkedHashSet<>(List.of(10, 20))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * SQL {@code x NOT IN (..., NULL, ...)} is never TRUE — FALSE for matches, NULL otherwise —
     * so every row is filtered out. {@code inlineAsHashJoin} short-circuits ANTI + null-on-right
     * to {@code Filter(FALSE)} without invoking BlockHash or the LEFT join.
     */
    public void testInlineAsHashJoinAntiJoinWithNullOnRightShortCircuitsToFalse() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        Block keyBlock = intBlockWithNulls(new int[] { 10, 0, 20 }, new boolean[] { false, true, false });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = SemiJoin.inlineData(antiJoin, result, 0, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        assertThat(filter.condition(), equalTo(Literal.FALSE));
    }

    /**
     * SEMI with a subquery whose every right value is NULL has no candidate match key, so the
     * predicate is NULL for every left row and the whole join collapses to {@code Filter(FALSE)}.
     */
    public void testInlineAsHashJoinSemiJoinAllNullRightShortCircuitsToFalse() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = intBlockWithNulls(new int[] { 0, 0 }, new boolean[] { true, true });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        assertThat(filter.condition(), equalTo(Literal.FALSE));
    }

    /**
     * Complementing the right-side NULL stripping above, {@code inlineAsHashJoin} also wraps
     * {@code semiJoin.left()} in {@code Filter(IsNotNull(leftField))} so a NULL-keyed left row
     * can't reach the LEFT join. Together these two changes give full SQL {@code IN} semantics.
     */
    public void testInlineAsHashJoinInsertsIsNotNullOnLeftField() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = intBlock(10, 20, 30);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter sentinelFilter = as(project.child(), Filter.class);
        Join join = as(sentinelFilter.child(), Join.class);

        Filter leftFilter = as(join.left(), Filter.class);
        IsNotNull condition = as(leftFilter.condition(), IsNotNull.class);
        assertThat(condition.field(), equalTo(leftField));
    }

    /**
     * Same {@code IsNotNull(leftField)} wrapping applies to ANTI joins (when not short-circuited
     * by a NULL on the right): {@code null NOT IN (...)} is NULL ⇒ filtered out, so NULL-keyed
     * left rows must be dropped before the LEFT join.
     */
    public void testInlineAsHashJoinAntiJoinInsertsIsNotNullOnLeftField() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        Block keyBlock = intBlock(10, 20);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = SemiJoin.inlineData(antiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter sentinelFilter = as(project.child(), Filter.class);
        as(sentinelFilter.condition(), IsNull.class);
        Join join = as(sentinelFilter.child(), Join.class);

        Filter leftFilter = as(join.left(), Filter.class);
        IsNotNull condition = as(leftFilter.condition(), IsNotNull.class);
        assertThat(condition.field(), equalTo(leftField));
    }

    /**
     * All-unique input must pass through dedup unchanged. Verifies that BlockHash doesn't drop or
     * reorder rows in the absence of duplicates (modulo the fact that {@link
     * org.elasticsearch.compute.aggregation.blockhash.BlockHash#nonEmpty()} returns selected ords
     * which need not equal input order — but the multiset of keys must match).
     */
    public void testInlineAsHashJoinUniqueInputProducesUniqueOutput() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = intBlock(1, 2, 3, 4, 5);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.INTEGER);
        assertThat(dedup.getPositionCount(), equalTo(5));
        assertThat(intValuesOf(dedup, 0), equalTo(new LinkedHashSet<>(List.of(1, 2, 3, 4, 5))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * Duplicates in the subquery's key column must collapse to a single dedup row, and the constant
     * TRUE sentinel column must have the same position count. Without dedup the LEFT join would emit
     * a row per duplicate and inflate the SEMI join's output.
     */
    public void testInlineAsHashJoinDeduplicatesInteger() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        // 6 positions, 3 unique values: 10, 20, 10, 20, 30, 10
        Block keyBlock = intBlock(10, 20, 10, 20, 30, 10);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.INTEGER);
        assertThat(dedup.getPositionCount(), equalTo(3));
        assertThat(intValuesOf(dedup, 0), equalTo(new LinkedHashSet<>(List.of(10, 20, 30))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * Exercise the LongBlockHash specialization. The integer/long block hashes share the LongHash
     * primitive but build different ElementType blocks via {@code getKeys}, so this is a meaningful
     * separate test.
     */
    public void testInlineAsHashJoinDeduplicatesLong() {
        FieldAttribute leftField = getFieldAttribute("ts", DataType.LONG);
        FieldAttribute rightField = getFieldAttribute("ts", DataType.LONG);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = longBlock(100L, 200L, 100L, 300L, 200L);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.LONG);
        assertThat(dedup.getPositionCount(), equalTo(3));
        LongBlock keys = dedup.getBlock(0);
        Set<Long> seen = new LinkedHashSet<>();
        for (int i = 0; i < keys.getPositionCount(); i++) {
            seen.add(keys.getLong(keys.getFirstValueIndex(i)));
        }
        assertThat(seen, equalTo(new LinkedHashSet<>(List.of(100L, 200L, 300L))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * Exercise the DoubleBlockHash specialization. Two distinct doubles that happen to be very close
     * should not be deduped (BlockHash uses bit-level equality for doubles).
     */
    public void testInlineAsHashJoinDeduplicatesDouble() {
        FieldAttribute leftField = getFieldAttribute("height", DataType.DOUBLE);
        FieldAttribute rightField = getFieldAttribute("height", DataType.DOUBLE);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = doubleBlock(1.5, 1.7, 1.5, 1.7, 1.9);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.DOUBLE);
        assertThat(dedup.getPositionCount(), equalTo(3));
        DoubleBlock keys = dedup.getBlock(0);
        Set<Double> seen = new LinkedHashSet<>();
        for (int i = 0; i < keys.getPositionCount(); i++) {
            seen.add(keys.getDouble(keys.getFirstValueIndex(i)));
        }
        assertThat(seen, equalTo(new LinkedHashSet<>(List.of(1.5, 1.7, 1.9))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * Exercise the BytesRefBlockHash specialization. Covers KEYWORD/TEXT/IP/VERSION paths which all
     * funnel through {@link org.elasticsearch.compute.aggregation.blockhash.BytesRefBlockHash}.
     */
    public void testInlineAsHashJoinDeduplicatesKeyword() {
        FieldAttribute leftField = getFieldAttribute("name", DataType.KEYWORD);
        FieldAttribute rightField = getFieldAttribute("name", DataType.KEYWORD);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = bytesRefBlock("alice", "bob", "alice", "carol", "bob");
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.KEYWORD);
        assertThat(dedup.getPositionCount(), equalTo(3));
        BytesRefBlock keys = dedup.getBlock(0);
        Set<String> seen = new LinkedHashSet<>();
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < keys.getPositionCount(); i++) {
            BytesRef bytes = keys.getBytesRef(keys.getFirstValueIndex(i), scratch);
            seen.add(bytes.utf8ToString());
        }
        assertThat(seen, equalTo(new LinkedHashSet<>(List.of("alice", "bob", "carol"))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * ANTI join with duplicates uses the same dedup machinery but wraps the sentinel filter in
     * {@link IsNull} instead of {@link IsNotNull}. Verifies the dedup page is identical and the
     * filter condition is the ANTI variant.
     */
    public void testInlineAsHashJoinAntiJoinDedupAndIsNullFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        Block keyBlock = intBlock(10, 10, 20);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = SemiJoin.inlineData(antiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNull.class);
        Join join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));

        LocalRelation dedupRelation = as(join.right(), LocalRelation.class);
        Page dedup = ((ImmediateLocalSupplier) dedupRelation.supplier()).get();
        assertThat(dedup.getPositionCount(), equalTo(2));
        assertThat(intValuesOf(dedup, 0), equalTo(new LinkedHashSet<>(List.of(10, 20))));
        assertSentinelAllTrue(dedup);
    }

    /**
     * The dedup LocalRelation schema must have exactly the key column plus one synthetic boolean
     * sentinel ({@code $$matched}). The Project on top must drop the sentinel and the right key.
     */
    public void testInlineAsHashJoinSchemaAndProjectShape() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = intBlock(1, 2, 3);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        // Project preserves the left-side output only (no sentinel, no right-side key).
        for (var ne : project.projections()) {
            assertThat(ne.name(), not(equalTo("$$matched")));
        }
        Filter filter = as(project.child(), Filter.class);
        Join join = as(filter.child(), Join.class);
        LocalRelation dedupRelation = as(join.right(), LocalRelation.class);
        List<Attribute> schema = dedupRelation.output();
        assertThat(schema, hasSize(2));
        assertThat(schema.get(0).name(), equalTo("emp_no"));
        assertThat(schema.get(0).dataType(), equalTo(DataType.INTEGER));
        assertThat(schema.get(1).name(), equalTo("$$matched"));
        assertThat(schema.get(1).dataType(), equalTo(DataType.BOOLEAN));
    }

    /**
     * When a non-null {@code pageHolder} is passed, the hash-join path must swap it from the source
     * page to the new dedup page so {@code EsqlSession.releaseLocalRelationBlocks} releases the
     * right one at end of plan. This pins that contract.
     */
    public void testInlineAsHashJoinSwapsPageHolderToDedupPage() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = intBlock(1, 1, 2, 2, 3);
        Page sourcePage = new Page(keyBlock);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(sourcePage));

        java.util.concurrent.atomic.AtomicReference<Page> pageHolder = new java.util.concurrent.atomic.AtomicReference<>(sourcePage);
        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, pageHolder);

        Project project = as(inlined, Project.class);
        Filter filter = as(project.child(), Filter.class);
        Join join = as(filter.child(), Join.class);
        LocalRelation dedupRelation = as(join.right(), LocalRelation.class);
        Page dedupPage = ((ImmediateLocalSupplier) dedupRelation.supplier()).get();

        assertThat("pageHolder should now reference the dedup page", pageHolder.get(), sameInstance(dedupPage));
        assertThat("pageHolder should no longer reference the source page", pageHolder.get(), not(sameInstance(sourcePage)));
        assertThat(dedupPage.getPositionCount(), equalTo(3));
    }

    /**
     * The filter (below-threshold) path must release the source page eagerly via {@code pageHolder}
     * since the produced literals don't reference it. This pins that contract.
     */
    public void testInlineAsFilterReleasesSourcePage() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block keyBlock = intBlock(1, 2, 3);
        Page sourcePage = new Page(keyBlock);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(sourcePage));

        java.util.concurrent.atomic.AtomicReference<Page> pageHolder = new java.util.concurrent.atomic.AtomicReference<>(sourcePage);
        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, pageHolder);

        as(inlined, Filter.class);
        assertThat("pageHolder should be cleared after filter path consumed the page", pageHolder.get(), nullValue());
    }

    // -- subplan discovery tests --

    public void testFirstSubPlanFindsSemiJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        LogicalPlan rightPlan = emptyLocalRelation(List.of(rightField));

        SemiJoin semiJoin = new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            rightPlan,
            JoinTypes.SEMI,
            List.of(leftField),
            List.of(rightField)
        );

        var subPlan = SemiJoin.firstSubPlan(semiJoin, new HashSet<>());
        assertThat(subPlan, notNullValue());
    }

    public void testFirstSubPlanReturnsNullWithNoSemiJoin() {
        LogicalPlan plan = emptyLocalRelation(List.of(getFieldAttribute("x", DataType.INTEGER)));
        var subPlan = SemiJoin.firstSubPlan(plan, new HashSet<>());
        assertThat(subPlan, nullValue());
    }

    public void testFirstSubPlanSkipsAlreadyProcessedResults() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        LocalRelation alreadyProcessed = new LocalRelation(
            Source.EMPTY,
            List.of(rightField),
            LocalSupplier.of(new Page(BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 1, 1)))
        );

        SemiJoin semiJoin = new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            alreadyProcessed,
            JoinTypes.SEMI,
            List.of(leftField),
            List.of(rightField)
        );

        Set<LocalRelation> subPlansResults = new HashSet<>();
        subPlansResults.add(alreadyProcessed);

        var subPlan = SemiJoin.firstSubPlan(semiJoin, subPlansResults);
        assertThat(subPlan, nullValue());
    }

    // -- newMainPlan tests --

    public void testNewMainPlanReplacesSemiJoinWithFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        LogicalPlan rightPlan = emptyLocalRelation(List.of(rightField));

        SemiJoin semiJoin = new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            rightPlan,
            JoinTypes.SEMI,
            List.of(leftField),
            List.of(rightField)
        );

        var subPlan = SemiJoin.firstSubPlan(semiJoin, new HashSet<>());
        assertThat(subPlan, notNullValue());

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 42, 1) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan newPlan = SemiJoin.newMainPlan(semiJoin, subPlan, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(newPlan, Filter.class);
        assertThat(filter.condition(), instanceOf(In.class));
    }

    // -- helpers --

    private static LocalRelation emptyLocalRelation(List<org.elasticsearch.xpack.esql.core.expression.Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }

    private static SemiJoin makeSemiJoin(FieldAttribute leftField, FieldAttribute rightField, JoinType joinType) {
        if (JoinTypes.ANTI.equals(joinType)) {
            return new AntiJoin(
                Source.EMPTY,
                emptyLocalRelation(List.of(leftField)),
                emptyLocalRelation(List.of(rightField)),
                List.of(leftField),
                List.of(rightField)
            );
        }
        return new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            emptyLocalRelation(List.of(rightField)),
            joinType,
            List.of(leftField),
            List.of(rightField)
        );
    }

    /**
     * Forces the inlineAsHashJoin branch via {@code hashJoinThreshold=0}, walks the resulting plan
     * down to the dedup {@link LocalRelation}, and returns its {@link Page}. The test then asserts
     * on the dedup keys and the sentinel column directly. Centralizes plan-shape navigation so
     * the dedup-correctness tests stay focused on data.
     */
    private static Page extractHashJoinDedupPage(SemiJoin semiJoin, LocalRelation result, DataType expectedKeyType) {
        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
        Join join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
        LocalRelation dedupRelation = as(join.right(), LocalRelation.class);
        Page page = dedupRelation.supplier().get();
        assertThat("dedup page must have key column + sentinel", page.getBlockCount(), equalTo(2));
        assertThat(dedupRelation.output().get(0).dataType(), equalTo(expectedKeyType));
        return page;
    }

    /**
     * Reads non-null integer values out of {@code block} at the given column index into a
     * {@link LinkedHashSet} to allow membership comparisons that are insensitive to dedup ordering.
     * Single-valued positions only — multivalued positions wouldn't appear in our dedup output.
     */
    private static Set<Integer> intValuesOf(Page page, int channel) {
        IntBlock block = page.getBlock(channel);
        Set<Integer> values = new LinkedHashSet<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i) == false) {
                values.add(block.getInt(block.getFirstValueIndex(i)));
            }
        }
        return values;
    }

    /**
     * Asserts that the sentinel column (channel 1) is a dense all-true boolean. The sentinel must
     * never be null because the LEFT join uses it as a "matched" indicator via {@code IS NOT NULL}
     * for SEMI / {@code IS NULL} for ANTI.
     */
    private static void assertSentinelAllTrue(Page page) {
        BooleanBlock sentinel = page.getBlock(1);
        assertThat(sentinel.getPositionCount(), equalTo(page.getPositionCount()));
        assertThat("sentinel must be dense (no nulls)", sentinel.mayHaveNulls(), equalTo(false));
        for (int i = 0; i < sentinel.getPositionCount(); i++) {
            assertThat("sentinel at position " + i, sentinel.getBoolean(sentinel.getFirstValueIndex(i)), equalTo(true));
        }
    }

    private static IntBlock distinctIntBlock(int count) {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(count)) {
            for (int i = 0; i < count; i++) {
                builder.appendInt(i);
            }
            return builder.build();
        }
    }

    private static IntBlock intBlock(int... values) {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(values.length)) {
            for (int v : values) {
                builder.appendInt(v);
            }
            return builder.build();
        }
    }

    private static IntBlock intBlockWithNulls(int[] values, boolean[] isNull) {
        assert values.length == isNull.length;
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(values.length)) {
            for (int i = 0; i < values.length; i++) {
                if (isNull[i]) {
                    builder.appendNull();
                } else {
                    builder.appendInt(values[i]);
                }
            }
            return builder.build();
        }
    }

    private static LongBlock longBlock(long... values) {
        try (LongBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(values.length)) {
            for (long v : values) {
                builder.appendLong(v);
            }
            return builder.build();
        }
    }

    private static DoubleBlock doubleBlock(double... values) {
        try (DoubleBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newDoubleBlockBuilder(values.length)) {
            for (double v : values) {
                builder.appendDouble(v);
            }
            return builder.build();
        }
    }

    private static BytesRefBlock bytesRefBlock(String... values) {
        try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(values.length)) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }
}

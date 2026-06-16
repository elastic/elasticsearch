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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
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
 * Unit tests for {@link AbstractSubqueryJoin} static methods (inlineData, firstSubPlan, newMainPlan)
 * exercised through its {@link SemiJoin} / {@link AntiJoin} / {@link MarkJoin} subclasses.
 */
public class SubqueryJoinTests extends ESTestCase {

    private static final int HASH_JOIN_THRESHOLD = PlannerSettings.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getDefault(Settings.EMPTY);
    private static final BlockFactory BLOCK_FACTORY = TestBlockFactory.getNonBreakingInstance();

    // -- inlineData tests --

    public void testInlineDataProducesFilterWithIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // 3 distinct values so the dedup-first filter path produces an IN list of size 3.
        Block[] blocks = new Block[] { intBlock(1, 2, 3) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
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

        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // Many duplicates of one value: raw count > threshold, dedup count == 1.
        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(inlined, Filter.class);
        var inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.list().size(), equalTo(1));
    }

    public void testInlineDataAntiJoinProducesNotIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        AntiJoin antiJoin = antiJoin(leftField, rightField);

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, 2) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(antiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(inlined, Filter.class);
        var not = as(filter.condition(), Not.class);
        as(not.field(), In.class);
    }

    public void testInlineDataEmptyResult() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        LocalRelation emptyResult = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(0)));

        // SEMI with empty result -> FALSE
        SemiJoin semiJoin = semiJoin(leftField, rightField);
        var inlined = as(AbstractSubqueryJoin.inlineData(semiJoin, emptyResult, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.FALSE));

        // ANTI with empty result -> TRUE
        AntiJoin antiJoin = antiJoin(leftField, rightField);
        inlined = as(AbstractSubqueryJoin.inlineData(antiJoin, emptyResult, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.TRUE));
    }

    // -- hash join threshold tests --

    public void testInlineDataSemiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // Distinct values so dedup count exceeds the threshold and the hash-join branch is taken.
        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { distinctIntBlock(count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    public void testInlineDataAntiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        AntiJoin antiJoin = antiJoin(leftField, rightField);

        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { distinctIntBlock(count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(antiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    public void testInlineDataAtThresholdStillUsesFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // Distinct values: dedup count == threshold, still uses filter path.
        Block[] blocks = new Block[] { distinctIntBlock(HASH_JOIN_THRESHOLD) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

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
        AntiJoin antiJoin = antiJoin(leftField, rightField);

        Block keyBlock = intBlockWithNulls(new int[] { 10, 0, 20 }, new boolean[] { false, true, false });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(antiJoin, result, 0, BLOCK_FACTORY, null);
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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        Block keyBlock = intBlockWithNulls(new int[] { 0, 0 }, new boolean[] { true, true });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        assertThat(filter.condition(), equalTo(Literal.FALSE));
    }

    /**
     * Complementing the right-side NULL stripping above, {@code inlineAsHashJoin} wraps
     * {@code semiJoin.left()} in {@code Eval(svKey = MvSingleValueOrNull(leftField))} followed by
     * {@code Filter(IsNotNull(svKey))}. The Eval folds multi-valued positions to NULL (with the
     * standard "single-value function encountered multi-value" warning) and the Filter drops both
     * NULL-keyed and MV-keyed left rows uniformly — matching the filter path's three-valued
     * {@link In} semantics (where {@code null IN (...)} and {@code mv IN (...)} are both NULL,
     * treated as FALSE under WHERE).
     */
    public void testInlineAsHashJoinInsertsSvGuardOnLeftField() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        Block keyBlock = intBlock(10, 20, 30);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter sentinelFilter = as(project.child(), Filter.class);
        Join join = as(sentinelFilter.child(), Join.class);

        Filter svKeyFilter = as(join.left(), Filter.class);
        IsNotNull svKeyNotNull = as(svKeyFilter.condition(), IsNotNull.class);
        Eval svGuardEval = as(svKeyFilter.child(), Eval.class);
        assertThat(svGuardEval.fields(), hasSize(1));
        MvSingleValueOrNull svKey = as(svGuardEval.fields().get(0).child(), MvSingleValueOrNull.class);
        assertThat(svKey.field(), equalTo(leftField));
        // The IsNotNull guard is on the SV-guarded attribute, not the raw leftField.
        assertThat(svKeyNotNull.field(), equalTo(svGuardEval.fields().get(0).toAttribute()));
        // The LEFT join also joins on the SV-guarded attribute.
        assertThat(join.config().leftFields(), equalTo(List.of(svGuardEval.fields().get(0).toAttribute())));
    }

    /**
     * Same {@code Eval(svKey) + Filter(IsNotNull(svKey))} wrapping applies to ANTI joins (when
     * not short-circuited by a NULL on the right): {@code null NOT IN (...)} and
     * {@code mv NOT IN (...)} are both NULL ⇒ filtered out under WHERE, so NULL-keyed and
     * MV-keyed left rows must be dropped before the LEFT join.
     */
    public void testInlineAsHashJoinAntiJoinInsertsSvGuardOnLeftField() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        AntiJoin antiJoin = antiJoin(leftField, rightField);

        Block keyBlock = intBlock(10, 20);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(antiJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter sentinelFilter = as(project.child(), Filter.class);
        as(sentinelFilter.condition(), IsNull.class);
        Join join = as(sentinelFilter.child(), Join.class);

        Filter svKeyFilter = as(join.left(), Filter.class);
        IsNotNull svKeyNotNull = as(svKeyFilter.condition(), IsNotNull.class);
        Eval svGuardEval = as(svKeyFilter.child(), Eval.class);
        assertThat(svGuardEval.fields(), hasSize(1));
        MvSingleValueOrNull svKey = as(svGuardEval.fields().get(0).child(), MvSingleValueOrNull.class);
        assertThat(svKey.field(), equalTo(leftField));
        assertThat(svKeyNotNull.field(), equalTo(svGuardEval.fields().get(0).toAttribute()));
        assertThat(join.config().leftFields(), equalTo(List.of(svGuardEval.fields().get(0).toAttribute())));
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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

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
     * Exercise the {@code BytesRefBlockHash} specialization. Covers KEYWORD/TEXT/IP/VERSION paths which all
     * funnel through {@code BytesRefBlockHash}.
     */
    public void testInlineAsHashJoinDeduplicatesKeyword() {
        FieldAttribute leftField = getFieldAttribute("name", DataType.KEYWORD);
        FieldAttribute rightField = getFieldAttribute("name", DataType.KEYWORD);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

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
     * filter condition is the ANTI variant; {@link #extractHashJoinDedupPage} picks the right
     * sentinel-condition class via an {@code instanceof AntiJoin} check.
     */
    public void testInlineAsHashJoinAntiJoinDedupAndIsNullFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        AntiJoin antiJoin = antiJoin(leftField, rightField);

        Block keyBlock = intBlock(10, 10, 20);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(antiJoin, result, DataType.INTEGER);
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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        Block keyBlock = intBlock(1, 2, 3);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, null);
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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        Block keyBlock = intBlock(1, 1, 2, 2, 3);
        Page sourcePage = new Page(keyBlock);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(sourcePage));

        java.util.concurrent.atomic.AtomicReference<Page> pageHolder = new java.util.concurrent.atomic.AtomicReference<>(sourcePage);
        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, 0, BLOCK_FACTORY, pageHolder);

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
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        Block keyBlock = intBlock(1, 2, 3);
        Page sourcePage = new Page(keyBlock);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(sourcePage));

        java.util.concurrent.atomic.AtomicReference<Page> pageHolder = new java.util.concurrent.atomic.AtomicReference<>(sourcePage);
        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, pageHolder);

        as(inlined, Filter.class);
        assertThat("pageHolder should be cleared after filter path consumed the page", pageHolder.get(), nullValue());
    }

    // -- right-side NULL / MV canonicalization tests --
    //
    // These pin the post-refactor contract: {@code SemiJoin#inlineData} no longer pre-strips
    // NULL/MV positions from the BlockHash input. Instead it converts MV positions to NULL via
    // {@link SemiJoin#convertMvPositionsToNull} and lets BlockHash collapse every NULL (original
    // or MV-derived) into its reserved group 0, which surfaces as a single NULL position at index
    // 0 of the dedup output. The filter path keeps that NULL position as a NULL Literal in the
    // {@code In} list; the hash-join path strips it before the {@link LocalRelation} so the
    // runtime {@code BlockHash} can't match {@code null = null}; ANTI short-circuits on any NULL.

    /**
     * SEMI filter path with a pure NULL on the right: BlockHash collapses the NULL to its
     * reserved group 0 → dedup output has a single NULL position at index 0 → the IN list
     * built from the dedup output contains a NULL literal alongside the distinct values. We
     * keep the NULL even though {@code x IN (a, b, NULL)} ≡ {@code x IN (a, b)} under WHERE
     * (in case the surrounding rewrite ever wants the unfiltered three-valued result, e.g. via
     * MARK's mark expression).
     */
    public void testInlineDataFilterPathKeepsNullLiteralWhenRightHasNulls() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // Right side: 10, null, 20 -> BlockHash dedup output: {null, 10, 20}
        Block keyBlock = intBlockWithNulls(new int[] { 10, 0, 20 }, new boolean[] { false, true, false });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        In inExpr = as(filter.condition(), In.class);
        assertThat("two distinct values + one NULL literal", inExpr.list(), hasSize(3));

        boolean hasNullLiteral = false;
        Set<Integer> nonNullValues = new LinkedHashSet<>();
        for (Expression e : inExpr.list()) {
            Literal lit = (Literal) e;
            if (lit.value() == null) {
                hasNullLiteral = true;
            } else {
                nonNullValues.add((Integer) lit.value());
            }
        }
        assertTrue("IN list must carry the BlockHash NULL group as a NULL literal", hasNullLiteral);
        assertThat(nonNullValues, equalTo(new LinkedHashSet<>(List.of(10, 20))));
    }

    /**
     * SEMI filter path with a multi-valued position on the right: {@code AbstractSubqueryJoin#convertMvPositionsToNull}
     * folds the MV to NULL before BlockHash, so the MV's element values ({@code 20, 30} here) are
     * <strong>not</strong> matchable — the dedup output carries the MV as a single NULL position,
     * mirroring the left-side {@code MvSingleValueOrNull} guard. This is the core right-side
     * contribution to the {@code In}-operator parity between filter and hash-join paths.
     */
    public void testInlineDataFilterPathConvertsMvOnRightToSingleNullLiteral() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // Right side: 10, [20, 30] (MV), 40 -> after MV→NULL: 10, null, 40 -> dedup: {null, 10, 40}
        Block keyBlock = intBlockMv(new int[] { 10 }, new int[] { 20, 30 }, new int[] { 40 });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        In inExpr = as(filter.condition(), In.class);
        assertThat("MV folds to a single NULL, not to its expanded values", inExpr.list(), hasSize(3));

        boolean hasNullLiteral = false;
        Set<Integer> nonNullValues = new LinkedHashSet<>();
        for (Expression e : inExpr.list()) {
            Literal lit = (Literal) e;
            if (lit.value() == null) {
                hasNullLiteral = true;
            } else {
                nonNullValues.add((Integer) lit.value());
            }
        }
        assertTrue("MV → NULL must surface as a NULL literal in the IN list", hasNullLiteral);
        assertThat("only the SV positions contribute matchable values", nonNullValues, equalTo(new LinkedHashSet<>(List.of(10, 40))));
        assertFalse("MV element 20 must not become a matchable literal", nonNullValues.contains(20));
        assertFalse("MV element 30 must not become a matchable literal", nonNullValues.contains(30));
    }

    /**
     * BlockHash collapses <em>all</em> NULL inputs into the single group 0 — multiple original
     * NULLs plus MV-derived NULLs all share that one group. The dedup output therefore contains
     * at most one NULL position regardless of how many NULL/MV positions were on the right side.
     * This is what lets us key {@code rightHadNulls} off {@code dedupKeys[0].isNull(0)}.
     */
    public void testInlineDataFilterPathCollapsesMultipleNullsAndMvToSingleNullLiteral() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // 10, null, MV[20,30], null, 40, MV[50,60] -> after MV→NULL: 10, null, null, null, 40, null
        // BlockHash collapses every null to group 0 -> dedup: {null, 10, 40}
        Block keyBlock = intBlockMv(new int[] { 10 }, null, new int[] { 20, 30 }, null, new int[] { 40 }, new int[] { 50, 60 });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        In inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.list(), hasSize(3));

        int nullCount = 0;
        Set<Integer> nonNullValues = new LinkedHashSet<>();
        for (Expression e : inExpr.list()) {
            Literal lit = (Literal) e;
            if (lit.value() == null) {
                nullCount++;
            } else {
                nonNullValues.add((Integer) lit.value());
            }
        }
        assertThat("BlockHash collapses every NULL/MV input into a single NULL group", nullCount, equalTo(1));
        assertThat(nonNullValues, equalTo(new LinkedHashSet<>(List.of(10, 40))));
    }

    /**
     * SEMI when every right position is multi-valued: {@code AbstractSubqueryJoin#convertMvPositionsToNull}
     * folds every position to NULL, BlockHash emits exactly one NULL group, and the post-dedup
     * "all right NULL" check ({@code dedupPositions == 1 && rightHadNulls}) fires the
     * {@code AbstractSubqueryJoin#buildShortCircuitPlan} branch, returning {@code Filter(FALSE)} without ever
     * constructing an {@link In} or a LEFT join.
     */
    public void testInlineDataAllMvRightShortCircuitsToFalseForSemi() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        Block keyBlock = intBlockMv(new int[] { 1, 2 }, new int[] { 3, 4 });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        assertThat(filter.condition(), equalTo(Literal.FALSE));
    }

    /**
     * ANTI: {@code shortCircuitOnAnyRightNull()} is true, so any NULL position in the dedup
     * output (including one produced from an MV input via {@code AbstractSubqueryJoin#convertMvPositionsToNull})
     * forces a {@code Filter(FALSE)} short-circuit. This mirrors {@code x NOT IN (..., NULL, ...)}
     * semantics — never TRUE for any row.
     */
    public void testInlineDataAntiJoinMvRightShortCircuitsToFalse() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        AntiJoin antiJoin = antiJoin(leftField, rightField);

        // Mix of SV and MV: MV → NULL → ANTI short-circuits even though there are SV values too.
        Block keyBlock = intBlockMv(new int[] { 10 }, new int[] { 20, 30 });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(antiJoin, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Filter filter = as(inlined, Filter.class);
        assertThat(filter.condition(), equalTo(Literal.FALSE));
    }

    /**
     * Hash-join path: the MV-derived NULL produced by {@code AbstractSubqueryJoin#convertMvPositionsToNull}
     * surfaces at index 0 of the dedup output, then {@code AbstractSubqueryJoin#stripFirstPosition} drops it
     * before constructing the right-side {@link LocalRelation}. The dedup page handed to the LEFT
     * join therefore has no NULL key — important because the runtime BlockHash would otherwise
     * route a NULL left key to that same NULL group and produce {@code null = null} matches.
     */
    public void testInlineAsHashJoinDropsMvDerivedNullFromDedupOutput() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        SemiJoin semiJoin = semiJoin(leftField, rightField);

        // 10, MV[1,2], 20, MV[3,4] -> MV→NULL: 10, null, 20, null -> dedup {null, 10, 20}
        // -> hash-join strips the NULL -> LocalRelation carries {10, 20} only.
        Block keyBlock = intBlockMv(new int[] { 10 }, new int[] { 1, 2 }, new int[] { 20 }, new int[] { 3, 4 });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Page dedup = extractHashJoinDedupPage(semiJoin, result, DataType.INTEGER);
        assertThat(dedup.getPositionCount(), equalTo(2));
        IntBlock keys = dedup.getBlock(0);
        for (int i = 0; i < keys.getPositionCount(); i++) {
            assertFalse("dedup right side must not contain MV-derived NULL", keys.isNull(i));
        }
        assertThat(intValuesOf(dedup, 0), equalTo(new LinkedHashSet<>(List.of(10, 20))));
        assertSentinelAllTrue(dedup);
    }

    // -- MarkJoin terminal-plan hook tests --
    //
    // MarkJoin overrides every terminal-plan hook on {@link SemiJoin} so the produced plan
    // materializes the mark attribute (via {@link Alias} sharing the markAttribute's NameId)
    // instead of dropping rows. Six tests, one per scenario:
    // - {@code buildEmptyRightSidePlan} -> {@code Eval(mark = FALSE)}
    // - {@code buildShortCircuitPlan} -> {@code Eval(mark = NULL)} (only when every right value is NULL)
    // - {@code buildFilterPathPlan} -> {@code Eval(mark = In(...))}, no-null + has-null
    // - {@code buildHashJoinPathPlan} -> {@code Project(Eval(mark = CASE), Join)}, no-null + has-null CASE shapes
    // The has-null filter-path test also pins {@code shortCircuitOnAnyRightNull == false} and both
    // hash-join tests pin {@code filterNullLeftKeysBeforeHashJoin == false} (no
    // {@code Filter(IsNotNull(svKey))} above the SV-guard Eval) via {@link #assertHashJoinPathShape}.

    /**
     * Empty subquery: x IN () is FALSE for every non-NULL row, NULL for NULL rows. With no NULLs
     * on the right side, the mark collapses to a constant FALSE — preserving every left row.
     */
    public void testInlineDataMarkJoinEmptyResultProducesMarkFalseEval() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        MarkJoin mj = markJoin(leftField, rightField);
        LocalRelation emptyResult = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(0)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(mj, emptyResult, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Eval eval = as(inlined, Eval.class);
        Alias markAlias = singleMarkAlias(eval, mj);
        assertThat("mark is FALSE for empty subquery", markAlias.child(), equalTo(Literal.FALSE));
    }

    /**
     * All-NULL right side: no candidate match key exists and the subquery contains NULL, so the
     * mark is NULL for every row. Reaches {@link SemiJoin#buildShortCircuitPlan} with
     * {@code allRightNull = true}; the mixed-NULL case lives in
     * {@link #testInlineDataMarkJoinFilterPathWithNullRight} to also pin
     * {@link SemiJoin#shortCircuitOnAnyRightNull()} {@code == false}.
     */
    public void testInlineDataMarkJoinAllNullRightProducesMarkNullEval() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        MarkJoin mj = markJoin(leftField, rightField);

        Block keyBlock = intBlockWithNulls(new int[] { 0, 0 }, new boolean[] { true, true });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(mj, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Eval eval = as(inlined, Eval.class);
        Alias markAlias = singleMarkAlias(eval, mj);
        Literal nullLit = as(markAlias.child(), Literal.class);
        assertThat("mark is the NULL boolean literal", nullLit.value(), nullValue());
        assertThat(nullLit.dataType(), equalTo(DataType.BOOLEAN));
    }

    /**
     * Below the hash-join threshold, no NULLs on the right: build {@code Eval(mark = In(leftField, [literals]))}.
     * {@link In}'s three-valued semantics produce the correct mark — TRUE / FALSE / NULL —
     * without any extra wrapping. The alias's NameId matches {@link MarkJoin#markAttribute()}
     * so existing references in the surrounding boolean expression keep resolving, and the Eval
     * sits directly atop {@code left()} (no SV-guard, no IsNotNull filter) — the filter path
     * relies on {@link In}'s NULL handling for MV / NULL left keys.
     */
    public void testInlineDataMarkJoinFilterPathWithoutNullRight() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        MarkJoin mj = markJoin(leftField, rightField);

        Block keyBlock = intBlock(1, 2, 3);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(mj, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Eval eval = as(inlined, Eval.class);
        In in = as(singleMarkAlias(eval, mj).child(), In.class);
        assertThat(in.value(), equalTo(leftField));
        assertThat(in.list(), hasSize(3));
        for (Expression e : in.list()) {
            assertThat("no-null filter path must contain only non-null literals", ((Literal) e).value(), notNullValue());
        }
        assertThat(eval.child(), sameInstance(mj.left()));
    }

    /**
     * Filter path with mixed NULL right side. Two things to pin:
     * <ol>
     *   <li>{@link SemiJoin#shortCircuitOnAnyRightNull()} returns false for MarkJoin, so a
     *       NULL on the right alongside non-NULL values does NOT collapse to {@code Eval(mark = NULL)} —
     *       that would lose the TRUE mark for matching rows. We still take the filter path
     *       (proved by the {@link In} below — a short-circuit would have produced a constant).</li>
     *   <li>{@code buildFilterPathPlan} iterates dedup positions verbatim and BlockHash collapses
     *       every NULL/MV-derived NULL into group 0, so the resulting IN list carries a NULL
     *       literal — required for the three-valued mark when the right side has any NULL.</li>
     * </ol>
     */
    public void testInlineDataMarkJoinFilterPathWithNullRight() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        MarkJoin mj = markJoin(leftField, rightField);

        Block keyBlock = intBlockWithNulls(new int[] { 10, 0, 20 }, new boolean[] { false, true, false });
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(mj, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Eval eval = as(inlined, Eval.class);
        In in = as(singleMarkAlias(eval, mj).child(), In.class);
        assertThat(in.value(), equalTo(leftField));
        assertThat("two distinct non-null values + one NULL literal", in.list(), hasSize(3));

        boolean hasNullLiteral = false;
        Set<Integer> nonNullValues = new LinkedHashSet<>();
        for (Expression e : in.list()) {
            Literal lit = (Literal) e;
            if (lit.value() == null) {
                hasNullLiteral = true;
            } else {
                nonNullValues.add((Integer) lit.value());
            }
        }
        assertTrue("IN list must carry the BlockHash NULL group as a NULL literal", hasNullLiteral);
        assertThat(nonNullValues, equalTo(new LinkedHashSet<>(List.of(10, 20))));
        assertThat(eval.child(), sameInstance(mj.left()));
    }

    /**
     * Hash-join path with no NULLs on the right side. The surrounding {@code Project / Eval / Join}
     * shape and the {@code filterNullLeftKeysBeforeHashJoin == false} contract (no
     * {@code Filter(IsNotNull(svKey))} above the SV-guard Eval) are pinned by
     * {@link #assertHashJoinPathShape}; this test focuses on the no-null CASE shape:
     * {@code [keyIsNull, NULL, matched, TRUE, FALSE]} — the explicit "key IS NULL ⇒ NULL" branch
     * is needed because the dedup right has no NULL group for the LEFT join to route NULL-keyed
     * left rows to.
     */
    public void testInlineDataMarkJoinHashJoinPathWithoutNullRight() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        MarkJoin mj = markJoin(leftField, rightField);

        Block keyBlock = distinctIntBlock(HASH_JOIN_THRESHOLD + 1);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Case caseExpr = assertHashJoinPathShape(mj, result);

        // WHEN keyIsNull THEN NULL WHEN matched THEN TRUE ELSE FALSE
        assertThat(caseExpr.children(), hasSize(5));
        as(caseExpr.children().get(0), IsNull.class);
        Literal nullLit = as(caseExpr.children().get(1), Literal.class);
        assertThat(nullLit.value(), nullValue());
        assertThat(nullLit.dataType(), equalTo(DataType.BOOLEAN));
        as(caseExpr.children().get(2), IsNotNull.class);
        assertThat(caseExpr.children().get(3), equalTo(Literal.TRUE));
        assertThat(caseExpr.children().get(4), equalTo(Literal.FALSE));
    }

    /**
     * Hash-join path with NULL on the right side. Same surrounding shape and
     * {@code filterNullLeftKeysBeforeHashJoin == false} contract as
     * {@link #testInlineDataMarkJoinHashJoinPathWithoutNullRight} (pinned again via
     * {@link #assertHashJoinPathShape}); the CASE collapses to the 2-child form
     * {@code [matched, TRUE]} because the absent ELSE clause already yields the implicit NULL
     * mark for both non-matches and NULL-keyed left rows (their sentinel is NULL too).
     */
    public void testInlineDataMarkJoinHashJoinPathWithNullRight() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        MarkJoin mj = markJoin(leftField, rightField);

        // One NULL plus enough distinct non-null values to exceed the threshold so we take the
        // hash-join path with rightHadNulls = true.
        int count = HASH_JOIN_THRESHOLD + 1;
        int[] values = new int[count + 1];
        boolean[] nulls = new boolean[count + 1];
        values[0] = 0;
        nulls[0] = true;
        for (int i = 0; i < count; i++) {
            values[i + 1] = i;
            nulls[i + 1] = false;
        }
        Block keyBlock = intBlockWithNulls(values, nulls);
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(keyBlock)));

        Case caseExpr = assertHashJoinPathShape(mj, result);

        // WHEN matched THEN TRUE (no else — implicit NULL on no-match)
        assertThat(caseExpr.children(), hasSize(2));
        as(caseExpr.children().get(0), IsNotNull.class);
        assertThat(caseExpr.children().get(1), equalTo(Literal.TRUE));
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
            List.of(leftField),
            List.of(rightField)
        );

        var subPlan = AbstractSubqueryJoin.firstSubPlan(semiJoin, new HashSet<>());
        assertThat(subPlan, notNullValue());
    }

    public void testFirstSubPlanReturnsNullWithNoSemiJoin() {
        LogicalPlan plan = emptyLocalRelation(List.of(getFieldAttribute("x", DataType.INTEGER)));
        var subPlan = AbstractSubqueryJoin.firstSubPlan(plan, new HashSet<>());
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
            List.of(leftField),
            List.of(rightField)
        );

        Set<LocalRelation> subPlansResults = new HashSet<>();
        subPlansResults.add(alreadyProcessed);

        var subPlan = AbstractSubqueryJoin.firstSubPlan(semiJoin, subPlansResults);
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
            List.of(leftField),
            List.of(rightField)
        );

        var subPlan = AbstractSubqueryJoin.firstSubPlan(semiJoin, new HashSet<>());
        assertThat(subPlan, notNullValue());

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 42, 1) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan newPlan = AbstractSubqueryJoin.newMainPlan(semiJoin, subPlan, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        var filter = as(newPlan, Filter.class);
        assertThat(filter.condition(), instanceOf(In.class));
    }

    // -- helpers --

    private static LocalRelation emptyLocalRelation(List<org.elasticsearch.xpack.esql.core.expression.Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }

    private static MarkJoin markJoin(FieldAttribute leftField, FieldAttribute rightField) {
        Attribute markAttribute = new ReferenceAttribute(
            Source.EMPTY,
            null,
            "$$mark",
            DataType.BOOLEAN,
            Nullability.TRUE,
            new NameId(),
            true
        );
        return new MarkJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            emptyLocalRelation(List.of(rightField)),
            List.of(leftField),
            List.of(rightField),
            markAttribute
        );
    }

    /**
     * Reads the single {@link Alias} produced by a MarkJoin terminal Eval and asserts that
     * its NameId matches {@link MarkJoin#markAttribute()}'s — the contract that lets the
     * surrounding boolean expression keep resolving against the mark.
     */
    private static Alias singleMarkAlias(Eval eval, MarkJoin mj) {
        assertThat(eval.fields(), hasSize(1));
        Alias markAlias = eval.fields().get(0);
        assertThat("mark alias must share markAttribute's NameId", markAlias.id(), equalTo(mj.markAttribute().id()));
        return markAlias;
    }

    /**
     * Walks a MarkJoin hash-join plan and pins every invariant that does not depend on the
     * specific CASE shape, then returns the {@link Case} for the caller to assert on:
     * <ul>
     *   <li>top-level {@link Project} whose projections include {@link MarkJoin#markAttribute()}'s NameId
     *       (so the mark is in scope for downstream Filter/Project nodes);</li>
     *   <li>{@link Eval} with a single mark alias under {@link #singleMarkAlias};</li>
     *   <li>{@link Join} with {@link JoinTypes#LEFT};</li>
     *   <li>{@code join.right()} is the dedup {@link LocalRelation};</li>
     *   <li>{@code join.left()} is the SV-guard {@link Eval} directly — NOT wrapped in
     *       {@code Filter(IsNotNull(svKey))} (this is the {@link MarkJoin#filterNullLeftKeysBeforeHashJoin}
     *       {@code == false} contract; SEMI / ANTI would have wrapped it via that hook).</li>
     * </ul>
     * Forces the hash-join path via {@code hashJoinThreshold = 0} — callers can use a small dedup
     * block and don't need to construct above-threshold data themselves.
     */
    private static Case assertHashJoinPathShape(MarkJoin mj, LocalRelation result) {
        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(mj, result, HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        boolean hasMarkAttr = project.projections().stream().anyMatch(ne -> ne.id().equals(mj.markAttribute().id()));
        assertTrue("projection must include the mark attribute", hasMarkAttr);

        Eval eval = as(project.child(), Eval.class);
        Alias markAlias = singleMarkAlias(eval, mj);
        Case caseExpr = as(markAlias.child(), Case.class);

        Join join = as(eval.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
        as(join.right(), LocalRelation.class);
        // filterNullLeftKeysBeforeHashJoin() == false: the SV-guard Eval is here directly,
        // not wrapped in Filter(IsNotNull(svKey)) — that's what lets NULL-keyed and MV-keyed
        // left rows survive so the CASE can assign them mark = NULL.
        Eval svGuardEval = as(join.left(), Eval.class);
        assertThat(svGuardEval.fields(), hasSize(1));

        return caseExpr;
    }

    private static SemiJoin semiJoin(FieldAttribute leftField, FieldAttribute rightField) {
        return new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            emptyLocalRelation(List.of(rightField)),
            List.of(leftField),
            List.of(rightField)
        );
    }

    private static AntiJoin antiJoin(FieldAttribute leftField, FieldAttribute rightField) {
        return new AntiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            emptyLocalRelation(List.of(rightField)),
            List.of(leftField),
            List.of(rightField)
        );
    }

    /**
     * Forces the inlineAsHashJoin branch via {@code hashJoinThreshold=0}, walks the resulting plan
     * down to the dedup {@link LocalRelation}, and returns its {@link Page}. The test then asserts
     * on the dedup keys and the sentinel column directly. Centralizes plan-shape navigation so
     * the dedup-correctness tests stay focused on data.
     * <p>
     * The expected sentinel-filter class is derived from whether the join is an {@link AntiJoin}:
     * {@link IsNotNull} for SEMI / MARK, {@link IsNull} for ANTI.
     */
    private static Page extractHashJoinDedupPage(AbstractSubqueryJoin subqueryJoin, LocalRelation result, DataType expectedKeyType) {
        Class<? extends Expression> expectedSentinelCondition = subqueryJoin instanceof AntiJoin ? IsNull.class : IsNotNull.class;
        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(subqueryJoin, result, 0, BLOCK_FACTORY, null);
        Project project = as(inlined, Project.class);
        Filter filter = as(project.child(), Filter.class);
        as(filter.condition(), expectedSentinelCondition);
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

    /**
     * Builds an {@link IntBlock} where each varargs entry describes one position: {@code null}
     * encodes a NULL position, a single-element array encodes a single-valued position, and a
     * multi-element array encodes a multi-valued position. Convenience for tests that need to
     * mix SV, NULL, and MV positions in a single right-side block — see the right-side MV / NULL
     * canonicalization tests above.
     */
    private static IntBlock intBlockMv(int[]... positions) {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(positions.length)) {
            for (int[] values : positions) {
                if (values == null) {
                    builder.appendNull();
                } else if (values.length == 1) {
                    builder.appendInt(values[0]);
                } else {
                    builder.beginPositionEntry();
                    for (int v : values) {
                        builder.appendInt(v);
                    }
                    builder.endPositionEntry();
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

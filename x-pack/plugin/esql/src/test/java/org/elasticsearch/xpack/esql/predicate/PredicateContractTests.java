/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.esql.predicate.PredicateCapability.unsupported;
import static org.elasticsearch.xpack.esql.predicate.PredicateExpr.FieldEquals;
import static org.elasticsearch.xpack.esql.predicate.PredicateExpr.ProductGreaterThan;
import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.all;
import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.tri;
import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.values;
import static org.hamcrest.Matchers.equalTo;

/**
 * The engine's Boolean contract, checked against an exact iterator source and a superset row-group source. Those stand in
 * for Lucene and Parquet: each produces candidates differently, and both must accept the same rows. Every loaded column
 * is a {@code Block}, every result is a three-valued {@code BooleanBlock}, and the base class fails a test that leaves a
 * block unreleased.
 */
public class PredicateContractTests extends ComputeTestCase {

    private static final PredicateExpr CATEGORY = new FieldEquals("category", "electronics");
    private static final PredicateExpr EXPENSIVE = new ProductGreaterThan("price", "quantity", 100);
    private static final PredicateExpr MIXED = new PredicateExpr.And(CATEGORY, EXPENSIVE);

    private BlockFactory factory;
    private DriverContext context;

    @Before
    public void createContext() {
        factory = blockFactory();
        context = new DriverContext(nonBreakingBigArrays(), factory, null);
    }

    public void testExactAndSupersetSourcesAcceptTheSameRows() {
        List<Row> rows = List.of(
            new Row("electronics", 20L, 10L),
            new Row("books", 100L, 100L),
            new Row("books", 1L, 1L),
            new Row("books", 1L, 1L)
        );
        ExactIteratorSource exact = new ExactIteratorSource(rows);
        StatsPruningSource pruned = new StatsPruningSource(rows);
        FullScanSource scan = new FullScanSource(rows);

        assertThat(accepted(MIXED, exact), equalTo(List.of(exact.row(0))));
        assertThat(accepted(MIXED, pruned), equalTo(List.of(pruned.row(0))));
        assertThat(accepted(MIXED, scan), equalTo(List.of(scan.row(0))));

        assertThat(exact.yielded, equalTo(List.of(0)));
        assertThat(pruned.yielded, equalTo(List.of(0, 1)));
        assertThat(scan.yielded, equalTo(List.of(0, 1, 2, 3)));
    }

    public void testExactConstraintDoesNotLoadTheConstrainedField() {
        ExactIteratorSource source = new ExactIteratorSource(sampleRows());
        try (FilterPlan plan = PredicatePlans.filter(MIXED, source, context)) {
            assertThat(plan.constraint().predicates(), equalTo(List.of(CATEGORY)));
            assertThat(plan.coverage(), equalTo(CandidateCoverage.SUPERSET));
            assertThat(plan.projection().fields(), equalTo(List.of(PRICE, QUANTITY)));
            plan.acceptedRows();
        }
        assertThat(source.loads, equalTo(List.of("price:0", "quantity:0")));
    }

    public void testSupersetConstraintKeepsThePredicateForVerification() {
        StatsPruningSource source = new StatsPruningSource(sampleRows());
        try (FilterPlan plan = PredicatePlans.filter(MIXED, source, context)) {
            assertThat(plan.constraint().predicates(), equalTo(List.of(CATEGORY)));
            assertThat(plan.coverage(), equalTo(CandidateCoverage.SUPERSET));
            assertThat(plan.projection().fields(), equalTo(List.of(CATEGORY_FIELD, PRICE, QUANTITY)));
            plan.acceptedRows();
        }
        assertThat(source.loads, equalTo(List.of("category:0", "category:1", "price:0", "quantity:0")));
    }

    public void testDisjunctionIsNotSplitIntoOneBranch() {
        FullScanSource source = new FullScanSource(sampleRows());
        PredicateExpr either = new PredicateExpr.Or(CATEGORY, EXPENSIVE);
        try (FilterPlan plan = PredicatePlans.filter(either, source, context)) {
            assertThat(plan.constraint(), equalTo(CandidateConstraint.none()));
            assertThat(plan.coverage(), equalTo(CandidateCoverage.COMPLETE));
            assertThat(plan.acceptedRows(), equalTo(List.of(source.row(0), source.row(1))));
        }
        assertThat(
            source.loads,
            equalTo(
                List.of(
                    "category:0",
                    "category:1",
                    "category:2",
                    "category:3",
                    "price:1",
                    "price:2",
                    "price:3",
                    "quantity:1",
                    "quantity:2",
                    "quantity:3"
                )
            )
        );
    }

    public void testEvalKeepsFalseAndNullRows() {
        MemoryBatch batch = MemoryBatch.of(
            factory,
            new int[] { 0, 1, 2 },
            Map.of(
                "category",
                new Object[] { "electronics", null, "books" },
                "price",
                new Object[] { 1L, 1L, 1L },
                "quantity",
                new Object[] { 1L, 1L, 1L }
            ),
            new ArrayList<>()
        );
        try (
            EvalPlan plan = PredicatePlans.eval(CATEGORY, new ExactIteratorSource(List.of()), context);
            BooleanBlock result = plan.evaluate(batch)
        ) {
            assertThat(values(result), equalTo(tri(true, null, false)));
        }
        assertThat(batch.positionCount(), equalTo(3));
    }

    public void testNotPreservesNull() {
        MemoryBatch batch = MemoryBatch.of(
            factory,
            new int[] { 0, 1, 2 },
            Map.of("category", new Object[] { "electronics", null, "books" }),
            new ArrayList<>()
        );
        try (
            EvalPlan plan = PredicatePlans.eval(new PredicateExpr.Not(CATEGORY), new FullScanSource(List.of()), context);
            BooleanBlock result = plan.evaluate(batch)
        ) {
            assertThat(values(result), equalTo(tri(false, null, true)));
        }
    }

    public void testAndNullStillEvaluatesTheRightSide() {
        MemoryBatch batch = MemoryBatch.of(
            factory,
            new int[] { 0 },
            Map.of("category", new Object[] { null }, "price", new Object[] { 1L }, "quantity", new Object[] { 1L }),
            new ArrayList<>()
        );
        try (
            RowPredicate predicate = Predicates.compile(MIXED, new FullScanSource(List.of()), context);
            BooleanBlock result = predicate.evaluate(batch, all(1))
        ) {
            assertThat(values(result), equalTo(tri(false)));
        }
        assertThat(batch.loads, equalTo(List.of("category:0", "price:0", "quantity:0")));
    }

    public void testNullProductIsNotAccepted() {
        List<Row> rows = List.of(new Row("electronics", null, 10L));
        assertThat(accepted(MIXED, new FullScanSource(rows)), equalTo(List.of()));
    }

    public void testNestedAnyKeepsSameChildAcrossBatches() {
        // Parent 0 has the sku on one child and the expensive product on another. Parent 1's match arrives in a later batch.
        MemoryBatch first = sameChildBatchOne();
        MemoryBatch second = sameChildBatchTwo();
        AnyReduction reduction = new AnyReduction(factory, 2);
        try (RowPredicate child = Predicates.compile(sameChild(), new FullScanSource(List.of()), context)) {
            add(reduction, first, child);
            try (BooleanBlock known = reduction.knownTrues()) {
                assertThat(values(known), equalTo(tri(false, false)));
            }
            add(reduction, second, child);
            try (BooleanBlock known = reduction.knownTrues()) {
                assertThat(values(known), equalTo(tri(false, true)));
            }
            try (BooleanBlock parents = reduction.seal()) {
                assertThat(values(parents), equalTo(tri(false, true)));
            }
        }
    }

    public void testNestedPlanSealsWhenTheChildCursorEnds() {
        try (
            RowPredicate child = Predicates.compile(sameChild(), new FullScanSource(List.of()), context);
            BooleanBlock parents = NestedAnyPlan.evaluate(factory, 2, pages(sameChildBatchOne(), sameChildBatchTwo()), child)
        ) {
            assertThat(values(parents), equalTo(tri(false, true)));
        }
    }

    public void testCategoryAloneIsAnExactTrueSet() {
        ExactIteratorSource source = new ExactIteratorSource(
            List.of(new Row("electronics", 1L, 1L), new Row(null, 1L, 1L), new Row("books", 1L, 1L))
        );
        try (FilterPlan plan = PredicatePlans.filter(CATEGORY, source, context)) {
            assertThat(plan.coverage(), equalTo(CandidateCoverage.EXACT));
            assertThat(plan.acceptedRows(), equalTo(List.of(source.row(0))));
        }
    }

    public void testNotIsAnalyzedAsAWholeAndKeepsNull() {
        List<Row> rows = List.of(new Row("electronics", 1L, 1L), new Row(null, 1L, 1L), new Row("books", 1L, 1L));
        ExactIteratorSource source = new ExactIteratorSource(rows);
        try (FilterPlan plan = PredicatePlans.filter(new PredicateExpr.Not(CATEGORY), source, context)) {
            assertThat(source.analyzed.getFirst(), equalTo(new PredicateExpr.Not(CATEGORY)));
            assertThat(plan.constraint(), equalTo(CandidateConstraint.none()));
            assertThat(plan.coverage(), equalTo(CandidateCoverage.COMPLETE));
            assertThat(plan.acceptedRows(), equalTo(List.of(source.row(2))));
        }
    }

    public void testTrueOnlyProducerDoesNotEraseNullOnEval() {
        MemoryBatch batch = MemoryBatch.of(
            factory,
            new int[] { 0, 1 },
            Map.of("category", new Object[] { "electronics", null }),
            new ArrayList<>()
        );
        try (EvalPlan plan = PredicatePlans.eval(CATEGORY, new TrueOnlySource(), context); BooleanBlock result = plan.evaluate(batch)) {
            assertThat(values(result), equalTo(tri(true, null)));
        }
    }

    public void testOrKeepsNullWhenTheRightSideIsFalse() {
        MemoryBatch batch = MemoryBatch.of(
            factory,
            new int[] { 0, 1, 2 },
            Map.of(
                "category",
                new Object[] { "electronics", null, "books" },
                "price",
                new Object[] { 1L, 1L, 1L },
                "quantity",
                new Object[] { 1L, 1L, 1L }
            ),
            new ArrayList<>()
        );
        try (
            EvalPlan plan = PredicatePlans.eval(new PredicateExpr.Or(CATEGORY, EXPENSIVE), new FullScanSource(List.of()), context);
            BooleanBlock result = plan.evaluate(batch)
        ) {
            assertThat(values(result), equalTo(tri(true, null, false)));
        }
        assertThat(
            batch.loads,
            equalTo(List.of("category:0", "category:1", "category:2", "price:1", "price:2", "quantity:1", "quantity:2"))
        );
    }

    public void testASelectionOnlyReadsItsOwnPositions() {
        MemoryBatch batch = MemoryBatch.of(
            factory,
            new int[] { 10, 11, 12, 13 },
            Map.of("category", new Object[] { "electronics", "electronics", null, "books" }),
            new ArrayList<>()
        );
        try (
            RowPredicate predicate = Predicates.compile(CATEGORY, new FullScanSource(List.of()), context);
            BooleanBlock result = predicate.evaluate(batch, new int[] { 1, 2 })
        ) {
            assertThat(values(result), equalTo(tri(true, null)));
        }
        assertThat(batch.loads, equalTo(List.of("category:11", "category:12")));
    }

    private List<SourceRow> accepted(PredicateExpr predicate, PredicateSource source) {
        try (FilterPlan plan = PredicatePlans.filter(predicate, source, context)) {
            return plan.acceptedRows();
        }
    }

    private static void add(AnyReduction reduction, ChildBatch batch, RowPredicate predicate) {
        int[] selection = batch.candidates();
        try (BooleanBlock result = predicate.evaluate(batch, selection)) {
            reduction.add(batch, selection, result);
        }
    }

    private static PredicateExpr sameChild() {
        return new PredicateExpr.And(new FieldEquals("sku", "a"), new ProductGreaterThan("qty", "price", 100));
    }

    private MemoryBatch sameChildBatchOne() {
        return MemoryBatch.children(
            factory,
            new int[] { 0, 1, 2 },
            new int[] { 0, 0, 1 },
            Map.of("sku", new Object[] { "a", "b", "a" }, "qty", new Object[] { 1L, 50L, 1L }, "price", new Object[] { 10L, 50L, 1L })
        );
    }

    private MemoryBatch sameChildBatchTwo() {
        return MemoryBatch.children(
            factory,
            new int[] { 3 },
            new int[] { 1 },
            Map.of("sku", new Object[] { "a" }, "qty", new Object[] { 10L }, "price", new Object[] { 20L })
        );
    }

    private static CandidateCursor pages(ChildBatch... batches) {
        return new CandidateCursor() {
            private int next;

            @Override
            public SelectionBatch nextBatch() {
                return next < batches.length ? batches[next++] : null;
            }
        };
    }

    private static List<Row> sampleRows() {
        return List.of(new Row("electronics", 20L, 10L), new Row("books", 100L, 100L), new Row("books", 1L, 1L), new Row("books", 1L, 1L));
    }

    private static final FieldRef CATEGORY_FIELD = new FieldRef("category", DataType.KEYWORD);
    private static final FieldRef PRICE = new FieldRef("price", DataType.LONG);
    private static final FieldRef QUANTITY = new FieldRef("quantity", DataType.LONG);

    private record Row(String category, Long price, Long quantity) {}

    private static final class MemoryBatch implements ChildBatch {
        private final BlockFactory factory;
        private final RowSpace space;
        private final int[] rowIds;
        private final int[] parents;
        private final Map<String, Object[]> values;
        private final List<String> loads;

        private MemoryBatch(
            BlockFactory factory,
            RowSpace space,
            int[] rowIds,
            int[] parents,
            Map<String, Object[]> values,
            List<String> loads
        ) {
            this.factory = factory;
            this.space = space;
            this.rowIds = rowIds;
            this.parents = parents;
            this.values = values;
            this.loads = loads;
        }

        private static MemoryBatch of(BlockFactory factory, int[] rowIds, Map<String, Object[]> values, List<String> loads) {
            return new MemoryBatch(factory, new RowSpace("batch"), rowIds, null, values, loads);
        }

        private static MemoryBatch select(BlockFactory factory, RowSpace space, List<Row> rows, List<Integer> ids, List<String> loads) {
            int count = ids.size();
            int[] rowIds = new int[count];
            Object[] category = new Object[count];
            Object[] price = new Object[count];
            Object[] quantity = new Object[count];
            for (int i = 0; i < count; i++) {
                rowIds[i] = ids.get(i);
                Row row = rows.get(ids.get(i));
                category[i] = row.category();
                price[i] = row.price();
                quantity[i] = row.quantity();
            }
            return new MemoryBatch(factory, space, rowIds, null, Map.of("category", category, "price", price, "quantity", quantity), loads);
        }

        private static MemoryBatch children(BlockFactory factory, int[] rowIds, int[] parents, Map<String, Object[]> values) {
            return new MemoryBatch(factory, new RowSpace("children"), rowIds, parents, values, new ArrayList<>());
        }

        @Override
        public int positionCount() {
            return rowIds.length;
        }

        @Override
        public int[] candidates() {
            return all(rowIds.length);
        }

        @Override
        public Block load(FieldRef field, int[] selection) {
            for (int position : selection) {
                loads.add(field.name() + ":" + rowIds[position]);
            }
            return PredicateTestBlocks.column(factory, field, values.get(field.name()), selection);
        }

        @Override
        public RowSpace rowSpace() {
            return space;
        }

        @Override
        public SourceRow sourceRow(int position) {
            return new SourceRow(space, rowIds[position]);
        }

        @Override
        public int parentPosition(int position) {
            return parents[position];
        }
    }

    /** Produces exactly the rows whose category matches, the way a Lucene doc-id iterator would. */
    private final class ExactIteratorSource implements PredicateSource {
        private final List<Row> rows;
        private final RowSpace space = new RowSpace("exact");
        private final List<Integer> yielded = new ArrayList<>();
        private final List<String> loads = new ArrayList<>();
        private final List<PredicateExpr> analyzed = new ArrayList<>();

        private ExactIteratorSource(List<Row> rows) {
            this.rows = rows;
        }

        @Override
        public PredicateCapability analyze(PredicateExpr predicate) {
            analyzed.add(predicate);
            if (predicate instanceof FieldEquals) {
                return PredicateCapability.of(
                    FilteringGuarantee.EXACT_TRUE,
                    BooleanFidelity.THREE_VALUED,
                    SourceOperation.PRODUCE,
                    SourceOperation.REFINE
                );
            }
            return unsupported();
        }

        @Override
        public CandidateCursor open(CandidateConstraint constraint, Projection projection) {
            List<Integer> ids = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                if (keeps(constraint, rows.get(i))) {
                    ids.add(i);
                }
            }
            yielded.addAll(ids);
            return once(MemoryBatch.select(factory, space, rows, ids, loads));
        }

        @Override
        public Optional<PredicateRefiner> refiner(PredicateExpr predicate) {
            if (predicate instanceof FieldEquals equals) {
                return Optional.of(new FieldEqualsRefiner(equals));
            }
            return Optional.empty();
        }

        private SourceRow row(int id) {
            return new SourceRow(space, id);
        }

        private static boolean keeps(CandidateConstraint constraint, Row row) {
            for (PredicateExpr predicate : constraint.predicates()) {
                if (predicate instanceof FieldEquals equals && equals.value().equals(row.category()) == false) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Drops a row group only when its category statistics cannot contain the requested value. */
    private final class StatsPruningSource implements PredicateSource {
        private final List<Row> rows;
        private final RowSpace space = new RowSpace("stats");
        private final List<Integer> yielded = new ArrayList<>();
        private final List<String> loads = new ArrayList<>();

        private StatsPruningSource(List<Row> rows) {
            this.rows = rows;
        }

        @Override
        public PredicateCapability analyze(PredicateExpr predicate) {
            if (predicate instanceof FieldEquals) {
                return PredicateCapability.of(FilteringGuarantee.SUPERSET, BooleanFidelity.NONE, SourceOperation.PRODUCE);
            }
            return unsupported();
        }

        @Override
        public CandidateCursor open(CandidateConstraint constraint, Projection projection) {
            List<Integer> ids = new ArrayList<>();
            for (int start = 0; start < rows.size(); start += 2) {
                int end = Math.min(start + 2, rows.size());
                if (groupCanMatch(constraint, start, end)) {
                    for (int i = start; i < end; i++) {
                        ids.add(i);
                    }
                }
            }
            yielded.addAll(ids);
            return once(MemoryBatch.select(factory, space, rows, ids, loads));
        }

        @Override
        public Optional<PredicateRefiner> refiner(PredicateExpr predicate) {
            return Optional.empty();
        }

        private boolean groupCanMatch(CandidateConstraint constraint, int start, int end) {
            for (PredicateExpr predicate : constraint.predicates()) {
                if (predicate instanceof FieldEquals equals) {
                    boolean seen = false;
                    for (int i = start; i < end; i++) {
                        seen |= equals.value().equals(rows.get(i).category());
                    }
                    if (seen == false) {
                        return false;
                    }
                }
            }
            return true;
        }

        private SourceRow row(int id) {
            return new SourceRow(space, id);
        }
    }

    /** Evaluates every predicate itself. Candidate production returns every row. */
    private final class FullScanSource implements PredicateSource {
        private final List<Row> rows;
        private final RowSpace space = new RowSpace("scan");
        private final List<Integer> yielded = new ArrayList<>();
        private final List<String> loads = new ArrayList<>();

        private FullScanSource(List<Row> rows) {
            this.rows = rows;
        }

        @Override
        public PredicateCapability analyze(PredicateExpr predicate) {
            return unsupported();
        }

        @Override
        public CandidateCursor open(CandidateConstraint constraint, Projection projection) {
            List<Integer> ids = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                ids.add(i);
            }
            yielded.addAll(ids);
            return once(MemoryBatch.select(factory, space, rows, ids, loads));
        }

        @Override
        public Optional<PredicateRefiner> refiner(PredicateExpr predicate) {
            return Optional.empty();
        }

        private SourceRow row(int id) {
            return new SourceRow(space, id);
        }
    }

    /** Produces the true rows of a comparison and cannot tell false from null. */
    private static final class TrueOnlySource implements PredicateSource {
        @Override
        public PredicateCapability analyze(PredicateExpr predicate) {
            if (predicate instanceof FieldEquals) {
                return PredicateCapability.of(FilteringGuarantee.EXACT_TRUE, BooleanFidelity.TRUE_ONLY, SourceOperation.PRODUCE);
            }
            return unsupported();
        }

        @Override
        public CandidateCursor open(CandidateConstraint constraint, Projection projection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<PredicateRefiner> refiner(PredicateExpr predicate) {
            return Optional.empty();
        }
    }

    private static CandidateCursor once(SelectionBatch batch) {
        boolean[] pending = { true };
        return () -> {
            if (pending[0] == false) {
                return null;
            }
            pending[0] = false;
            return batch.positionCount() == 0 ? null : batch;
        };
    }

    private final class FieldEqualsRefiner implements PredicateRefiner {
        private final FieldEquals equals;

        private FieldEqualsRefiner(FieldEquals equals) {
            this.equals = equals;
        }

        @Override
        public BooleanBlock evaluate(SelectionBatch batch, int[] selection) {
            try (RowPredicate predicate = Predicates.compile(equals, new FullScanSource(List.of()), context)) {
                return predicate.evaluate(batch, selection);
            }
        }
    }
}

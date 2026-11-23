/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.esql.predicate.PredicateCapability.unsupported;
import static org.elasticsearch.xpack.esql.predicate.PredicateExpr.FieldEquals;
import static org.elasticsearch.xpack.esql.predicate.PredicateExpr.ProductGreaterThan;
import static org.hamcrest.Matchers.equalTo;

/**
 * The engine's Boolean contract, checked against an exact iterator source and a superset row-group source. Those stand in
 * for Lucene and Parquet: each produces candidates differently, and both must accept the same rows.
 */
public class PredicateContractTests extends ESTestCase {

    private static final PredicateExpr CATEGORY = new FieldEquals("category", "electronics");
    private static final PredicateExpr EXPENSIVE = new ProductGreaterThan("price", "quantity", 100);
    private static final PredicateExpr MIXED = new PredicateExpr.And(CATEGORY, EXPENSIVE);

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

        assertThat(PredicatePlans.filter(MIXED, exact).acceptedRows(), equalTo(List.of(exact.row(0))));
        assertThat(PredicatePlans.filter(MIXED, pruned).acceptedRows(), equalTo(List.of(pruned.row(0))));
        assertThat(PredicatePlans.filter(MIXED, scan).acceptedRows(), equalTo(List.of(scan.row(0))));

        assertThat(exact.yielded, equalTo(List.of(0)));
        assertThat(pruned.yielded, equalTo(List.of(0, 1)));
        assertThat(scan.yielded, equalTo(List.of(0, 1, 2, 3)));
    }

    public void testExactConstraintDoesNotLoadTheConstrainedField() {
        ExactIteratorSource source = new ExactIteratorSource(sampleRows());
        FilterPlan plan = PredicatePlans.filter(MIXED, source);

        assertThat(plan.constraint().predicates(), equalTo(List.of(CATEGORY)));
        assertThat(plan.coverage(), equalTo(CandidateCoverage.SUPERSET));
        assertThat(plan.projection().fields(), equalTo(List.of(new FieldRef("price"), new FieldRef("quantity"))));
        plan.acceptedRows();
        assertThat(source.loads, equalTo(List.of("price:0", "quantity:0")));
    }

    public void testSupersetConstraintKeepsThePredicateForVerification() {
        StatsPruningSource source = new StatsPruningSource(sampleRows());
        FilterPlan plan = PredicatePlans.filter(MIXED, source);

        assertThat(plan.constraint().predicates(), equalTo(List.of(CATEGORY)));
        assertThat(plan.coverage(), equalTo(CandidateCoverage.SUPERSET));
        assertThat(plan.projection().fields(), equalTo(List.of(new FieldRef("category"), new FieldRef("price"), new FieldRef("quantity"))));
        plan.acceptedRows();
        assertThat(source.loads, equalTo(List.of("category:0", "category:1", "price:0", "quantity:0")));
    }

    public void testDisjunctionIsNotSplitIntoOneBranch() {
        FullScanSource source = new FullScanSource(sampleRows());
        PredicateExpr either = new PredicateExpr.Or(CATEGORY, EXPENSIVE);
        FilterPlan plan = PredicatePlans.filter(either, source);

        assertThat(plan.constraint(), equalTo(CandidateConstraint.none()));
        assertThat(plan.coverage(), equalTo(CandidateCoverage.COMPLETE));
        assertThat(plan.acceptedRows(), equalTo(List.of(source.row(0), source.row(1))));
        assertThat(
            source.loads,
            equalTo(
                List.of(
                    "category:0",
                    "category:1",
                    "category:2",
                    "category:3",
                    "price:1",
                    "quantity:1",
                    "price:2",
                    "quantity:2",
                    "price:3",
                    "quantity:3"
                )
            )
        );
    }

    public void testEvalKeepsFalseAndNullRows() {
        MemoryBatch batch = MemoryBatch.of(
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
        PredicateResult result = PredicatePlans.eval(CATEGORY, new ExactIteratorSource(List.of())).evaluate(batch);

        assertThat(result.trues(), equalTo(SelectionMask.of(true, false, false)));
        assertThat(result.nulls(), equalTo(SelectionMask.of(false, true, false)));
        assertThat(batch.positionCount(), equalTo(3));
    }

    public void testNotPreservesNull() {
        MemoryBatch batch = MemoryBatch.of(
            new int[] { 0, 1, 2 },
            Map.of("category", new Object[] { "electronics", null, "books" }),
            new ArrayList<>()
        );
        PredicateResult result = PredicatePlans.eval(new PredicateExpr.Not(CATEGORY), new FullScanSource(List.of())).evaluate(batch);

        assertThat(result.trues(), equalTo(SelectionMask.of(false, false, true)));
        assertThat(result.nulls(), equalTo(SelectionMask.of(false, true, false)));
    }

    public void testAndNullStillEvaluatesTheRightSide() {
        MemoryBatch batch = MemoryBatch.of(
            new int[] { 0 },
            Map.of("category", new Object[] { null }, "price", new Object[] { 1L }, "quantity", new Object[] { 1L }),
            new ArrayList<>()
        );
        PredicateResult result = Predicates.compile(MIXED, new FullScanSource(List.of())).evaluate(batch, SelectionMask.all(1));

        assertThat(result.trues(), equalTo(SelectionMask.of(false)));
        assertThat(result.nulls(), equalTo(SelectionMask.of(false)));
        assertThat(batch.loads, equalTo(List.of("category:0", "price:0", "quantity:0")));
    }

    public void testNullProductIsNotAccepted() {
        List<Row> rows = List.of(new Row("electronics", null, 10L));
        assertThat(PredicatePlans.filter(MIXED, new FullScanSource(rows)).acceptedRows(), equalTo(List.of()));
    }

    public void testNestedAnyKeepsSameChildAcrossBatches() {
        // Parent 0 has the sku on one child and the expensive product on another. Parent 1's match arrives in a later batch.
        MemoryBatch first = MemoryBatch.children(
            new int[] { 0, 1, 2 },
            new int[] { 0, 0, 1 },
            Map.of("sku", new Object[] { "a", "b", "a" }, "qty", new Object[] { 1L, 50L, 1L }, "price", new Object[] { 10L, 50L, 1L })
        );
        MemoryBatch second = MemoryBatch.children(
            new int[] { 3 },
            new int[] { 1 },
            Map.of("sku", new Object[] { "a" }, "qty", new Object[] { 10L }, "price", new Object[] { 20L })
        );
        PredicateExpr sameChild = new PredicateExpr.And(new FieldEquals("sku", "a"), new ProductGreaterThan("qty", "price", 100));
        AnyReduction reduction = new AnyReduction(2);
        RowPredicate child = Predicates.compile(sameChild, new FullScanSource(List.of()));
        reduction.add(first, child.evaluate(first, first.candidates()));
        assertThat(reduction.knownTrues(), equalTo(SelectionMask.of(false, false)));
        reduction.add(second, child.evaluate(second, second.candidates()));
        assertThat(reduction.knownTrues(), equalTo(SelectionMask.of(false, true)));
        PredicateResult parents = reduction.seal();

        assertThat(parents.trues(), equalTo(SelectionMask.of(false, true)));
        assertThat(parents.nulls(), equalTo(SelectionMask.none(2)));
    }

    public void testNestedPlanSealsWhenTheChildCursorEnds() {
        MemoryBatch first = MemoryBatch.children(
            new int[] { 0, 1, 2 },
            new int[] { 0, 0, 1 },
            Map.of("sku", new Object[] { "a", "b", "a" }, "qty", new Object[] { 1L, 50L, 1L }, "price", new Object[] { 10L, 50L, 1L })
        );
        MemoryBatch second = MemoryBatch.children(
            new int[] { 3 },
            new int[] { 1 },
            Map.of("sku", new Object[] { "a" }, "qty", new Object[] { 10L }, "price", new Object[] { 20L })
        );
        PredicateExpr sameChild = new PredicateExpr.And(new FieldEquals("sku", "a"), new ProductGreaterThan("qty", "price", 100));
        RowPredicate child = Predicates.compile(sameChild, new FullScanSource(List.of()));

        PredicateResult parents = NestedAnyPlan.evaluate(2, pages(first, second), child);

        assertThat(parents.trues(), equalTo(SelectionMask.of(false, true)));
        assertThat(parents.nulls(), equalTo(SelectionMask.none(2)));
    }

    public void testCategoryAloneIsAnExactTrueSet() {
        ExactIteratorSource source = new ExactIteratorSource(
            List.of(new Row("electronics", 1L, 1L), new Row(null, 1L, 1L), new Row("books", 1L, 1L))
        );
        FilterPlan plan = PredicatePlans.filter(CATEGORY, source);

        assertThat(plan.coverage(), equalTo(CandidateCoverage.EXACT));
        assertThat(plan.acceptedRows(), equalTo(List.of(source.row(0))));
    }

    public void testNotIsAnalyzedAsAWholeAndKeepsNull() {
        List<Row> rows = List.of(new Row("electronics", 1L, 1L), new Row(null, 1L, 1L), new Row("books", 1L, 1L));
        ExactIteratorSource source = new ExactIteratorSource(rows);
        FilterPlan plan = PredicatePlans.filter(new PredicateExpr.Not(CATEGORY), source);

        assertThat(source.analyzed.getFirst(), equalTo(new PredicateExpr.Not(CATEGORY)));
        assertThat(plan.constraint(), equalTo(CandidateConstraint.none()));
        assertThat(plan.coverage(), equalTo(CandidateCoverage.COMPLETE));
        assertThat(plan.acceptedRows(), equalTo(List.of(source.row(2))));
    }

    public void testTrueOnlyProducerDoesNotEraseNullOnEval() {
        MemoryBatch batch = MemoryBatch.of(new int[] { 0, 1 }, Map.of("category", new Object[] { "electronics", null }), new ArrayList<>());
        PredicateResult result = PredicatePlans.eval(CATEGORY, new TrueOnlySource()).evaluate(batch);

        assertThat(result.trues(), equalTo(SelectionMask.of(true, false)));
        assertThat(result.nulls(), equalTo(SelectionMask.of(false, true)));
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

    private record Row(String category, Long price, Long quantity) {}

    private static final class MemoryBatch implements ChildBatch {
        private final RowSpace space;
        private final int[] rowIds;
        private final int[] parents;
        private final Map<String, Object[]> values;
        private final SelectionMask mask;
        private final List<String> loads;

        private MemoryBatch(
            RowSpace space,
            int[] rowIds,
            int[] parents,
            Map<String, Object[]> values,
            SelectionMask mask,
            List<String> loads
        ) {
            this.space = space;
            this.rowIds = rowIds;
            this.parents = parents;
            this.values = values;
            this.mask = mask;
            this.loads = loads;
        }

        private static MemoryBatch of(int[] rowIds, Map<String, Object[]> values, List<String> loads) {
            return new MemoryBatch(new RowSpace("batch"), rowIds, null, values, SelectionMask.all(rowIds.length), loads);
        }

        private static MemoryBatch select(RowSpace space, List<Row> rows, List<Integer> ids, List<String> loads) {
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
            return new MemoryBatch(
                space,
                rowIds,
                null,
                Map.of("category", category, "price", price, "quantity", quantity),
                SelectionMask.all(count),
                loads
            );
        }

        private static MemoryBatch children(int[] rowIds, int[] parents, Map<String, Object[]> values) {
            return new MemoryBatch(new RowSpace("children"), rowIds, parents, values, SelectionMask.all(rowIds.length), new ArrayList<>());
        }

        @Override
        public int positionCount() {
            return rowIds.length;
        }

        @Override
        public SelectionMask candidates() {
            return mask;
        }

        @Override
        public Column load(FieldRef field, SelectionMask selection) {
            if (selection.positionCount() != rowIds.length) {
                throw new IllegalArgumentException("selection covers [" + selection.positionCount() + "] positions");
            }
            Object[] column = values.get(field.name());
            int[] positions = selection.positions();
            return ordinal -> {
                int position = positions[ordinal];
                if (mask.get(position) == false) {
                    throw new AssertionError("loaded eliminated position [" + position + "] of [" + field.name() + "]");
                }
                loads.add(field.name() + ":" + rowIds[position]);
                return column[position];
            };
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
    private static final class ExactIteratorSource implements PredicateSource {
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
            return once(MemoryBatch.select(space, rows, ids, loads));
        }

        @Override
        public Optional<PredicateRefiner> refiner(PredicateExpr predicate) {
            if (predicate instanceof FieldEquals equals) {
                return Optional.of((batch, candidates) -> new FieldEqualsRefiner(equals).evaluate(batch, candidates));
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
    private static final class StatsPruningSource implements PredicateSource {
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
            return once(MemoryBatch.select(space, rows, ids, loads));
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
    private static final class FullScanSource implements PredicateSource {
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
            return once(MemoryBatch.select(space, rows, ids, loads));
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

    private static final class FieldEqualsRefiner implements PredicateRefiner {
        private final FieldEquals equals;

        private FieldEqualsRefiner(FieldEquals equals) {
            this.equals = equals;
        }

        @Override
        public PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates) {
            return Predicates.compile(equals, new FullScanSource(List.of())).evaluate(batch, candidates);
        }
    }
}

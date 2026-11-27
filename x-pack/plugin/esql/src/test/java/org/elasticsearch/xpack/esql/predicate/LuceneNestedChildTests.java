/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Same-child {@code NESTED_ANY}. A Lucene iterator keeps children whose sku is {@code "a"}, one child per batch, and the
 * ordinary ES|QL evaluator decides {@code qty * price > 100} on those same positions. A parent stays unfinished until
 * the child cursor ends.
 */
public class LuceneNestedChildTests extends ESTestCase {
    private static final FieldRef QTY = new FieldRef("qty");
    private static final FieldRef PRICE = new FieldRef("price");

    public void testSameChildAcrossBatches() throws IOException {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, writerConfig())) {
            add(writer, 0, "a", 1L, 10L);
            add(writer, 0, "b", 50L, 50L);
            add(writer, 1, "a", 1L, 1L);
            add(writer, 1, "a", 10L, 20L);
            add(writer, 2, "a", 10L, null);
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                int[] matches = matchingDocs(skuWeight(new IndexSearcher(reader)), leaf);
                int[] parents = parentsOf(leaf.reader(), matches);
                assertThat(matches, equalTo(new int[] { 0, 2, 3, 4 }));

                List<SourceRow> loaded = new ArrayList<>();
                RowSpace children = new RowSpace("children");
                DriverContext context = driverContext();
                try (ExpressionEvaluator evaluator = evaluator(context)) {
                    AnyReduction reduction = new AnyReduction(3);
                    List<SelectionMask> unfinished = new ArrayList<>();
                    CandidateCursor cursor = pages(children, leaf.reader(), matches, parents, loaded, 1);
                    for (SelectionBatch batch = cursor.nextBatch(); batch != null; batch = cursor.nextBatch()) {
                        assertThat(batch.positionCount(), equalTo(1));
                        reduction.add((ChildBatch) batch, expensive(evaluator).evaluate(batch, batch.candidates()));
                        unfinished.add(reduction.knownTrues());
                    }
                    PredicateResult sealed = reduction.seal();
                    PredicateResult planned = NestedAnyPlan.evaluate(
                        3,
                        pages(children, leaf.reader(), matches, parents, new ArrayList<>(), 1),
                        expensive(evaluator)
                    );

                    assertThat(unfinished.get(0), equalTo(SelectionMask.of(false, false, false)));
                    assertThat(unfinished.get(1), equalTo(SelectionMask.of(false, false, false)));
                    assertThat(unfinished.get(2), equalTo(SelectionMask.of(false, true, false)));
                    assertThat(unfinished.get(3), equalTo(SelectionMask.of(false, true, false)));
                    assertThat(sealed.trues(), equalTo(SelectionMask.of(false, true, false)));
                    assertThat(sealed.nulls(), equalTo(SelectionMask.none(3)));
                    assertThat(planned, equalTo(sealed));
                    assertThat(loaded.contains(new SourceRow(children, 1)), equalTo(false));
                }
            }
        }
    }

    private static RowPredicate expensive(ExpressionEvaluator evaluator) {
        return (batch, candidates) -> {
            int count = candidates.cardinality();
            int[] positions = candidates.positions();
            boolean[] trues = new boolean[batch.positionCount()];
            boolean[] nulls = new boolean[batch.positionCount()];
            LongBlock qty = longs(batch.load(QTY, candidates), count);
            LongBlock price = longs(batch.load(PRICE, candidates), count);
            try (Page page = new Page(qty, price); Block raw = evaluator.eval(page)) {
                BooleanBlock flags = (BooleanBlock) raw;
                for (int ordinal = 0; ordinal < flags.getPositionCount(); ordinal++) {
                    int position = positions[ordinal];
                    if (flags.isNull(ordinal)) {
                        nulls[position] = true;
                    } else if (flags.getBoolean(flags.getFirstValueIndex(ordinal))) {
                        trues[position] = true;
                    }
                }
            }
            return PredicateResult.of(SelectionMask.of(trues), SelectionMask.of(nulls));
        };
    }

    private static LongBlock longs(Column column, int count) {
        try (column; LongBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(count)) {
            for (int ordinal = 0; ordinal < count; ordinal++) {
                Object value = column.value(ordinal);
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendLong((Long) value);
                }
            }
            return builder.build();
        }
    }

    private static CandidateCursor pages(
        RowSpace space,
        LeafReader reader,
        int[] docs,
        int[] parents,
        List<SourceRow> loaded,
        int pageSize
    ) {
        return new CandidateCursor() {
            private int next;

            @Override
            public SelectionBatch nextBatch() {
                if (next >= docs.length) {
                    return null;
                }
                int end = Math.min(next + pageSize, docs.length);
                int[] pageDocs = new int[end - next];
                int[] pageParents = new int[end - next];
                System.arraycopy(docs, next, pageDocs, 0, pageDocs.length);
                System.arraycopy(parents, next, pageParents, 0, pageParents.length);
                next = end;
                return new ChildLeaf(space, reader, pageDocs, pageParents, loaded);
            }
        };
    }

    private static ExpressionEvaluator evaluator(DriverContext context) {
        FieldAttribute qty = field("qty");
        FieldAttribute price = field("price");
        Layout layout = new Layout.Builder().append(qty).append(price).build();
        var expression = new GreaterThan(
            Source.EMPTY,
            new Mul(Source.EMPTY, qty, price),
            new Literal(Source.EMPTY, 100L, DataType.LONG),
            null
        );
        return EvalMapper.toEvaluator(FoldContext.small(), expression, layout).get(context);
    }

    private static void add(IndexWriter writer, int parent, String sku, long qty, Long price) throws IOException {
        Document document = new Document();
        document.add(new LongField("parent", parent, Field.Store.NO));
        document.add(new KeywordField("sku", sku, Field.Store.NO));
        document.add(new LongField("qty", qty, Field.Store.NO));
        if (price != null) {
            document.add(new LongField("price", price, Field.Store.NO));
        }
        writer.addDocument(document);
    }

    private static IndexWriterConfig writerConfig() {
        return new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    }

    private static Weight skuWeight(IndexSearcher searcher) throws IOException {
        Query query = new TermQuery(new Term("sku", "a"));
        return searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    private static int[] matchingDocs(Weight weight, LeafReaderContext leaf) throws IOException {
        ScorerSupplier supplier = weight.scorerSupplier(leaf);
        if (supplier == null) {
            return new int[0];
        }
        DocIdSetIterator iterator = supplier.get(0L).iterator();
        List<Integer> docs = new ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        int[] ids = new int[docs.size()];
        for (int i = 0; i < docs.size(); i++) {
            ids[i] = docs.get(i);
        }
        return ids;
    }

    private static int[] parentsOf(LeafReader reader, int[] docs) throws IOException {
        SortedNumericDocValues values = reader.getSortedNumericDocValues("parent");
        int[] parents = new int[docs.length];
        for (int i = 0; i < docs.length; i++) {
            if (values == null || values.advanceExact(docs[i]) == false || values.docValueCount() != 1) {
                throw new IllegalStateException("missing parent for doc [" + docs[i] + "]");
            }
            parents[i] = (int) values.nextValue();
        }
        return parents;
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.LONG, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static DriverContext driverContext() {
        return new DriverContext(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking(),
            TestBlockFactory.getNonBreakingInstance(),
            null
        );
    }

    private static final class ChildLeaf implements ChildBatch {
        private final RowSpace space;
        private final LeafReader reader;
        private final int[] docs;
        private final int[] parents;
        private final List<SourceRow> loaded;

        private ChildLeaf(RowSpace space, LeafReader reader, int[] docs, int[] parents, List<SourceRow> loaded) {
            this.space = space;
            this.reader = reader;
            this.docs = docs;
            this.parents = parents;
            this.loaded = loaded;
        }

        @Override
        public int positionCount() {
            return docs.length;
        }

        @Override
        public SelectionMask candidates() {
            return SelectionMask.all(docs.length);
        }

        @Override
        public Column load(FieldRef field, SelectionMask selection) {
            try {
                SortedNumericDocValues values = reader.getSortedNumericDocValues(field.name());
                int[] positions = selection.positions();
                Object[] compacted = new Object[positions.length];
                for (int ordinal = 0; ordinal < positions.length; ordinal++) {
                    int doc = docs[positions[ordinal]];
                    loaded.add(new SourceRow(space, doc));
                    if (values != null && values.advanceExact(doc) && values.docValueCount() == 1) {
                        compacted[ordinal] = values.nextValue();
                    }
                }
                return ordinal -> compacted[ordinal];
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public RowSpace rowSpace() {
            return space;
        }

        @Override
        public SourceRow sourceRow(int position) {
            return new SourceRow(space, docs[position]);
        }

        @Override
        public int parentPosition(int position) {
            return parents[position];
        }
    }
}

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
 * A Lucene doc-id iterator selects candidate positions, and the ordinary ES|QL evaluator decides the residual
 * {@code price * quantity > 100} on those positions only. Doc ids are compared inside one leaf's row space.
 */
public class LuceneSelectedPositionsTests extends ESTestCase {
    private static final FieldRef PRICE = new FieldRef("price");
    private static final FieldRef QUANTITY = new FieldRef("quantity");

    public void testIteratorAndEvaluatorAgreeAcrossSegments() throws IOException {
        try (Directory directory = newDirectory()) {
            writeTwoSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(2));
                Weight category = categoryWeight(new IndexSearcher(reader));
                DriverContext context = driverContext();
                FieldAttribute price = field("price");
                FieldAttribute quantity = field("quantity");
                Layout layout = new Layout.Builder().append(price).append(quantity).build();
                var expression = new GreaterThan(
                    Source.EMPTY,
                    new Mul(Source.EMPTY, price, quantity),
                    new Literal(Source.EMPTY, 100L, DataType.LONG),
                    null
                );
                try (ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FoldContext.small(), expression, layout).get(context)) {
                    List<SourceRow> selectedTrues = new ArrayList<>();
                    List<SourceRow> selectedNulls = new ArrayList<>();
                    List<SourceRow> scannedTrues = new ArrayList<>();
                    List<SourceRow> scannedNulls = new ArrayList<>();
                    List<SourceRow> loaded = new ArrayList<>();
                    for (LeafReaderContext leaf : reader.leaves()) {
                        RowSpace space = new RowSpace("segment-" + leaf.ord);
                        int[] matches = matchingDocs(category, leaf);
                        Outcome selected = evaluate(evaluator, new LeafBatch(space, leaf.reader(), matches, loaded), null);
                        selectedTrues.addAll(selected.trues());
                        selectedNulls.addAll(selected.nulls());
                        Outcome scanned = evaluate(
                            evaluator,
                            new LeafBatch(space, leaf.reader(), everyDoc(leaf.reader().maxDoc()), new ArrayList<>()),
                            matches
                        );
                        scannedTrues.addAll(scanned.trues());
                        scannedNulls.addAll(scanned.nulls());
                    }
                    RowSpace first = selectedTrues.get(0).space();
                    assertThat(selectedTrues, equalTo(List.of(new SourceRow(first, 0))));
                    assertThat(selectedTrues, equalTo(scannedTrues));
                    assertThat(selectedNulls, equalTo(scannedNulls));
                    assertThat(selectedNulls, equalTo(List.of(new SourceRow(readerSpace(selectedNulls), 1))));
                    assertThat(loaded.contains(new SourceRow(first, 1)), equalTo(false));
                }
            }
        }
    }

    public void testLoadedValueFollowsTheSelection() throws IOException {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, writerConfig())) {
            add(writer, "electronics", 1L, 1L);
            add(writer, "electronics", null, 10L);
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                LeafBatch batch = new LeafBatch(new RowSpace("segment"), leaf.reader(), new int[] { 0, 1 }, new ArrayList<>());
                try (var column = batch.load(PRICE, SelectionMask.of(false, true))) {
                    assertThat(column.value(0), equalTo(null));
                }
                assertThat(batch.loaded, equalTo(List.of(new SourceRow(batch.rowSpace(), 1))));
            }
        }
    }

    private static Outcome evaluate(ExpressionEvaluator evaluator, LeafBatch batch, int[] restrictTo) {
        int count = batch.positionCount();
        if (count == 0) {
            return new Outcome(List.of(), List.of());
        }
        LongBlock priceBlock = longs(batch.load(PRICE, batch.candidates()), count);
        LongBlock quantityBlock = longs(batch.load(QUANTITY, batch.candidates()), count);
        try (Page page = new Page(priceBlock, quantityBlock); Block raw = evaluator.eval(page)) {
            BooleanBlock flags = (BooleanBlock) raw;
            List<SourceRow> trues = new ArrayList<>();
            List<SourceRow> nulls = new ArrayList<>();
            for (int position = 0; position < flags.getPositionCount(); position++) {
                int doc = batch.docs[position];
                if (restrictTo != null && contains(restrictTo, doc) == false) {
                    continue;
                }
                SourceRow row = batch.sourceRow(position);
                if (flags.isNull(position)) {
                    nulls.add(row);
                } else if (flags.getBoolean(flags.getFirstValueIndex(position))) {
                    trues.add(row);
                }
            }
            return new Outcome(trues, nulls);
        }
    }

    private static boolean contains(int[] docs, int doc) {
        for (int candidate : docs) {
            if (candidate == doc) {
                return true;
            }
        }
        return false;
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

    private static RowSpace readerSpace(List<SourceRow> rows) {
        return rows.get(0).space();
    }

    private static void writeTwoSegments(Directory directory) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
            add(writer, "electronics", 20L, 10L);
            add(writer, "books", 100L, 100L);
            writer.commit();
            add(writer, "electronics", 1L, 1L);
            add(writer, "electronics", null, 10L);
            writer.commit();
        }
    }

    private static IndexWriterConfig writerConfig() {
        return new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    }

    private static void add(IndexWriter writer, String category, Long price, long quantity) throws IOException {
        Document document = new Document();
        document.add(new KeywordField("category", category, Field.Store.NO));
        if (price != null) {
            document.add(new LongField("price", price, Field.Store.NO));
        }
        document.add(new LongField("quantity", quantity, Field.Store.NO));
        writer.addDocument(document);
    }

    private static Weight categoryWeight(IndexSearcher searcher) throws IOException {
        Query query = new TermQuery(new Term("category", "electronics"));
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

    private static int[] everyDoc(int maxDoc) {
        int[] docs = new int[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            docs[doc] = doc;
        }
        return docs;
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

    private record Outcome(List<SourceRow> trues, List<SourceRow> nulls) {}

    private static final class LeafBatch implements SelectionBatch {
        private final RowSpace space;
        private final LeafReader reader;
        private final int[] docs;
        private final List<SourceRow> loaded;

        private LeafBatch(RowSpace space, LeafReader reader, int[] docs, List<SourceRow> loaded) {
            this.space = space;
            this.reader = reader;
            this.docs = docs;
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
    }
}

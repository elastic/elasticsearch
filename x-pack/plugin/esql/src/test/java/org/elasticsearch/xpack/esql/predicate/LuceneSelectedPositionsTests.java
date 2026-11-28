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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.all;
import static org.hamcrest.Matchers.equalTo;

/**
 * A Lucene doc-id iterator selects candidate positions, and the ordinary ES|QL evaluator decides the residual
 * {@code price * quantity > 100} on those positions only. Doc ids are compared inside one leaf's row space.
 */
public class LuceneSelectedPositionsTests extends ComputeTestCase {
    private static final FieldRef PRICE = new FieldRef("price", DataType.LONG);
    private static final PredicateExpr EXPENSIVE = new PredicateExpr.ProductGreaterThan("price", "quantity", 100);

    private BlockFactory factory;
    private DriverContext context;

    @Before
    public void createContext() {
        factory = blockFactory();
        context = new DriverContext(nonBreakingBigArrays(), factory, null);
    }

    public void testIteratorAndEvaluatorAgreeAcrossSegments() throws IOException {
        try (Directory directory = newDirectory()) {
            writeTwoSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory); RowPredicate predicate = expensive()) {
                assertThat(reader.leaves().size(), equalTo(2));
                Weight category = categoryWeight(new IndexSearcher(reader));
                List<SourceRow> selectedTrues = new ArrayList<>();
                List<SourceRow> selectedNulls = new ArrayList<>();
                List<SourceRow> scannedTrues = new ArrayList<>();
                List<SourceRow> scannedNulls = new ArrayList<>();
                List<SourceRow> selectedLoads = new ArrayList<>();
                List<SourceRow> scannedLoads = new ArrayList<>();
                for (LeafReaderContext leaf : reader.leaves()) {
                    RowSpace space = new RowSpace("segment-" + leaf.ord);
                    int[] matches = matchingDocs(category, leaf);
                    Outcome selected = evaluate(
                        predicate,
                        new LeafBatch(factory, space, leaf.reader(), matches, selectedLoads),
                        all(matches.length)
                    );
                    selectedTrues.addAll(selected.trues());
                    selectedNulls.addAll(selected.nulls());
                    Outcome scanned = evaluate(
                        predicate,
                        new LeafBatch(factory, space, leaf.reader(), everyDoc(leaf.reader().maxDoc()), scannedLoads),
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
                assertThat(selectedLoads.contains(new SourceRow(first, 1)), equalTo(false));
                assertThat(scannedLoads.contains(new SourceRow(first, 1)), equalTo(false));
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
                LeafBatch batch = new LeafBatch(factory, new RowSpace("segment"), leaf.reader(), new int[] { 0, 1 }, new ArrayList<>());
                try (LongBlock block = (LongBlock) batch.load(PRICE, new int[] { 1 })) {
                    assertThat(block.getPositionCount(), equalTo(1));
                    assertThat(block.isNull(0), equalTo(true));
                }
                assertThat(batch.loaded, equalTo(List.of(new SourceRow(batch.rowSpace(), 1))));
            }
        }
    }

    private RowPredicate expensive() {
        return Predicates.compile(EXPENSIVE, PredicateTestBlocks.scanOnly(), context);
    }

    private static Outcome evaluate(RowPredicate predicate, LeafBatch batch, int[] selection) {
        List<SourceRow> trues = new ArrayList<>();
        List<SourceRow> nulls = new ArrayList<>();
        try (BooleanBlock flags = predicate.evaluate(batch, selection)) {
            for (int ordinal = 0; ordinal < flags.getPositionCount(); ordinal++) {
                SourceRow row = batch.sourceRow(selection[ordinal]);
                if (flags.isNull(ordinal)) {
                    nulls.add(row);
                } else if (flags.getBoolean(flags.getFirstValueIndex(ordinal))) {
                    trues.add(row);
                }
            }
        }
        return new Outcome(trues, nulls);
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
        return all(maxDoc);
    }

    private record Outcome(List<SourceRow> trues, List<SourceRow> nulls) {}

    private static final class LeafBatch implements SelectionBatch {
        private final BlockFactory factory;
        private final RowSpace space;
        private final LeafReader reader;
        private final int[] docs;
        private final List<SourceRow> loaded;

        private LeafBatch(BlockFactory factory, RowSpace space, LeafReader reader, int[] docs, List<SourceRow> loaded) {
            this.factory = factory;
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
        public int[] candidates() {
            return all(docs.length);
        }

        @Override
        public Block load(FieldRef field, int[] selection) {
            try (LongBlock.Builder builder = factory.newLongBlockBuilder(selection.length)) {
                SortedNumericDocValues values = reader.getSortedNumericDocValues(field.name());
                for (int position : selection) {
                    int doc = docs[position];
                    loaded.add(new SourceRow(space, doc));
                    if (values != null && values.advanceExact(doc) && values.docValueCount() == 1) {
                        builder.appendLong(values.nextValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
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

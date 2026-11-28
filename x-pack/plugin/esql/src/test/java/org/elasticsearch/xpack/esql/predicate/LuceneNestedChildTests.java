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
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.all;
import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.tri;
import static org.elasticsearch.xpack.esql.predicate.PredicateTestBlocks.values;
import static org.hamcrest.Matchers.equalTo;

/**
 * Same-child {@code NESTED_ANY}. A Lucene iterator keeps children whose sku is {@code "a"}, one child per batch, and the
 * ordinary ES|QL evaluator decides {@code qty * price > 100} on those same positions. A parent stays unfinished until
 * the child cursor ends.
 */
public class LuceneNestedChildTests extends ComputeTestCase {
    private static final PredicateExpr EXPENSIVE = new PredicateExpr.ProductGreaterThan("qty", "price", 100);

    private BlockFactory factory;
    private DriverContext context;

    @Before
    public void createContext() {
        factory = blockFactory();
        context = new DriverContext(nonBreakingBigArrays(), factory, null);
    }

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
                try (RowPredicate expensive = Predicates.compile(EXPENSIVE, PredicateTestBlocks.scanOnly(), context)) {
                    AnyReduction reduction = new AnyReduction(factory, 3);
                    List<List<Boolean>> unfinished = new ArrayList<>();
                    CandidateCursor cursor = pages(children, leaf.reader(), matches, parents, loaded, 1);
                    for (SelectionBatch batch = cursor.nextBatch(); batch != null; batch = cursor.nextBatch()) {
                        assertThat(batch.positionCount(), equalTo(1));
                        int[] selection = batch.candidates();
                        try (BooleanBlock result = expensive.evaluate(batch, selection)) {
                            reduction.add((ChildBatch) batch, selection, result);
                        }
                        try (BooleanBlock known = reduction.knownTrues()) {
                            unfinished.add(values(known));
                        }
                    }
                    List<Boolean> sealed;
                    try (BooleanBlock block = reduction.seal()) {
                        sealed = values(block);
                    }
                    List<Boolean> planned;
                    try (
                        BooleanBlock block = NestedAnyPlan.evaluate(
                            factory,
                            3,
                            pages(children, leaf.reader(), matches, parents, new ArrayList<>(), 1),
                            expensive
                        )
                    ) {
                        planned = values(block);
                    }

                    assertThat(unfinished.get(0), equalTo(tri(false, false, false)));
                    assertThat(unfinished.get(1), equalTo(tri(false, false, false)));
                    assertThat(unfinished.get(2), equalTo(tri(false, true, false)));
                    assertThat(unfinished.get(3), equalTo(tri(false, true, false)));
                    assertThat(sealed, equalTo(tri(false, true, false)));
                    assertThat(planned, equalTo(sealed));
                    assertThat(loaded.contains(new SourceRow(children, 1)), equalTo(false));
                }
            }
        }
    }

    private CandidateCursor pages(RowSpace space, LeafReader reader, int[] docs, int[] parents, List<SourceRow> loaded, int pageSize) {
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
                return new ChildLeaf(factory, space, reader, pageDocs, pageParents, loaded);
            }
        };
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

    private static final class ChildLeaf implements ChildBatch {
        private final BlockFactory factory;
        private final RowSpace space;
        private final LeafReader reader;
        private final int[] docs;
        private final int[] parents;
        private final List<SourceRow> loaded;

        private ChildLeaf(BlockFactory factory, RowSpace space, LeafReader reader, int[] docs, int[] parents, List<SourceRow> loaded) {
            this.factory = factory;
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

        @Override
        public int parentPosition(int position) {
            return parents[position];
        }
    }
}

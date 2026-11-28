/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The one-pass filter tree on real Lucene segments. Each test compares the docs that pass with a brute-force answer
 * computed from the arrays the index was built from, so a tree that reads the wrong window or keeps a deleted doc fails.
 */
public class DocFilterTests extends ComputeTestCase {
    private static final int SEGMENTS = 3;
    private static final int DOCS_PER_SEGMENT = 500;
    private static final long THRESHOLD = 100;

    private BlockFactory factory;
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private String[] category;
    private Long[] price;
    private Long[] quantity;
    private boolean[] deleted;
    private int windowSize;

    @Before
    public void buildIndex() throws IOException {
        factory = blockFactory();
        windowSize = randomFrom(37, 64, 128, 4096);
        int docs = SEGMENTS * DOCS_PER_SEGMENT;
        category = new String[docs];
        price = new Long[docs];
        quantity = new Long[docs];
        deleted = new boolean[docs];
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < docs; i++) {
                category[i] = randomBoolean() ? "a" : "b";
                price[i] = randomInt(9) == 0 ? null : (long) randomIntBetween(1, 40);
                quantity[i] = (long) randomIntBetween(1, 40);
                Document document = new Document();
                document.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                document.add(new KeywordField("category", category[i], Field.Store.NO));
                if (price[i] != null) {
                    document.add(new LongField("price", price[i], Field.Store.NO));
                }
                document.add(new LongField("quantity", quantity[i], Field.Store.NO));
                writer.addDocument(document);
                if ((i + 1) % DOCS_PER_SEGMENT == 0) {
                    writer.commit();
                }
            }
            for (int i = 0; i < docs; i++) {
                if (randomInt(19) == 0) {
                    deleted[i] = true;
                    writer.deleteDocuments(new Term("id", Integer.toString(i)));
                }
            }
            writer.commit();
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        assertThat(reader.leaves().size(), equalTo(SEGMENTS));
    }

    @After
    public void closeIndex() throws IOException {
        reader.close();
        directory.close();
    }

    public void testANativeLeafPassesTheMatchingLiveDocsOfEverySegment() throws IOException {
        try (DocFilter filter = NativeDocFilter.of(searcher, new TermQuery(new Term("category", "a")))) {
            assertThat(passing(filter), equalTo(expected(doc -> category[doc].equals("a"))));
        }
    }

    public void testDeletedDocsNeverPass() throws IOException {
        try (DocFilter filter = NativeDocFilter.of(searcher, new TermQuery(new Term("category", "a")))) {
            for (int doc : passing(filter)) {
                assertThat(deleted[doc], is(false));
            }
        }
    }

    public void testANativeLeafReadsEachWindowWithIntoBitSet() throws IOException {
        CountingQuery counting = new CountingQuery(new TermQuery(new Term("category", "a")));
        try (DocFilter filter = NativeDocFilter.of(searcher, counting)) {
            assertThat(passing(filter), equalTo(expected(doc -> category[doc].equals("a"))));
        }
        assertThat(counting.nextDocCalls.get(), equalTo(0));
        assertThat(counting.intoBitSetCalls.get(), greaterThan(0));
        assertThat(counting.advanceCalls.get(), lessThanOrEqualTo(counting.intoBitSetCalls.get()));
    }

    public void testANativeTwoPhaseQueryPassesTheConfirmedDocs() throws IOException {
        Query range = SortedNumericDocValuesField.newSlowRangeQuery("quantity", 10, 20);
        Weight weight = searcher.createWeight(searcher.rewrite(range), ScoreMode.COMPLETE_NO_SCORES, 1f);
        assertThat(weight.scorer(reader.leaves().get(0)).twoPhaseIterator() != null, is(true));
        try (DocFilter filter = NativeDocFilter.of(searcher, range)) {
            assertThat(passing(filter), equalTo(expected(doc -> quantity[doc] >= 10 && quantity[doc] <= 20)));
        }
    }

    public void testAnExpressionAloneEvaluatesEveryLiveDoc() throws IOException {
        Columns columns = new Columns();
        try (DocFilter filter = expensive(columns)) {
            assertThat(passing(filter), equalTo(expected(this::expensiveDoc)));
        }
    }

    public void testANullExpressionResultDoesNotPass() throws IOException {
        Columns columns = new Columns();
        try (DocFilter filter = expensive(columns)) {
            for (int doc : passing(filter)) {
                assertThat(price[doc] != null, is(true));
            }
        }
    }

    public void testANativeLeafAndAnExpressionCountTheSameDocsAsBruteForce() throws IOException {
        Columns columns = new Columns();
        try (DocFilter filter = new AndDocFilter(category("a"), expensive(columns))) {
            assertThat(passing(filter), equalTo(expected(doc -> category[doc].equals("a") && expensiveDoc(doc))));
            assertThat(
                DocFilterPass.count(reader, filter, windowSize),
                equalTo((long) expected(doc -> category[doc].equals("a") && expensiveDoc(doc)).size())
            );
        }
    }

    public void testTheExpressionLoadsOnlyDocsTheNativeLeafKept() throws IOException {
        Columns columns = new Columns();
        try (DocFilter filter = new AndDocFilter(category("a"), expensive(columns))) {
            passing(filter);
        }
        assertThat(columns.loaded.isEmpty(), is(false));
        for (int doc : columns.loaded) {
            assertThat(category[doc], equalTo("a"));
            assertThat(deleted[doc], is(false));
        }
        assertThat(columns.loaded.size(), lessThan(expected(doc -> true).size()));
    }

    public void testNothingIsLoadedWhenTheNativeLeafKeepsNothing() throws IOException {
        Columns columns = new Columns();
        try (
            DocFilter filter = new AndDocFilter(
                NativeDocFilter.of(searcher, new TermQuery(new Term("category", "none"))),
                expensive(columns)
            )
        ) {
            assertThat(passing(filter), is(empty()));
            assertThat(DocFilterPass.count(reader, filter, windowSize), equalTo(0L));
        }
        assertThat(columns.loaded, is(empty()));
    }

    public void testAnOrExpressionOnlySeesWhatTheOtherSideRejected() throws IOException {
        Columns columns = new Columns();
        try (DocFilter filter = new OrDocFilter(category("a"), expensive(columns))) {
            assertThat(passing(filter), equalTo(expected(doc -> category[doc].equals("a") || expensiveDoc(doc))));
        }
        assertThat(columns.loaded.isEmpty(), is(false));
        for (int doc : columns.loaded) {
            assertThat(category[doc], equalTo("b"));
        }
    }

    public void testAnAndInsideAnOrKeepsTheBranchesApart() throws IOException {
        Columns columns = new Columns();
        try (
            DocFilter filter = new OrDocFilter(
                new AndDocFilter(category("a"), expensive(columns)),
                new AndDocFilter(
                    category("b"),
                    NativeDocFilter.of(searcher, SortedNumericDocValuesField.newSlowRangeQuery("quantity", 1, 5))
                )
            )
        ) {
            assertThat(
                passing(filter),
                equalTo(
                    expected(doc -> (category[doc].equals("a") && expensiveDoc(doc)) || (category[doc].equals("b") && quantity[doc] <= 5))
                )
            );
        }
    }

    private DocFilter category(String value) throws IOException {
        return NativeDocFilter.of(searcher, new TermQuery(new Term("category", value)));
    }

    private DocFilter expensive(Columns columns) {
        return new ExpressionDocFilter(new ProductGreaterThan(THRESHOLD), List.of("price", "quantity"), columns);
    }

    private boolean expensiveDoc(int doc) {
        return price[doc] != null && price[doc] * quantity[doc] > THRESHOLD;
    }

    private TreeSet<Integer> expected(IntPredicate keep) {
        TreeSet<Integer> docs = new TreeSet<>();
        for (int doc = 0; doc < category.length; doc++) {
            if (deleted[doc] == false && keep.test(doc)) {
                docs.add(doc);
            }
        }
        return docs;
    }

    private TreeSet<Integer> passing(DocFilter filter) throws IOException {
        TreeSet<Integer> docs = new TreeSet<>();
        DocFilterPass.run(reader, filter, windowSize, (window, passing) -> {
            for (int bit = passing.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = next(passing, bit)) {
                docs.add(window.leaf().docBase + window.from() + bit);
            }
        });
        return docs;
    }

    private static int next(FixedBitSet bits, int bit) {
        return bit + 1 < bits.length() ? bits.nextSetBit(bit + 1) : DocIdSetIterator.NO_MORE_DOCS;
    }

    /** Loads a long column from doc values and records the docs it was asked for. */
    private final class Columns implements DocColumns {
        private final List<Integer> loaded = new ArrayList<>();

        @Override
        public Block load(String field, LeafReaderContext leaf, int[] docs) throws IOException {
            SortedNumericDocValues values = leaf.reader().getSortedNumericDocValues(field);
            try (LongBlock.Builder builder = factory.newLongBlockBuilder(docs.length)) {
                for (int doc : docs) {
                    if (field.equals("price")) {
                        loaded.add(leaf.docBase + doc);
                    }
                    if (values != null && values.advanceExact(doc) && values.docValueCount() == 1) {
                        builder.appendLong(values.nextValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }

    /** {@code price * quantity > threshold}, null when either factor is null. */
    private final class ProductGreaterThan implements ExpressionEvaluator {
        private final long threshold;

        private ProductGreaterThan(long threshold) {
            this.threshold = threshold;
        }

        @Override
        public Block eval(Page page) {
            LongBlock left = page.getBlock(0);
            LongBlock right = page.getBlock(1);
            try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(page.getPositionCount())) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (left.isNull(position) || right.isNull(position)) {
                        builder.appendNull();
                    } else {
                        long product = left.getLong(left.getFirstValueIndex(position)) * right.getLong(right.getFirstValueIndex(position));
                        builder.appendBoolean(product > threshold);
                    }
                }
                return builder.build();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    /** Counts how a native leaf walks the matching docs. */
    private static final class CountingQuery extends Query {
        private final Query in;
        private final AtomicInteger nextDocCalls = new AtomicInteger();
        private final AtomicInteger advanceCalls = new AtomicInteger();
        private final AtomicInteger intoBitSetCalls = new AtomicInteger();

        private CountingQuery(Query in) {
            this.in = in;
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) {
            return this;
        }

        @Override
        public Weight createWeight(IndexSearcher indexSearcher, ScoreMode scoreMode, float boost) throws IOException {
            return new FilterWeight(this, in.createWeight(indexSearcher, scoreMode, boost)) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    ScorerSupplier supplier = in.scorerSupplier(context);
                    if (supplier == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            Scorer scorer = supplier.get(leadCost);
                            DocIdSetIterator iterator = new CountingIterator(scorer.iterator(), CountingQuery.this);
                            return new Scorer() {
                                @Override
                                public int docID() {
                                    return iterator.docID();
                                }

                                @Override
                                public DocIdSetIterator iterator() {
                                    return iterator;
                                }

                                @Override
                                public float score() throws IOException {
                                    return scorer.score();
                                }

                                @Override
                                public float getMaxScore(int upTo) throws IOException {
                                    return scorer.getMaxScore(upTo);
                                }
                            };
                        }

                        @Override
                        public long cost() {
                            return supplier.cost();
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            in.visit(visitor);
        }

        @Override
        public String toString(String field) {
            return "counting(" + in.toString(field) + ")";
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    private static final class CountingIterator extends DocIdSetIterator {
        private final DocIdSetIterator in;
        private final CountingQuery counts;

        private CountingIterator(DocIdSetIterator in, CountingQuery counts) {
            this.in = in;
            this.counts = counts;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            counts.nextDocCalls.incrementAndGet();
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            counts.advanceCalls.incrementAndGet();
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            counts.intoBitSetCalls.incrementAndGet();
            in.intoBitSet(upTo, bitSet, offset);
        }
    }
}

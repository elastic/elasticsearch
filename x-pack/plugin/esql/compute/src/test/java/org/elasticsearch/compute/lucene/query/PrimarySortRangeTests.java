/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests {@link PrimarySortRange} and how {@link LuceneSliceQueue.PartitioningStrategy#DOC} uses it. The key
 * property is that the range, and the slices cut from it, never miss a matching doc. Index sorted data is
 * written in several segments, so the skipper search runs on more than one leaf.
 */
public class PrimarySortRangeTests extends ESTestCase {
    private static final String SORT_FIELD = "f";
    private static final int NUM_DOCS = 100_000;
    private static final int NUM_VALUES = 400;

    private Directory directory;
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    /**
     * Writes {@code NUM_DOCS} docs with random values in {@code [0, NUM_VALUES)} on {@link #SORT_FIELD} and
     * on {@code other}, sorted by {@link #SORT_FIELD} if {@code reverse} is not {@code null}.
     */
    private void index(Boolean reverse) throws IOException {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        config.setMaxBufferedDocs(NUM_DOCS / between(1, 3));
        if (reverse != null) {
            config.setIndexSort(new Sort(new SortedNumericSortField(SORT_FIELD, SortField.Type.LONG, reverse)));
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(SortedNumericDocValuesField.indexedField(SORT_FIELD, between(0, NUM_VALUES - 1)));
                doc.add(SortedNumericDocValuesField.indexedField("other", between(0, NUM_VALUES - 1)));
                writer.addDocument(doc);
            }
        }
        reader = DirectoryReader.open(directory);
    }

    private static List<NumericDocValuesRangeQuery> clauses(Query query) {
        return PrimarySortRange.requiredRangeClauses(query);
    }

    public void testRangeContainsEveryMatchAndIsNarrow() throws IOException {
        for (boolean reverse : new boolean[] { false, true }) {
            closeIndex();
            index(reverse);
            long lower = between(0, NUM_VALUES - 1);
            long upper = lower + between(0, 5);
            List<NumericDocValuesRangeQuery> clauses = clauses(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, lower, upper));
            long rangeDocs = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                PrimarySortRange.DocRange range = PrimarySortRange.find(leaf, clauses);
                assertThat(range, org.hamcrest.Matchers.notNullValue());
                SortedNumericDocValues values = DocValues.getSortedNumeric(leaf.reader(), SORT_FIELD);
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    assertTrue(values.advanceExact(doc));
                    long value = values.nextValue();
                    if (value >= lower && value <= upper) {
                        var skipper = leaf.reader().getDocValuesSkipper(SORT_FIELD);
                        String detail = "reverse="
                            + reverse
                            + " lower="
                            + lower
                            + " upper="
                            + upper
                            + " range="
                            + range
                            + " maxDoc="
                            + leaf.reader().maxDoc()
                            + " skipper=["
                            + skipper.minValue()
                            + ","
                            + skipper.maxValue()
                            + "] docCount="
                            + skipper.docCount()
                            + " value="
                            + value;
                        assertThat("doc " + doc + " " + detail, doc, greaterThanOrEqualTo(range.minDoc()));
                        assertThat("doc " + doc + " " + detail, doc, lessThan(range.maxDoc()));
                    }
                }
                rangeDocs += range.length();
            }
            // 6 of 400 values match at most, so the ranges must be much smaller than the data. Allow for skipper block rounding.
            assertThat("reverse=" + reverse, rangeDocs, lessThan((long) NUM_DOCS / 2));
        }
    }

    public void testEmptyRange() throws IOException {
        index(randomBoolean());
        List<NumericDocValuesRangeQuery> clauses = clauses(
            SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, NUM_VALUES + 10, NUM_VALUES + 20)
        );
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(PrimarySortRange.find(leaf, clauses), equalTo(new PrimarySortRange.DocRange(0, 0)));
        }
    }

    public void testClausesIntersect() throws IOException {
        index(randomBoolean());
        BooleanQuery query = new BooleanQuery.Builder().add(
            SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 0, 300),
            BooleanClause.Occur.FILTER
        ).add(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 100, 400), BooleanClause.Occur.MUST).build();
        List<NumericDocValuesRangeQuery> both = clauses(query);
        List<NumericDocValuesRangeQuery> one = clauses(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 100, 300));
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(PrimarySortRange.find(leaf, both), equalTo(PrimarySortRange.find(leaf, one)));
        }
    }

    public void testNoRangeOnPrimarySortField() throws IOException {
        index(randomBoolean());
        List<NumericDocValuesRangeQuery> clauses = clauses(SortedNumericDocValuesField.newSlowRangeQuery("other", 1, 2));
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(PrimarySortRange.find(leaf, clauses), nullValue());
        }
    }

    public void testNoIndexSort() throws IOException {
        index(null);
        List<NumericDocValuesRangeQuery> clauses = clauses(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 1, 2));
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(PrimarySortRange.find(leaf, clauses), nullValue());
        }
    }

    public void testRequiredClauses() {
        Query a = SortedNumericDocValuesField.newSlowRangeQuery("a", 1, 2);
        Query b = SortedNumericDocValuesField.newSlowRangeQuery("b", 1, 2);
        Query c = SortedNumericDocValuesField.newSlowRangeQuery("c", 1, 2);
        Query d = SortedNumericDocValuesField.newSlowRangeQuery("d", 1, 2);
        Query query = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(new BoostQuery(a, 2f), BooleanClause.Occur.FILTER)
                .add(b, BooleanClause.Occur.MUST)
                // A doc does not have to match a SHOULD or a MUST_NOT clause as a range, so they must not narrow the result.
                .add(c, BooleanClause.Occur.SHOULD)
                .add(d, BooleanClause.Occur.MUST_NOT)
                .build()
        );
        assertThat(clauses(query).stream().map(NumericDocValuesRangeQuery::getField).toList(), equalTo(List.of("a", "b")));
    }

    /**
     * Slices must cover every matching doc, be smaller than the whole leaves would give, and spread a narrow
     * range over more slices than the default partitioning does.
     */
    public void testSlicesCoverMatches() throws IOException {
        boolean reverse = randomBoolean();
        index(reverse);
        long lower = between(0, NUM_VALUES - 10);
        long upper = lower + between(0, 8);
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, lower, upper)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        int taskConcurrency = between(2, 8);
        int minDocsPerSlice = between(50, 500);
        List<List<PartialLeafReaderContext>> groups = LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            taskConcurrency,
            weight,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            minDocsPerSlice
        );
        long sliceDocs = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedNumericDocValues values = DocValues.getSortedNumeric(leaf.reader(), SORT_FIELD);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                assertTrue(values.advanceExact(doc));
                long value = values.nextValue();
                if (value >= lower && value <= upper) {
                    boolean covered = false;
                    for (List<PartialLeafReaderContext> group : groups) {
                        for (PartialLeafReaderContext slice : group) {
                            covered |= slice.leafReaderContext() == leaf && doc >= slice.minDoc() && doc < slice.maxDoc();
                        }
                    }
                    assertTrue("doc " + doc + " of leaf " + leaf.ord + " is in no slice", covered);
                }
            }
        }
        for (List<PartialLeafReaderContext> group : groups) {
            for (PartialLeafReaderContext slice : group) {
                assertThat(slice.minDoc(), lessThan(slice.maxDoc()));
                assertThat(slice.maxDoc(), lessThanOrEqualTo(slice.leafReaderContext().reader().maxDoc()));
                sliceDocs += slice.maxDoc() - slice.minDoc();
            }
        }
        assertThat("slices must skip the docs that cannot match", sliceDocs, lessThan((long) NUM_DOCS / 2));
        assertThat(groups.size(), greaterThan(0));
    }

    /**
     * Without a range on the primary sort field the leaves are sliced as before.
     */
    public void testOtherFieldKeepsDefaultSlices() throws IOException {
        index(randomBoolean());
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery("other", 1, 2)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        long sliceDocs = 0;
        for (List<PartialLeafReaderContext> group : LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            4,
            weight,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            100
        )) {
            for (PartialLeafReaderContext slice : group) {
                sliceDocs += slice.maxDoc() - slice.minDoc();
            }
        }
        assertThat(sliceDocs, equalTo((long) NUM_DOCS));
    }
}

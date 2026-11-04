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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
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
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

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
        index(reverse, NUM_DOCS / between(1, 3));
    }

    /**
     * Like {@link #index(Boolean)} with segments of {@code docsPerSegment} docs. Segments are not merged, so
     * the number of leaves is known.
     */
    private void index(Boolean reverse, int docsPerSegment) throws IOException {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        config.setMaxBufferedDocs(docsPerSegment);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
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

    private Map<LeafReaderContext, PrimarySortRange.DocRange> ranges(Query query) {
        return PrimarySortRange.find(reader.leaves(), query);
    }

    public void testRangeContainsEveryMatchAndIsNarrow() throws IOException {
        for (boolean reverse : new boolean[] { false, true }) {
            closeIndex();
            index(reverse);
            long lower = between(0, NUM_VALUES - 1);
            long upper = lower + between(0, 5);
            Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = ranges(
                SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, lower, upper)
            );
            long rangeDocs = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                PrimarySortRange.DocRange range = ranges.get(leaf);
                assertThat(range, notNullValue());
                SortedNumericDocValues values = DocValues.getSortedNumeric(leaf.reader(), SORT_FIELD);
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    assertTrue(values.advanceExact(doc));
                    long value = values.nextValue();
                    if (value >= lower && value <= upper) {
                        String message = "reverse=" + reverse + " doc=" + doc + " value=" + value + " range=" + range;
                        assertThat(message, doc, greaterThanOrEqualTo(range.minDoc()));
                        assertThat(message, doc, lessThan(range.maxDoc()));
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
        Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = ranges(
            SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, NUM_VALUES + 10, NUM_VALUES + 20)
        );
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(ranges.get(leaf), equalTo(new PrimarySortRange.DocRange(0, 0)));
        }
    }

    public void testClausesIntersect() throws IOException {
        index(randomBoolean());
        BooleanQuery both = new BooleanQuery.Builder().add(
            SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 0, 300),
            BooleanClause.Occur.FILTER
        ).add(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 100, 400), BooleanClause.Occur.MUST).build();
        Map<LeafReaderContext, PrimarySortRange.DocRange> bothRanges = ranges(both);
        Map<LeafReaderContext, PrimarySortRange.DocRange> oneRanges = ranges(
            SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 100, 300)
        );
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(bothRanges.get(leaf), equalTo(oneRanges.get(leaf)));
        }
    }

    /**
     * The index is sorted by {@link #SORT_FIELD}, but the filter is on {@code other}, so there is no range to find.
     */
    public void testFilterOnOtherFieldGivesNoRange() throws IOException {
        index(randomBoolean());
        assertThat(ranges(SortedNumericDocValuesField.newSlowRangeQuery("other", 1, 2)).keySet(), empty());
    }

    public void testNoIndexSort() throws IOException {
        index(null);
        assertThat(ranges(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 1, 2)).keySet(), empty());
    }

    /**
     * Only the clauses a doc must match count. A doc does not have to match a SHOULD or a MUST_NOT clause as a
     * range, so they must not narrow the result. Boost and constant score wrappers are looked through.
     */
    public void testOnlyRequiredClausesAreUsed() throws IOException {
        index(randomBoolean());
        Query required = SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 10, 12);
        Query query = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(new BoostQuery(required, 2f), BooleanClause.Occur.FILTER)
                .add(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 200, 300), BooleanClause.Occur.SHOULD)
                .add(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 0, 5), BooleanClause.Occur.MUST_NOT)
                .build()
        );
        Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = ranges(query);
        Map<LeafReaderContext, PrimarySortRange.DocRange> expected = ranges(required);
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(ranges.get(leaf), equalTo(expected.get(leaf)));
        }
        Query onlyOptional = new BooleanQuery.Builder().add(required, BooleanClause.Occur.SHOULD).build();
        assertThat(ranges(onlyOptional).keySet(), empty());
    }

    /**
     * Slices must cover every matching doc, be smaller than the whole leaves would give, and spread a narrow
     * range over more slices than the default partitioning does.
     */
    public void testSlicesCoverMatches() throws IOException {
        boolean reverse = randomBoolean();
        // One leaf, and a high concurrency so the leaf is large for the splitter: at least 5 times the slice size.
        index(reverse, NUM_DOCS);
        long lower = between(0, NUM_VALUES - 10);
        long upper = lower + between(0, 8);
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, lower, upper)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        int taskConcurrency = between(50, 200);
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

    /**
     * A range that covers almost all docs must not give slices larger than the default for the whole leaf.
     */
    public void testWideRangeSlicesAreNotLargerThanDefault() throws IOException {
        index(randomBoolean(), NUM_DOCS);
        int taskConcurrency = between(5, 20);
        int minDocsPerSlice = between(50, 500);
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 1, NUM_VALUES - 2)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        int defaultDesired = Math.max(minDocsPerSlice, Math.ceilDiv(NUM_DOCS, taskConcurrency));
        for (List<PartialLeafReaderContext> group : LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            taskConcurrency,
            weight,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            minDocsPerSlice
        )) {
            for (PartialLeafReaderContext slice : group) {
                assertThat(slice.maxDoc() - slice.minDoc(), lessThanOrEqualTo(defaultDesired * 5 / 4));
            }
        }
    }

    /**
     * Small leaves are grouped as before, so many small segments do not get a slice each.
     */
    public void testSmallLeavesKeepDefaultSlices() throws IOException {
        index(randomBoolean(), 500);
        assertThat(reader.leaves().size(), equalTo(NUM_DOCS / 500));
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 10, 12)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        int taskConcurrency = between(2, 20);
        int minDocsPerSlice = between(50, 500);
        assertThat(
            LuceneSliceQueue.PartitioningStrategy.DOC.groups(
                searcher,
                taskConcurrency,
                weight,
                LuceneSliceQueue.LeafSplitGuard.NEVER,
                minDocsPerSlice
            ),
            equalTo(
                LuceneSliceQueue.PartitioningStrategy.DOC.groups(
                    searcher,
                    taskConcurrency,
                    null,
                    LuceneSliceQueue.LeafSplitGuard.NEVER,
                    minDocsPerSlice
                )
            )
        );
    }

    /**
     * A leaf the guard keeps whole stays one full-leaf slice. Other large leaves are still cut by the range.
     */
    public void testKeptWholeLeafIsNotCut() throws IOException {
        index(randomBoolean(), NUM_DOCS / 2);
        assertThat(reader.leaves().size(), equalTo(2));
        LeafReaderContext whole = reader.leaves().get(0);
        LeafReaderContext other = reader.leaves().get(1);
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 10, 12)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        List<List<PartialLeafReaderContext>> groups = LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            between(50, 200),
            weight,
            (w, leaf) -> leaf == whole,
            between(50, 500)
        );
        long wholeDocs = 0;
        long otherDocs = 0;
        for (List<PartialLeafReaderContext> group : groups) {
            for (PartialLeafReaderContext slice : group) {
                if (slice.leafReaderContext() == whole) {
                    assertThat(slice, equalTo(new PartialLeafReaderContext(whole)));
                    wholeDocs += slice.maxDoc() - slice.minDoc();
                } else {
                    assertThat(slice.leafReaderContext(), equalTo(other));
                    otherDocs += slice.maxDoc() - slice.minDoc();
                }
            }
        }
        assertThat(wholeDocs, equalTo((long) whole.reader().maxDoc()));
        assertThat("the other leaf is cut to the range", otherDocs, lessThan((long) other.reader().maxDoc() / 2));
    }

    /**
     * Required clauses on the primary sort field are merged into one range. If the merged range is empty, no doc can match.
     */
    public void testContradictoryClausesGiveEmptyRange() throws IOException {
        index(randomBoolean());
        BooleanQuery query = new BooleanQuery.Builder().add(
            SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 0, 5),
            BooleanClause.Occur.FILTER
        ).add(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 10, 20), BooleanClause.Occur.MUST).build();
        Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = ranges(query);
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(ranges.get(leaf), equalTo(new PrimarySortRange.DocRange(0, 0)));
        }
    }

    public void testNoLeaves() {
        assertThat(PrimarySortRange.find(List.of(), SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 1, 2)).keySet(), empty());
    }

    /**
     * The range is cut into slices sized for the range, not for the whole leaf. Otherwise a narrow range lands in a few
     * large slices and most drivers have nothing to do.
     */
    public void testNarrowRangeIsCutIntoSmallSlices() throws IOException {
        index(randomBoolean(), NUM_DOCS);
        int taskConcurrency = 100;
        int minDocsPerSlice = 100;
        IndexSearcher searcher = new IndexSearcher(reader);
        long lower = between(0, NUM_VALUES - 10);
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, lower, lower + between(0, 8))),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        // The whole leaf would be cut into slices of about 1,000 docs. The range holds at most about 8,000 docs, so slices of at
        // most 100 docs (the minimum) are expected, plus the 25% the partitioner allows.
        long sliceDocs = 0;
        for (List<PartialLeafReaderContext> group : LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            taskConcurrency,
            weight,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            minDocsPerSlice
        )) {
            for (PartialLeafReaderContext slice : group) {
                assertThat(slice.maxDoc() - slice.minDoc(), lessThanOrEqualTo(minDocsPerSlice * 5 / 4));
                sliceDocs += slice.maxDoc() - slice.minDoc();
            }
        }
        assertThat(sliceDocs, greaterThan(0L));
    }

    /**
     * The slice size is sized for the matching docs of all ranged leaves together, like the default size is sized for all docs of
     * the shard. If every leaf were sized on its own, the number of slices would grow with the number of leaves.
     */
    public void testSliceCountDoesNotGrowWithTheNumberOfRangedLeaves() throws IOException {
        // Two leaves of 50,000 docs. With 20 drivers a slice of the whole shard is 5,000 docs, so both leaves are large.
        index(randomBoolean(), NUM_DOCS / 2);
        assertThat(reader.leaves().size(), equalTo(2));
        int taskConcurrency = 20;
        IndexSearcher searcher = new IndexSearcher(reader);
        // About half of the docs in each leaf match, so about 50,000 docs in total.
        Weight weight = searcher.createWeight(
            searcher.rewrite(SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, 0, NUM_VALUES / 2 - 1)),
            ScoreMode.COMPLETE_NO_SCORES,
            1
        );
        List<List<PartialLeafReaderContext>> groups = LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            taskConcurrency,
            weight,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            100
        );
        long leavesWithSlices = groups.stream().flatMap(List::stream).map(PartialLeafReaderContext::leafReaderContext).distinct().count();
        assertThat("both leaves are cut by their range", leavesWithSlices, equalTo(2L));
        // Sized together this is about 20 slices. Sized per leaf it would be about 40.
        assertThat(groups.size(), greaterThanOrEqualTo(taskConcurrency / 2));
        assertThat(groups.size(), lessThanOrEqualTo(taskConcurrency * 3 / 2));
    }
}

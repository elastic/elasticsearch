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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests {@link PrimarySortRange} and how {@link LuceneSliceQueue.PartitioningStrategy#DOC} uses it. The key
 * property is that the range, and the slices cut from it, never miss a matching doc. Every test that checks this reads
 * the value of every doc. Index sorted data is written in several segments, so the skipper search runs on more than
 * one leaf.
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
        Sort sort = reverse == null ? null : new Sort(new SortedNumericSortField(SORT_FIELD, SortField.Type.LONG, reverse));
        index(sort, docsPerSegment, doc -> {
            doc.add(SortedNumericDocValuesField.indexedField(SORT_FIELD, between(0, NUM_VALUES - 1)));
            doc.add(SortedNumericDocValuesField.indexedField("other", between(0, NUM_VALUES - 1)));
        });
    }

    /**
     * Writes {@code NUM_DOCS} docs with the fields that {@code fields} adds, in segments of {@code docsPerSegment} docs.
     */
    private void index(@Nullable Sort sort, int docsPerSegment, Consumer<Document> fields) throws IOException {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        config.setMaxBufferedDocs(docsPerSegment);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        if (sort != null) {
            config.setIndexSort(sort);
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                fields.accept(doc);
                writer.addDocument(doc);
            }
        }
        reader = DirectoryReader.open(directory);
    }

    private static Query range(long lower, long upper) {
        return SortedNumericDocValuesField.newSlowRangeQuery(SORT_FIELD, lower, upper);
    }

    private Map<LeafReaderContext, PrimarySortRange.DocRange> ranges(Query query) {
        return PrimarySortRange.find(reader.leaves(), query);
    }

    /**
     * The slices {@link LuceneSliceQueue.PartitioningStrategy#DOC} makes for {@code query}, or for no query if it is
     * {@code null}. No query is the slicing that does not know about any filter.
     */
    private List<List<PartialLeafReaderContext>> groups(@Nullable Query query, int taskConcurrency, int minDocsPerSlice)
        throws IOException {
        return groups(query, taskConcurrency, minDocsPerSlice, LuceneSliceQueue.LeafSplitGuard.NEVER);
    }

    private List<List<PartialLeafReaderContext>> groups(
        @Nullable Query query,
        int taskConcurrency,
        int minDocsPerSlice,
        LuceneSliceQueue.LeafSplitGuard guard
    ) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = query == null ? null : searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1);
        return LuceneSliceQueue.PartitioningStrategy.DOC.groups(searcher, taskConcurrency, weight, guard, minDocsPerSlice);
    }

    private void assertSlicedAsWithoutFilter(Query query, int taskConcurrency, int minDocsPerSlice) throws IOException {
        assertThat(groups(query, taskConcurrency, minDocsPerSlice), equalTo(groups(null, taskConcurrency, minDocsPerSlice)));
    }

    /**
     * Calls {@code match} for every doc whose value on {@link #SORT_FIELD} is in {@code [lower, upper]}.
     */
    private void forEachMatch(long lower, long upper, ObjIntConsumer<LeafReaderContext> match) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedNumericDocValues values = DocValues.getSortedNumeric(leaf.reader(), SORT_FIELD);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                assertTrue(values.advanceExact(doc));
                long value = values.nextValue();
                if (value >= lower && value <= upper) {
                    match.accept(leaf, doc);
                }
            }
        }
    }

    public void testRangeContainsEveryMatchAndIsNarrow() throws IOException {
        for (boolean reverse : new boolean[] { false, true }) {
            closeIndex();
            index(reverse);
            long lower = between(0, NUM_VALUES - 1);
            long upper = lower + between(0, 5);
            Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = ranges(range(lower, upper));
            for (LeafReaderContext leaf : reader.leaves()) {
                assertThat(ranges.get(leaf), notNullValue());
            }
            forEachMatch(lower, upper, (leaf, doc) -> {
                PrimarySortRange.DocRange range = ranges.get(leaf);
                String message = "reverse=" + reverse + " doc=" + doc + " range=" + range;
                assertThat(message, doc, greaterThanOrEqualTo(range.minDoc()));
                assertThat(message, doc, lessThan(range.maxDoc()));
            });
            // 6 of 400 values match at most, so the ranges must be much smaller than the data. Allow for skipper block rounding.
            long rangeDocs = ranges.values().stream().mapToLong(PrimarySortRange.DocRange::length).sum();
            assertThat("reverse=" + reverse, rangeDocs, lessThan((long) NUM_DOCS / 2));
        }
    }

    public void testEmptyRange() throws IOException {
        index(randomBoolean());
        Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = ranges(range(NUM_VALUES + 10, NUM_VALUES + 20));
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(ranges.get(leaf), equalTo(new PrimarySortRange.DocRange(0, 0)));
        }
    }

    /**
     * Required clauses on the primary sort field are merged into one range. Clauses that overlap give the range of their
     * overlap. Clauses that do not overlap give an empty range, because no doc can match.
     */
    public void testRequiredClausesAreMerged() throws IOException {
        index(randomBoolean());
        BooleanQuery overlapping = new BooleanQuery.Builder().add(range(0, 300), BooleanClause.Occur.FILTER)
            .add(range(100, 400), BooleanClause.Occur.MUST)
            .build();
        Map<LeafReaderContext, PrimarySortRange.DocRange> overlappingRanges = ranges(overlapping);
        Map<LeafReaderContext, PrimarySortRange.DocRange> overlapRanges = ranges(range(100, 300));
        BooleanQuery disjoint = new BooleanQuery.Builder().add(range(0, 5), BooleanClause.Occur.FILTER)
            .add(range(10, 20), BooleanClause.Occur.MUST)
            .build();
        Map<LeafReaderContext, PrimarySortRange.DocRange> disjointRanges = ranges(disjoint);
        for (LeafReaderContext leaf : reader.leaves()) {
            assertThat(overlappingRanges.get(leaf), equalTo(overlapRanges.get(leaf)));
            assertThat(disjointRanges.get(leaf), equalTo(new PrimarySortRange.DocRange(0, 0)));
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
        assertThat(ranges(range(1, 2)).keySet(), empty());
    }

    public void testNoLeaves() {
        assertThat(PrimarySortRange.find(List.of(), range(1, 2)).keySet(), empty());
    }

    /**
     * A doc with several values does not sit at one place in the sort order, so the range of values does not give a range of
     * docs. The leaf must not give a range, even though it has a skipper.
     */
    public void testMultiValuedSortFieldGivesNoRange() throws IOException {
        index(new Sort(new SortedNumericSortField(SORT_FIELD, SortField.Type.LONG, randomBoolean())), NUM_DOCS, doc -> {
            doc.add(SortedNumericDocValuesField.indexedField(SORT_FIELD, between(0, NUM_VALUES - 1)));
            doc.add(SortedNumericDocValuesField.indexedField(SORT_FIELD, between(0, NUM_VALUES - 1)));
        });
        assertThat(ranges(range(1, 2)).keySet(), empty());
    }

    /**
     * Only {@code INT} and {@code LONG} sort fields are supported. A range of the stored values means nothing for a double.
     */
    public void testUnsupportedSortTypeGivesNoRange() throws IOException {
        index(new Sort(new SortedNumericSortField(SORT_FIELD, SortField.Type.DOUBLE, randomBoolean())), NUM_DOCS, doc -> {
            doc.add(SortedNumericDocValuesField.indexedField(SORT_FIELD, NumericUtils.doubleToSortableLong(between(0, NUM_VALUES - 1))));
        });
        assertThat(ranges(range(1, 2)).keySet(), empty());
    }

    /**
     * Without a skipper the range cannot be found cheaply, so the leaf does not give one.
     */
    public void testMissingSkipperGivesNoRange() throws IOException {
        index(new Sort(new SortedNumericSortField(SORT_FIELD, SortField.Type.LONG, randomBoolean())), NUM_DOCS, doc -> {
            doc.add(new SortedNumericDocValuesField(SORT_FIELD, between(0, NUM_VALUES - 1)));
        });
        assertThat(ranges(range(1, 2)).keySet(), empty());
    }

    /**
     * Only the clauses a doc must match count. A doc does not have to match a SHOULD or a MUST_NOT clause as a
     * range, so they must not narrow the result. Boost and constant score wrappers are looked through.
     */
    public void testOnlyRequiredClausesAreUsed() throws IOException {
        index(randomBoolean());
        Query required = range(10, 12);
        Query query = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(new BoostQuery(required, 2f), BooleanClause.Occur.FILTER)
                .add(range(200, 300), BooleanClause.Occur.SHOULD)
                .add(range(0, 5), BooleanClause.Occur.MUST_NOT)
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
     * The slices cover every matching doc. They are sized for the range, not for the whole leaf: the whole leaf would be cut
     * into slices of about 1,000 docs, the range holds at most about 8,000 docs, so slices of at most 100 docs (the minimum) are
     * expected, plus the 25% the partitioner allows. Otherwise a narrow range lands in a few large slices and most drivers
     * have nothing to do.
     */
    public void testNarrowRangeIsCutIntoSmallSlicesThatCoverEveryMatch() throws IOException {
        // One leaf, and a high concurrency so the leaf is large for the splitter: at least 5 times the slice size.
        index(randomBoolean(), NUM_DOCS);
        int minDocsPerSlice = 100;
        long lower = between(0, NUM_VALUES - 10);
        long upper = lower + between(0, 8);
        List<List<PartialLeafReaderContext>> groups = groups(range(lower, upper), 100, minDocsPerSlice);
        List<PartialLeafReaderContext> slices = groups.stream().flatMap(List::stream).toList();
        forEachMatch(
            lower,
            upper,
            (leaf, doc) -> assertTrue(
                "doc " + doc + " of leaf " + leaf.ord + " is in no slice",
                slices.stream().anyMatch(s -> s.leafReaderContext() == leaf && doc >= s.minDoc() && doc < s.maxDoc())
            )
        );
        long sliceDocs = 0;
        for (PartialLeafReaderContext slice : slices) {
            assertThat(slice.minDoc(), lessThan(slice.maxDoc()));
            assertThat(slice.maxDoc(), lessThanOrEqualTo(slice.leafReaderContext().reader().maxDoc()));
            assertThat(slice.maxDoc() - slice.minDoc(), lessThanOrEqualTo(minDocsPerSlice * 5 / 4));
            sliceDocs += slice.maxDoc() - slice.minDoc();
        }
        assertThat("slices must skip the docs that cannot match", sliceDocs, lessThan((long) NUM_DOCS / 2));
        assertThat(groups.size(), greaterThan(1));
    }

    /**
     * A range that covers almost all docs must not give slices larger than the default for the whole leaf.
     */
    public void testWideRangeSlicesAreNotLargerThanDefault() throws IOException {
        index(randomBoolean(), NUM_DOCS);
        int taskConcurrency = between(5, 20);
        int minDocsPerSlice = between(50, 500);
        int defaultDesired = Math.max(minDocsPerSlice, Math.ceilDiv(NUM_DOCS, taskConcurrency));
        for (List<PartialLeafReaderContext> group : groups(range(1, NUM_VALUES - 2), taskConcurrency, minDocsPerSlice)) {
            for (PartialLeafReaderContext slice : group) {
                assertThat(slice.maxDoc() - slice.minDoc(), lessThanOrEqualTo(defaultDesired * 5 / 4));
            }
        }
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
        // About half of the docs in each leaf match, so about 50,000 docs in total.
        List<List<PartialLeafReaderContext>> groups = groups(range(0, NUM_VALUES / 2 - 1), taskConcurrency, 100);
        long leavesWithSlices = groups.stream().flatMap(List::stream).map(PartialLeafReaderContext::leafReaderContext).distinct().count();
        assertThat("both leaves are cut by their range", leavesWithSlices, equalTo(2L));
        // Sized together this is about 20 slices. Sized per leaf it would be about 40.
        assertThat(groups.size(), greaterThanOrEqualTo(taskConcurrency / 2));
        assertThat(groups.size(), lessThanOrEqualTo(taskConcurrency * 3 / 2));
    }

    /**
     * A filter on another field gives no range, so the leaves are sliced as before.
     */
    public void testFilterOnOtherFieldKeepsDefaultSlices() throws IOException {
        index(randomBoolean(), NUM_DOCS);
        assertSlicedAsWithoutFilter(SortedNumericDocValuesField.newSlowRangeQuery("other", 1, 2), between(5, 20), between(50, 500));
    }

    /**
     * Small leaves are grouped as before, so many small segments do not get a slice each.
     */
    public void testSmallLeavesKeepDefaultSlices() throws IOException {
        index(randomBoolean(), 500);
        assertThat(reader.leaves().size(), equalTo(NUM_DOCS / 500));
        assertSlicedAsWithoutFilter(range(10, 12), between(2, 20), between(50, 500));
    }

    /**
     * A leaf the guard keeps whole stays one full-leaf slice. Other large leaves are still cut by the range.
     */
    public void testKeptWholeLeafIsNotCut() throws IOException {
        index(randomBoolean(), NUM_DOCS / 2);
        assertThat(reader.leaves().size(), equalTo(2));
        LeafReaderContext whole = reader.leaves().get(0);
        LeafReaderContext other = reader.leaves().get(1);
        List<List<PartialLeafReaderContext>> groups = groups(range(10, 12), between(50, 200), between(50, 500), (w, leaf) -> leaf == whole);
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
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.elasticsearch.lucene.comparators.XLongComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A sorted top N over a stub segment, collected in slices by several collectors the way ES|QL drivers do it, must give the rows that a
 * scan gives. The collection prunes the blocks of the skipper whose values cannot be competitive.
 */
public class ParquetSortPruningTests extends StubSegmentTestCase {

    private static final int HITS = 10;

    /** A sort field on a long column, with the comparator that ES uses, so that the collection uses the skipper. */
    private static SortField longField(String name) {
        return new SortField(name, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldName, int numHits, Pruning pruning, boolean reversed) {
                // the hit queue decides the pruning: it prunes the first field of a sort by several fields only for values above the bottom
                return new XLongComparator(numHits, fieldName, null, reversed, pruning);
            }
        });
    }

    /**
     * The rows of the sorted top N must be those of a scan, whatever the number of collectors that share the segment, the size of the
     * slices that they take, and the chunks in which they score them. Each slice gets a leaf collector of its own.
     */
    public void testSortedTopNWithSlicesGivesWhatAScanGives() throws IOException {
        runAttempts(true);
    }

    private boolean freshLeafCollectorPerSlice;

    private void runAttempts(boolean freshPerSlice) throws IOException {
        freshLeafCollectorPerSlice = freshPerSlice;
        for (int attempt = 0; attempt < 20; attempt++) {
            int rowGroupRows = randomIntBetween(1_500, 9_000);
            int rows = rowGroupRows * randomIntBetween(4, 9) + randomIntBetween(0, rowGroupRows - 1);
            long[] t = new long[rows];
            int[] u = new int[rows];
            // some row groups have small times that tie a lot, as the first rows of a time column do, and others have a wide range
            for (int i = 0; i < rows; i++) {
                boolean narrow = (i / rowGroupRows) % 3 == 0;
                t[i] = narrow ? randomLongBetween(0, 12) : randomLongBetween(0, 400);
                u[i] = randomIntBetween(0, 50);
            }
            BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeTimes(t, u, rowGroupRows));
            List<ColumnBinding> bindings = List.of(
                new ColumnBinding("t", 0, "t", DocValuesType.NUMERIC),
                new ColumnBinding("u", 1, "u", DocValuesType.NUMERIC)
            );
            long[] times = t;
            int[] others = u;
            withSegment(file, bindings, leaf -> checkTopN(leaf, times, others));
        }
    }

    private void checkTopN(LeafReader leaf, long[] t, int[] u) throws IOException {
        int rows = t.length;
        int filterBelow = randomBoolean() ? Integer.MAX_VALUE : randomIntBetween(5, 50);
        Query query = NumericDocValuesField.newSlowRangeQuery("u", 0, filterBelow);
        // the rows that a scan finds: the HITS smallest by t and then u
        List<long[]> expected = new ArrayList<>();
        for (int doc = 0; doc < rows; doc++) {
            if (u[doc] <= filterBelow) {
                expected.add(new long[] { t[doc], u[doc] });
            }
        }
        expected.sort(Comparator.<long[]>comparingLong(a -> a[0]).thenComparingLong(a -> a[1]));
        expected = expected.subList(0, Math.min(HITS, expected.size()));

        IndexSearcher searcher = new IndexSearcher(leaf);
        Sort sort = new Sort(longField("t"), longField("u"));
        TopFieldCollectorManager manager = new TopFieldCollectorManager(sort, HITS, null, 0);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        LeafReaderContext context = leaf.getContext();

        int drivers = randomIntBetween(1, 4);
        List<TopFieldCollector> collectors = new ArrayList<>();
        LeafCollector[] leafCollectors = new LeafCollector[drivers];
        for (int d = 0; d < drivers; d++) {
            collectors.add(manager.newCollector());
        }
        int sliceSize = randomIntBetween(500, Math.max(501, rows / 6));
        int chunk = randomIntBetween(64, 4_096);
        // the slices in the order in which the drivers take them: the next one, or one that is below the last one when it is stolen
        List<Integer> starts = new ArrayList<>();
        for (int start = 0; start < rows; start += sliceSize) {
            starts.add(start);
        }
        java.util.Collections.shuffle(starts, random());
        for (int start : starts) {
            int end = Math.min(rows, start + sliceSize);
            int driver = randomIntBetween(0, drivers - 1);
            if (leafCollectors[driver] == null || freshLeafCollectorPerSlice) {
                leafCollectors[driver] = collectors.get(driver).getLeafCollector(context);
            }
            // a new scorer for every slice, and a call of the same leaf collector for every chunk of the slice
            BulkScorer scorer = weight.bulkScorer(context);
            if (scorer == null) {
                continue;
            }
            // like ES|QL: the next position is what the scorer returns, which is the next document that matches and is competitive
            for (int position = start; position < end;) {
                position = scorer.score(leafCollectors[driver], leaf.getLiveDocs(), position, Math.min(end, position + chunk));
            }
        }
        TopFieldDocs[] perDriver = new TopFieldDocs[drivers];
        for (int d = 0; d < drivers; d++) {
            perDriver[d] = collectors.get(d).topDocs();
        }
        TopFieldDocs merged = TopDocs.merge(sort, HITS, perDriver);

        List<long[]> actual = new ArrayList<>();
        for (var hit : merged.scoreDocs) {
            Object[] fields = ((org.apache.lucene.search.FieldDoc) hit).fields;
            actual.add(new long[] { (Long) fields[0], (Long) fields[1] });
        }
        assertEquals(
            "rows=" + rows + " drivers=" + drivers + " slice=" + sliceSize + " chunk=" + chunk + " filter<=" + filterBelow,
            describe(expected),
            describe(actual)
        );
        // and the values that the segment serves are the values of the file
        NumericDocValues values = leaf.getNumericDocValues("t");
        for (int doc = 0; doc < rows; doc += randomIntBetween(1, 997)) {
            assertTrue(values.advanceExact(doc));
            assertEquals(t[doc], values.longValue());
        }
    }

    /**
     * The driver loop of ES|QL on a segment, which is the order in which the sort sees the documents there: slices of the segment that
     * the drivers claim from a shared queue (the next one of the last slice, or the first that is free), one bulk scorer and one leaf
     * collector for the many slices of a driver, the next position that the scorer returns, chunks of 4096 documents, and, when the
     * driver continues on another thread, a new bulk scorer and a new leaf collector in the middle of the segment. The query keeps the
     * documents that have a value, as {@code WHERE SearchPhrase != ""} does, which is a filter and a must-not clause.
     */
    public void testTheDriverLoopOfEsqlOverManySlicesGivesWhatAScanGives() throws IOException {
        for (int attempt = 0; attempt < 25; attempt++) {
            int rowGroupRows = randomIntBetween(3_000, 12_000);
            int rows = rowGroupRows * randomIntBetween(10, 20) + randomIntBetween(0, rowGroupRows - 1);
            long[] t = new long[rows];
            int[] u = new int[rows];
            for (int i = 0; i < rows; i++) {
                t[i] = 1_000 + randomLongBetween(0, 1_000_000);
                // about one document in eight has a value, as the search phrases of the ClickBench file
                u[i] = randomIntBetween(0, 7) == 0 ? randomIntBetween(1, 50) : 0;
            }
            for (int k = 0; k < 30; k++) {
                int i = randomIntBetween(0, rows - 1);
                t[i] = randomIntBetween(0, 1);
                u[i] = randomIntBetween(1, 50);
            }
            BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeTimes(t, u, rowGroupRows));
            List<ColumnBinding> bindings = List.of(
                new ColumnBinding("t", 0, "t", DocValuesType.NUMERIC),
                new ColumnBinding("u", 1, "u", DocValuesType.NUMERIC)
            );
            long[] times = t;
            int[] others = u;
            int sliceSize = randomIntBetween(1_500, 6_000);
            int drivers = randomIntBetween(2, 6);
            double threadHop = randomFrom(0.0, 0.0, 0.05, 0.3);
            // small chunks give a leaf collector many scoring calls, past the 256 updates after which the sort refreshes its iterator
            // rarely
            int chunkSize = randomFrom(64, 128, 512, 4_096);
            withSegment(file, bindings, leaf -> checkDriverLoop(leaf, times, others, sliceSize, drivers, threadHop, chunkSize));
        }
    }

    private static final class DriverModel {
        final TopFieldCollector collector;
        LeafCollector leafCollector;
        BulkScorer bulkScorer;
        int position;
        int maxPosition;
        int slice = -1;
        boolean finished;

        DriverModel(TopFieldCollector collector) {
            this.collector = collector;
        }
    }

    private void checkDriverLoop(LeafReader leaf, long[] t, int[] u, int sliceSize, int drivers, double threadHop, int chunkSize)
        throws IOException {
        int rows = t.length;
        List<long[]> expected = new ArrayList<>();
        for (int doc = 0; doc < rows; doc++) {
            if (u[doc] != 0) {
                expected.add(new long[] { t[doc], u[doc] });
            }
        }
        expected.sort(Comparator.<long[]>comparingLong(a -> a[0]).thenComparingLong(a -> a[1]));
        expected = expected.subList(0, Math.min(HITS, expected.size()));

        IndexSearcher searcher = new IndexSearcher(leaf);
        Query query = new org.apache.lucene.search.BooleanQuery.Builder().add(
            NumericDocValuesField.newSlowRangeQuery("u", 0, Long.MAX_VALUE),
            org.apache.lucene.search.BooleanClause.Occur.FILTER
        ).add(NumericDocValuesField.newSlowExactQuery("u", 0), org.apache.lucene.search.BooleanClause.Occur.MUST_NOT).build();
        Sort sort = new Sort(longField("t"), longField("u"));
        TopFieldCollectorManager manager = new TopFieldCollectorManager(sort, HITS, null, 0);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        LeafReaderContext context = leaf.getContext();

        int sliceCount = (rows + sliceSize - 1) / sliceSize;
        boolean[] taken = new boolean[sliceCount];
        int[] head = new int[] { 0 };
        List<DriverModel> models = new ArrayList<>();
        for (int d = 0; d < drivers; d++) {
            models.add(new DriverModel(manager.newCollector()));
        }
        List<DriverModel> running = new ArrayList<>(models);
        while (running.isEmpty() == false) {
            DriverModel driver = running.get(randomIntBetween(0, running.size() - 1));
            while (driver.slice == -1 || driver.position >= driver.maxPosition) {
                int next = -1;
                if (driver.slice >= 0 && driver.slice + 1 < sliceCount && taken[driver.slice + 1] == false) {
                    next = driver.slice + 1;
                } else {
                    while (head[0] < sliceCount && taken[head[0]]) {
                        head[0]++;
                    }
                    if (head[0] < sliceCount) {
                        next = head[0];
                    }
                }
                if (next < 0) {
                    driver.finished = true;
                    break;
                }
                taken[next] = true;
                driver.slice = next;
                if (driver.bulkScorer == null) {
                    driver.bulkScorer = weight.bulkScorer(context);
                    driver.leafCollector = driver.collector.getLeafCollector(context);
                    if (driver.bulkScorer == null) {
                        driver.finished = true;
                        break;
                    }
                }
                driver.maxPosition = Math.min(rows, (next + 1) * sliceSize);
                driver.position = Math.max(driver.position, next * sliceSize);
            }
            if (driver.finished) {
                running.remove(driver);
                continue;
            }
            if (random().nextDouble() < threadHop) {
                // the driver continues on another thread: a new bulk scorer and a new leaf collector, at the same position
                driver.bulkScorer = weight.bulkScorer(context);
                driver.leafCollector = driver.collector.getLeafCollector(context);
            }
            int numDocs = Math.min(driver.maxPosition - driver.position, chunkSize);
            driver.position = driver.bulkScorer.score(
                driver.leafCollector,
                leaf.getLiveDocs(),
                driver.position,
                Math.min(driver.maxPosition, driver.position + numDocs)
            );
        }
        TopFieldDocs[] perDriver = new TopFieldDocs[drivers];
        for (int d = 0; d < drivers; d++) {
            perDriver[d] = models.get(d).collector.topDocs();
        }
        TopFieldDocs merged = TopDocs.merge(sort, HITS, perDriver);
        List<long[]> actual = new ArrayList<>();
        for (var hit : merged.scoreDocs) {
            Object[] fields = ((org.apache.lucene.search.FieldDoc) hit).fields;
            actual.add(new long[] { (Long) fields[0], (Long) fields[1] });
        }
        assertEquals(
            "rows="
                + rows
                + " drivers="
                + drivers
                + " slice="
                + sliceSize
                + " slices="
                + sliceCount
                + " threadHop="
                + threadHop
                + " chunk="
                + chunkSize,
            describe(expected),
            describe(actual)
        );
    }

    private static String describe(List<long[]> rows) {
        StringBuilder text = new StringBuilder();
        for (long[] row : rows) {
            text.append('(').append(row[0]).append(',').append(row[1]).append(") ");
        }
        return text.toString();
    }
}

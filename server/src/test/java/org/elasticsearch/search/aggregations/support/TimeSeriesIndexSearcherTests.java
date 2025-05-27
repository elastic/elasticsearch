/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.IndexSortConfig.TIME_SERIES_SORT;
import static org.hamcrest.Matchers.greaterThan;

public class TimeSeriesIndexSearcherTests extends ESTestCase {

    // Index a random set of docs with timestamp and tsid with the tsid/timestamp sort order
    // Open a searcher over a set of leaves
    // Collection should be in order

    public void testCollectInOrderAcrossSegments() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter iw = getIndexWriter(dir);

        AtomicInteger clock = new AtomicInteger(0);

        final int THREADS = 5;
        final int DOC_COUNTS = 500;
        ExecutorService indexer = Executors.newFixedThreadPool(THREADS);
        try {
            for (int i = 0; i < THREADS; i++) {
                indexer.submit(() -> {
                    Document doc = new Document();
                    for (int j = 0; j < DOC_COUNTS; j++) {
                        int tsid = randomIntBetween(1, 30);
                        long time = clock.addAndGet(randomIntBetween(0, 10));
                        doc.clear();
                        doc.add(new NumericDocValuesField(TimeSeriesIdFieldMapper.NAME, tsid));
                        doc.add(new NumericDocValuesField(DataStream.TIMESTAMP_FIELD_NAME, time));
                        try {
                            iw.addDocument(doc);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
            }
        } finally {
            terminate(indexer);
        }
        iw.close();

        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = newSearcher(reader);

        TimeSeriesIndexSearcher indexSearcher = new TimeSeriesIndexSearcher(searcher, null, List.of());

        BucketCollector collector = getBucketCollector(THREADS * DOC_COUNTS);

        indexSearcher.search(new MatchAllDocsQuery(), collector);
        collector.postCollection();

        reader.close();
        dir.close();
    }

    public void testCollectMinScoreAcrossSegments() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter iw = getIndexWriter(dir);

        AtomicInteger clock = new AtomicInteger(0);

        final int DOC_COUNTS = 5;
        Document doc = new Document();
        for (int j = 0; j < DOC_COUNTS; j++) {
            int tsid = j % 30 + 1;
            long time = clock.addAndGet(j % 10);
            doc.clear();
            doc.add(new NumericDocValuesField(TimeSeriesIdFieldMapper.NAME, tsid));
            doc.add(new NumericDocValuesField(DataStream.TIMESTAMP_FIELD_NAME, time));
            try {
                iw.addDocument(doc);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        iw.close();

        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = newSearcher(reader);

        TimeSeriesIndexSearcher indexSearcher = new TimeSeriesIndexSearcher(searcher, null, List.of());
        indexSearcher.setMinimumScore(2f);

        {
            var collector = new TimeSeriesCancellationTests.CountingBucketCollector();
            var query = new BoostQuery(new MatchAllDocsQuery(), 2f);
            indexSearcher.search(query, collector);
            assertEquals(collector.count.get(), DOC_COUNTS);
        }
        {
            var collector = new TimeSeriesCancellationTests.CountingBucketCollector();
            var query = new BoostQuery(new MatchAllDocsQuery(), 1f);
            indexSearcher.search(query, collector);
            assertEquals(collector.count.get(), 0);
        }

        reader.close();
        dir.close();
    }

    /**
     * this test fixed the wrong init value of tsidOrd
     * See https://github.com/elastic/elasticsearch/issues/85711
     */
    public void testCollectFromMiddle() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter iw = getIndexWriter(dir);

        Document doc = new Document();
        final int DOC_COUNTS = 500;

        // segment 1
        // pre add a value
        doc.add(new NumericDocValuesField(TimeSeriesIdFieldMapper.NAME, 1111));
        doc.add(new NumericDocValuesField(DataStream.TIMESTAMP_FIELD_NAME, 1));
        iw.addDocument(doc);

        // segment 1 add value, timestamps are all larger than segment 2
        for (int j = 0; j < DOC_COUNTS; j++) {
            doc.clear();
            doc.add(new NumericDocValuesField(TimeSeriesIdFieldMapper.NAME, randomIntBetween(1, 2)));
            doc.add(new NumericDocValuesField(DataStream.TIMESTAMP_FIELD_NAME, randomIntBetween(20, 25)));
            try {
                iw.addDocument(doc);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        iw.commit();

        // segment 2
        // pre add a value
        doc.clear();
        doc.add(new NumericDocValuesField(TimeSeriesIdFieldMapper.NAME, 1111));
        doc.add(new NumericDocValuesField(DataStream.TIMESTAMP_FIELD_NAME, 1));
        iw.addDocument(doc);
        for (int j = 0; j < DOC_COUNTS; j++) {
            doc.clear();
            doc.add(new NumericDocValuesField(TimeSeriesIdFieldMapper.NAME, randomIntBetween(1, 2)));
            doc.add(new NumericDocValuesField(DataStream.TIMESTAMP_FIELD_NAME, randomIntBetween(10, 15)));
            try {
                iw.addDocument(doc);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        iw.close();
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = newSearcher(reader);

        TimeSeriesIndexSearcher indexSearcher = new TimeSeriesIndexSearcher(searcher, null, List.of());

        BucketCollector collector = getBucketCollector(2 * DOC_COUNTS);

        // skip the first doc of segment 1 and 2
        indexSearcher.search(NumericDocValuesField.newSlowSetQuery("_tsid", 1, 2), collector);
        collector.postCollection();

        reader.close();
        dir.close();
    }

    private RandomIndexWriter getIndexWriter(Directory dir) throws IOException {

        IndexWriterConfig iwc = newIndexWriterConfig();
        boolean tsidReverse = TIME_SERIES_SORT[0].getOrder() == SortOrder.DESC;
        boolean timestampReverse = TIME_SERIES_SORT[1].getOrder() == SortOrder.DESC;
        Sort sort = new Sort(
            new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.LONG, tsidReverse),
            new SortField(DataStream.TIMESTAMP_FIELD_NAME, SortField.Type.LONG, timestampReverse)
        );
        iwc.setIndexSort(sort);
        return new RandomIndexWriter(random(), dir, iwc);
    }

    private BucketCollector getBucketCollector(long totalCount) {
        return new BucketCollector() {

            final boolean tsidReverse = TIME_SERIES_SORT[0].getOrder() == SortOrder.DESC;
            final boolean timestampReverse = TIME_SERIES_SORT[1].getOrder() == SortOrder.DESC;
            long currentTSID = 0;
            long currentTimestamp = 0;
            long total = 0;

            @Override
            public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
                NumericDocValues tsid = DocValues.getNumeric(aggCtx.getLeafReaderContext().reader(), TimeSeriesIdFieldMapper.NAME);
                NumericDocValues timestamp = DocValues.getNumeric(aggCtx.getLeafReaderContext().reader(), DataStream.TIMESTAMP_FIELD_NAME);

                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        assertTrue(tsid.advanceExact(doc));
                        assertTrue(timestamp.advanceExact(doc));
                        long latestTSID = tsid.longValue();
                        long latestTimestamp = timestamp.longValue();

                        byte[] bytes = new byte[8];
                        ByteUtils.writeLongLE(latestTSID, bytes, 0);
                        assertEquals(latestTimestamp, aggCtx.getTimestamp());

                        if (currentTSID != 0) {
                            assertTrue(
                                currentTSID + "->" + latestTSID,
                                tsidReverse ? latestTSID <= currentTSID : latestTSID >= currentTSID
                            );
                            if (latestTSID == currentTSID) {
                                assertTrue(
                                    currentTimestamp + "->" + latestTimestamp,
                                    timestampReverse ? latestTimestamp <= currentTimestamp : latestTimestamp >= currentTimestamp
                                );
                                assertEquals(currentTSID, aggCtx.getTsid());
                            } else {
                                assertThat(aggCtx.getTsid(), greaterThan(currentTSID));
                            }
                        }
                        currentTimestamp = latestTimestamp;
                        currentTSID = latestTSID;
                        total++;
                    }
                };
            }

            @Override
            public void preCollection() {

            }

            @Override
            public void postCollection() {
                assertEquals(totalCount, total);
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
            }
        };
    }
}

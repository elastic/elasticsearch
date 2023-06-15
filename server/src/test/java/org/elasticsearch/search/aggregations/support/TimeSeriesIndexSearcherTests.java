/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.IndexSortConfig.TIME_SERIES_SORT;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class TimeSeriesIndexSearcherTests extends ESTestCase {

    // Index a random set of docs with timestamp and tsid with the tsid/timestamp sort order
    // Open a searcher over a set of leaves
    // Collection should be in order

    public void testCollectInOrderAcrossSegments() throws IOException, InterruptedException {
        Directory dir = newDirectory();
        RandomIndexWriter iw = getIndexWriter(dir);

        AtomicInteger clock = new AtomicInteger(0);

        final int THREADS = 5;
        final int DOC_COUNTS = 500;
        ExecutorService indexer = Executors.newFixedThreadPool(THREADS);
        for (int i = 0; i < THREADS; i++) {
            indexer.submit(() -> {
                Document doc = new Document();
                for (int j = 0; j < DOC_COUNTS; j++) {
                    String tsid = "tsid" + randomIntBetween(0, 30);
                    long time = clock.addAndGet(randomIntBetween(0, 10));
                    doc.clear();
                    doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(tsid)));
                    doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, time));
                    try {
                        iw.addDocument(doc);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        }
        indexer.shutdown();
        assertTrue(indexer.awaitTermination(30, TimeUnit.SECONDS));
        iw.close();

        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        TimeSeriesIndexSearcher indexSearcher = new TimeSeriesIndexSearcher(searcher, List.of());

        BucketCollector collector = getBucketCollector(THREADS * DOC_COUNTS);

        indexSearcher.search(new MatchAllDocsQuery(), collector);
        collector.postCollection();

        reader.close();
        dir.close();
    }

    public void testCollectMinScoreAcrossSegments() throws IOException, InterruptedException {
        Directory dir = newDirectory();
        RandomIndexWriter iw = getIndexWriter(dir);

        AtomicInteger clock = new AtomicInteger(0);

        final int DOC_COUNTS = 5;
        Document doc = new Document();
        for (int j = 0; j < DOC_COUNTS; j++) {
            String tsid = "tsid" + j % 30;
            long time = clock.addAndGet(j % 10);
            doc.clear();
            doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(tsid)));
            doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, time));
            try {
                iw.addDocument(doc);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        iw.close();

        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        TimeSeriesIndexSearcher indexSearcher = new TimeSeriesIndexSearcher(searcher, List.of());
        indexSearcher.setMinimumScore(0.99f);

        // Total count including arbitrary score filtering.
        BucketCollector collector = getBucketCollector(1);

        ScoreFunction scoreFunction = new ScoreFunction(CombineFunction.REPLACE) {
            @Override
            public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
                return new LeafScoreFunction() {

                    @Override
                    public double score(int docId, float subQueryScore) throws IOException {
                        return docId == 1 ? 1 : 0;
                    }

                    @Override
                    public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                        return null;
                    }
                };
            }

            @Override
            public boolean needsScores() {
                return false;
            }

            @Override
            protected boolean doEquals(ScoreFunction other) {
                return false;
            }

            @Override
            protected int doHashCode() {
                return 0;
            }
        };

        ScoreFunctionBuilder scoreFunctionBuilder = new ScoreFunctionBuilder() {
            @Override
            protected void doWriteTo(StreamOutput out) throws IOException {

            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            protected void doXContent(XContentBuilder builder, Params params) throws IOException {

            }

            @Override
            protected boolean doEquals(ScoreFunctionBuilder functionBuilder) {
                return false;
            }

            @Override
            protected int doHashCode() {
                return 0;
            }

            @Override
            protected ScoreFunction doToFunction(SearchExecutionContext context) throws IOException {
                return scoreFunction;
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return null;
            }
        };

        FunctionScoreQueryBuilder functionScoreQuery = new FunctionScoreQueryBuilder(new MatchAllQueryBuilder(), scoreFunctionBuilder);
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        indexSearcher.search(functionScoreQuery.toQuery(searchExecutionContext), collector);
        collector.postCollection();

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
        doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef("tsid")));
        doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, 1));
        iw.addDocument(doc);

        // segment 1 add value, timestamp is all large then segment 2
        for (int j = 0; j < DOC_COUNTS; j++) {
            String tsid = "tsid" + randomIntBetween(0, 1);
            doc.clear();
            doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(tsid)));
            doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, randomIntBetween(20, 25)));
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
        doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef("tsid")));
        doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, 1));
        iw.addDocument(doc);
        for (int j = 0; j < DOC_COUNTS; j++) {
            String tsid = "tsid" + randomIntBetween(0, 1);
            doc.clear();
            doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(tsid)));
            doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, randomIntBetween(10, 15)));
            try {
                iw.addDocument(doc);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        iw.close();
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        TimeSeriesIndexSearcher indexSearcher = new TimeSeriesIndexSearcher(searcher, List.of());

        BucketCollector collector = getBucketCollector(2 * DOC_COUNTS);

        // skip the first doc of segment 1 and 2
        indexSearcher.search(SortedSetDocValuesField.newSlowSetQuery("_tsid", new BytesRef("tsid0"), new BytesRef("tsid1")), collector);
        collector.postCollection();

        reader.close();
        dir.close();
    }

    private RandomIndexWriter getIndexWriter(Directory dir) throws IOException {

        IndexWriterConfig iwc = newIndexWriterConfig();
        boolean tsidReverse = TIME_SERIES_SORT[0].getOrder() == SortOrder.DESC;
        boolean timestampReverse = TIME_SERIES_SORT[1].getOrder() == SortOrder.DESC;
        Sort sort = new Sort(
            new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, tsidReverse),
            new SortField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, SortField.Type.LONG, timestampReverse)
        );
        iwc.setIndexSort(sort);
        return new RandomIndexWriter(random(), dir, iwc);
    }

    private BucketCollector getBucketCollector(long totalCount) {
        return new BucketCollector() {

            final boolean tsidReverse = TIME_SERIES_SORT[0].getOrder() == SortOrder.DESC;
            final boolean timestampReverse = TIME_SERIES_SORT[1].getOrder() == SortOrder.DESC;
            BytesRef currentTSID = null;
            int currentTSIDord = -1;
            long currentTimestamp = 0;
            long total = 0;

            @Override
            public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
                SortedDocValues tsid = DocValues.getSorted(aggCtx.getLeafReaderContext().reader(), TimeSeriesIdFieldMapper.NAME);
                NumericDocValues timestamp = DocValues.getNumeric(
                    aggCtx.getLeafReaderContext().reader(),
                    DataStream.TimestampField.FIXED_TIMESTAMP_FIELD
                );

                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        assertTrue(tsid.advanceExact(doc));
                        assertTrue(timestamp.advanceExact(doc));
                        BytesRef latestTSID = tsid.lookupOrd(tsid.ordValue());
                        long latestTimestamp = timestamp.longValue();
                        assertEquals(latestTSID, aggCtx.getTsid());
                        assertEquals(latestTimestamp, aggCtx.getTimestamp());

                        if (currentTSID != null) {
                            assertTrue(
                                currentTSID + "->" + latestTSID.utf8ToString(),
                                tsidReverse ? latestTSID.compareTo(currentTSID) <= 0 : latestTSID.compareTo(currentTSID) >= 0
                            );
                            if (latestTSID.equals(currentTSID)) {
                                assertTrue(
                                    currentTimestamp + "->" + latestTimestamp,
                                    timestampReverse ? latestTimestamp <= currentTimestamp : latestTimestamp >= currentTimestamp
                                );
                                assertEquals(currentTSIDord, aggCtx.getTsidOrd());
                            } else {
                                assertThat(aggCtx.getTsidOrd(), greaterThan(currentTSIDord));
                            }
                        }
                        currentTimestamp = latestTimestamp;
                        currentTSID = BytesRef.deepCopyOf(latestTSID);
                        currentTSIDord = aggCtx.getTsidOrd();
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

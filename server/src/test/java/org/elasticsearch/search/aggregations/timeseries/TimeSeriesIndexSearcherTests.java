/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSeriesIndexSearcherTests extends ESTestCase {

    // Index a random set of docs with timestamp and tsid with the tsid/timestamp sort order
    // Open a searcher over a set of leaves
    // Collection should be in order

    public void testCollectInOrderAcrossSegments() throws IOException, InterruptedException {

        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(
            new Sort(
                new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING),
                new SortField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, SortField.Type.LONG)
            )
        );
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        AtomicInteger clock = new AtomicInteger(0);

        final int THREADS = 5;
        ExecutorService indexer = Executors.newFixedThreadPool(THREADS);
        for (int i = 0; i < THREADS; i++) {
            indexer.submit(() -> {
                Document doc = new Document();
                for (int j = 0; j < 500; j++) {
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

        BucketCollector collector = new BucketCollector() {

            BytesRef currentTSID = null;
            long currentTimestamp = 0;
            long total = 0;

            @Override
            public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                SortedDocValues tsid = DocValues.getSorted(ctx.reader(), TimeSeriesIdFieldMapper.NAME);
                NumericDocValues timestamp = DocValues.getNumeric(ctx.reader(), DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);

                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        assertTrue(tsid.advanceExact(doc));
                        assertTrue(timestamp.advanceExact(doc));
                        BytesRef latestTSID = tsid.lookupOrd(tsid.ordValue());
                        long latestTimestamp = timestamp.longValue();
                        if (currentTSID != null) {
                            assertTrue(currentTSID + "->" + latestTSID.utf8ToString(), latestTSID.compareTo(currentTSID) >= 0);
                            if (latestTSID.equals(currentTSID)) {
                                assertTrue(currentTimestamp + "->" + latestTimestamp, latestTimestamp >= currentTimestamp);
                            }
                        }
                        currentTimestamp = latestTimestamp;
                        currentTSID = BytesRef.deepCopyOf(latestTSID);
                        total++;
                    }
                };
            }

            @Override
            public void preCollection() throws IOException {

            }

            @Override
            public void postCollection() throws IOException {
                assertEquals(2500, total);
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
            }
        };

        indexSearcher.search(new MatchAllDocsQuery(), collector);
        collector.postCollection();

        reader.close();
        dir.close();

    }

}

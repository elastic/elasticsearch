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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesCancellationTests extends ESTestCase {

    private static Directory dir;
    private static IndexReader reader;

    @BeforeClass
    public static void setup() throws IOException {
        dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(
            new Sort(
                new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING),
                new SortField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, SortField.Type.LONG)
            )
        );
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        indexRandomDocuments(iw, randomIntBetween(2048, 4096));
        iw.flush();
        reader = iw.getReader();
        iw.close();
    }

    private static void indexRandomDocuments(RandomIndexWriter w, int numDocs) throws IOException {
        for (int i = 1; i <= numDocs; ++i) {
            Document doc = new Document();
            String tsid = "tsid" + randomIntBetween(0, 30);
            long time = randomNonNegativeLong();
            doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(tsid)));
            doc.add(new NumericDocValuesField(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, time));
            w.addDocument(doc);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        IOUtils.close(reader, dir);
        dir = null;
        reader = null;
    }

    public void testLowLevelCancellationActions() throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );
        TimeSeriesIndexSearcher timeSeriesIndexSearcher = new TimeSeriesIndexSearcher(searcher, List.of(() -> {
            throw new TaskCancelledException("Cancel");
        }));
        CountingBucketCollector bc = new CountingBucketCollector();
        expectThrows(TaskCancelledException.class, () -> timeSeriesIndexSearcher.search(new MatchAllDocsQuery(), bc));
        // We count every segment and every record as 1 and break on 2048th iteration counting from 0
        // so we expect to see 2048 - number_of_segments - 1 (-1 is because we check before we collect)
        assertThat(bc.count.get(), equalTo(Math.max(0, 2048 - reader.leaves().size() - 1)));
    }

    public static class CountingBucketCollector extends BucketCollector {
        public AtomicInteger count = new AtomicInteger();

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    count.incrementAndGet();
                }
            };
        }

        @Override
        public void preCollection() throws IOException {

        }

        @Override
        public void postCollection() throws IOException {

        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }
    }
}

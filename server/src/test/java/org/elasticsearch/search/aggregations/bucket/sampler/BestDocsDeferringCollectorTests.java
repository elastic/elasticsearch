/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BestDocsDeferringCollectorTests extends AggregatorTestCase {

    public void testReplay() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        int numDocs = randomIntBetween(1, 128);
        int maxNumValues = randomInt(16);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", String.valueOf(randomInt(maxNumValues)), Field.Store.NO));
            indexWriter.addDocument(document);
        }

        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader);

        TermQuery termQuery = new TermQuery(new Term("field", String.valueOf(randomInt(maxNumValues))));
        TopDocs topDocs = indexSearcher.search(termQuery, numDocs);

        final AtomicLong bytes = new AtomicLong(0);

        BestDocsDeferringCollector collector = new BestDocsDeferringCollector(
            numDocs,
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
            bytes::addAndGet
        );
        Set<Integer> deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(testCollector(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(termQuery, collector.asCollector());
        collector.postCollection();
        collector.prepareSelectedBuckets(0);

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }
        collector.close();
        indexReader.close();
        directory.close();
    }

    private BucketCollector testCollector(Set<Integer> docIds) {
        return new BucketCollector() {
            @Override
            public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        docIds.add(aggCtx.getLeafReaderContext().docBase + doc);
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
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        };
    }

}

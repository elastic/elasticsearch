/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class MinimumScoreCollectorTests extends ESTestCase {

    private Directory directory;
    private IndexReader reader;
    private IndexSearcher searcher;
    private int numDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
        numDocs = randomIntBetween(900, 1000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field1", "value", Field.Store.NO));
            if (i == 0) {
                doc.add(new StringField("field2", "value", Field.Store.NO));
            }
            writer.addDocument(doc);
        }
        writer.flush();
        reader = writer.getReader();
        searcher = newSearcher(reader);
        searcher.setSimilarity(new BM25Similarity());
        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(reader, directory);
    }

    public void testMinScoreFiltering() throws IOException {
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(2, 1000);
            searcher.search(booleanQuery, topScoreDocCollector);
            TopDocs topDocs = topScoreDocCollector.topDocs();
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[1].score;
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            searcher.search(booleanQuery, new MinimumScoreCollector(topScoreDocCollector, maxScore));
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            searcher.search(booleanQuery, new MinimumScoreCollector(topScoreDocCollector, thresholdScore));
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            searcher.search(booleanQuery, new MinimumScoreCollector(topScoreDocCollector, maxScore + 100f));
            assertEquals(0, topScoreDocCollector.topDocs().totalHits.value);
        }
    }

    public void testWeightIsNotPropagated() throws IOException {
        {
            TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            searcher.search(new MatchAllDocsQuery(), totalHitCountCollector);
            assertEquals(reader.maxDoc(), totalHitCountCollector.getTotalHits());
        }
        {
            TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            searcher.search(new MatchAllDocsQuery(), new MinimumScoreCollector(totalHitCountCollector, 100f));
            assertEquals(0, totalHitCountCollector.getTotalHits());
        }
    }

    public void testManager() throws IOException {
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(2, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[1].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, MinimumScoreCollector.createManager(topDocsManager, maxScore));
            assertEquals(1, topDocs.totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, MinimumScoreCollector.createManager(topDocsManager, thresholdScore));
            assertEquals(numDocs, topDocs.totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, MinimumScoreCollector.createManager(topDocsManager, maxScore + 100f));
            assertEquals(0, topDocs.totalHits.value);
        }
    }
}

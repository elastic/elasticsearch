/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FilteredCollectorTests extends ESTestCase {

    private Directory directory;
    private IndexReader reader;
    private IndexSearcher searcher;
    private int numDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
        numDocs = randomIntBetween(10, 100);
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
        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(reader, directory);
    }

    public void testFiltering() throws IOException {
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 100);
            searcher.search(new MatchAllDocsQuery(), topScoreDocCollector);
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 100);
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            searcher.search(new MatchAllDocsQuery(), new FilteredCollector(topScoreDocCollector, filterWeight));
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 100);
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            searcher.search(new MatchAllDocsQuery(), new FilteredCollector(topScoreDocCollector, filterWeight));
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
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
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            searcher.search(new MatchAllDocsQuery(), new FilteredCollector(totalHitCountCollector, filterWeight));
            assertEquals(1, totalHitCountCollector.getTotalHits());
        }
    }

    public void testManager() throws IOException {
        CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 100);
        {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
        }
        {
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            CollectorManager<FilteredCollector, TopDocs> filteredManager = FilteredCollector.createManager(topDocsManager, filterWeight);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filteredManager);
            assertEquals(1, topDocs.totalHits.value);
        }
        {
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            CollectorManager<FilteredCollector, TopDocs> filteredManager = FilteredCollector.createManager(topDocsManager, filterWeight);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filteredManager);
            assertEquals(numDocs, topDocs.totalHits.value);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PartialHitCountCollectorTests extends ESTestCase {

    private Directory dir;
    private IndexSearcher searcher;
    private int numDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("string", "a" + i, Field.Store.NO));
            doc.add(new StringField("string", "b" + i, Field.Store.NO));
            writer.addDocument(doc);
        }
        if (randomBoolean()) {
            writer.deleteDocuments(new Term("string", "a10"));
            numDocs--;
        }
        IndexReader reader = writer.getReader();
        writer.close();
        searcher = newSearcher(reader);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        searcher.getIndexReader().close();
        dir.close();
    }

    public void testHitCountFromWeightNoTracking() throws IOException {
        PartialHitCountCollector partialHitCountCollector = new PartialHitCountCollector(0);
        searcher.search(new MatchAllDocsQuery(), partialHitCountCollector);
        assertEquals(0, partialHitCountCollector.getTotalHits());
        assertTrue(partialHitCountCollector.hasEarlyTerminated());
    }

    public void testHitCountFromWeightDoesNotEarlyTerminate() throws IOException {
        {
            PartialHitCountCollector partialHitCountCollector = new PartialHitCountCollector(numDocs);
            searcher.search(new MatchAllDocsQuery(), partialHitCountCollector);
            assertEquals(numDocs, partialHitCountCollector.getTotalHits());
            assertFalse(partialHitCountCollector.hasEarlyTerminated());
        }
        {
            PartialHitCountCollector partialHitCountCollector = new PartialHitCountCollector(randomIntBetween(1, numDocs - 1));
            searcher.search(new MatchAllDocsQuery(), partialHitCountCollector);
            assertEquals(numDocs, partialHitCountCollector.getTotalHits());
            assertFalse(partialHitCountCollector.hasEarlyTerminated());
        }
        {
            PartialHitCountCollector partialHitCountCollector = new PartialHitCountCollector(
                randomIntBetween(numDocs + 1, Integer.MAX_VALUE)
            );
            searcher.search(new MatchAllDocsQuery(), partialHitCountCollector);
            assertEquals(numDocs, partialHitCountCollector.getTotalHits());
            assertFalse(partialHitCountCollector.hasEarlyTerminated());
        }
    }

    public void testCollectedHitCount() throws Exception {
        Query query = new BooleanQuery.Builder().add(new TermQuery(new Term("string", "a1")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("string", "b3")), BooleanClause.Occur.SHOULD)
            .build();
        // there's two docs matching the query: any totalHitsThreshold greater than or equal to 2 will non cause early termination
        PartialHitCountCollector partialHitCountCollector = new PartialHitCountCollector(randomIntBetween(2, Integer.MAX_VALUE));
        searcher.search(query, partialHitCountCollector);
        assertEquals(2, partialHitCountCollector.getTotalHits());
        assertFalse(partialHitCountCollector.hasEarlyTerminated());
    }

    public void testCollectedHitCountEarlyTerminated() throws Exception {
        Query query = new BooleanQuery.Builder().add(termQuery(new Term("string", "a1")), BooleanClause.Occur.SHOULD)
            .add(termQuery(new Term("string", "b3")), BooleanClause.Occur.SHOULD)
            .add(termQuery(new Term("string", "b6")), BooleanClause.Occur.SHOULD)
            .build();
        // there's three docs matching the query: any totalHitsThreshold lower than 3 will trigger early termination
        int totalHitsThreshold = randomInt(2);
        PartialHitCountCollector partialHitCountCollector = new PartialHitCountCollector(totalHitsThreshold);
        searcher.search(query, partialHitCountCollector);

        assertEquals(totalHitsThreshold, partialHitCountCollector.getTotalHits());
        assertTrue(partialHitCountCollector.hasEarlyTerminated());
    }

    /**
     * Create a {@link TermQuery} which cannot terminate collection early
     */
    private TermQuery termQuery(Term term) {
        return new TermQuery(term) {
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

                Weight w = super.createWeight(searcher, scoreMode, boost);
                return new FilterWeight(w) {
                    public int count(LeafReaderContext context) throws IOException {
                        return -1;
                    }
                };
            }
        };
    }
}

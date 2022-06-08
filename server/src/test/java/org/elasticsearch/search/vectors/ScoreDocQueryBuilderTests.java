/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScoreDocQueryBuilderTests extends AbstractQueryTestCase<ScoreDocQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected ScoreDocQueryBuilder doCreateTestQueryBuilder() {
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        int numShards = randomInt(5);
        for (int shard = 0; shard < numShards; shard++) {
            int numDocs = randomInt(10);
            for (int doc = 0; doc < numDocs; doc++) {
                scoreDocs.add(new ScoreDoc(doc, randomFloat(), shard));
            }
        }
        return new ScoreDocQueryBuilder(scoreDocs.toArray(new ScoreDoc[0]));
    }

    @Override
    public void testValidOutput() {
        ScoreDocQueryBuilder query = new ScoreDocQueryBuilder(new ScoreDoc[] { new ScoreDoc(0, 4.25f), new ScoreDoc(5, 1.6f) });
        String expected = """
            {
              "score_doc" : {
                "values" : [
                  {
                    "doc" : 0,
                    "score" : 4.25
                  },
                  {
                    "doc" : 5,
                    "score" : 1.6
                  }
                ]
              }
            }""";
        assertEquals(expected, query.toString());
    }

    @Override
    protected void doAssertLuceneQuery(ScoreDocQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        // Not needed since we override testToQuery
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testToQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(new Document());
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(new IndexSearcher(reader));
                ScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                Query query = queryBuilder.doToQuery(context);

                assertTrue(query instanceof ScoreDocQuery);
                ScoreDocQuery scoreDocQuery = (ScoreDocQuery) query;

                // This search execution context always has shardIndex = 0
                int expectedDocs = 0;
                for (ScoreDoc scoreDoc : queryBuilder.scoreDocs()) {
                    if (scoreDoc.shardIndex == 0) {
                        expectedDocs++;
                    }
                }
                assertEquals(expectedDocs, scoreDocQuery.docs().length);
                assertEquals(expectedDocs, scoreDocQuery.scores().length);
            }
        }
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testCacheability() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(new Document());
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(new IndexSearcher(reader));
                ScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
                assertNotNull(rewriteQuery.toQuery(context));
                assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
            }
        }
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testMustRewrite() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(new Document());
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(new IndexSearcher(reader));
                context.setAllowUnmappedFields(true);
                ScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                queryBuilder.toQuery(context);
            }
        }
    }

    @Override
    public void testUnknownObjectException() {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testFromXContent() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testUnknownField() {
        // Test isn't relevant, since query is never parsed from xContent
    }

    public void testScoreDocQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < 50; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "value" + i, Field.Store.NO));
                iw.addDocument(doc);
                if (i % 10 == 0) {
                    iw.flush();
                }
            }

            try (IndexReader reader = iw.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // This search execution context always has shardIndex = 0
                SearchExecutionContext context = createSearchExecutionContext(searcher);

                List<ScoreDoc> allScoreDocs = new ArrayList<>();
                List<ScoreDoc> expectedScoreDocs = new ArrayList<>();
                for (int doc = 0; doc < 50; doc += 1 + random().nextInt(5)) {
                    int shardIndex = randomInt(3);
                    ScoreDoc scoreDoc = new ScoreDoc(doc, randomFloat(), shardIndex);
                    allScoreDocs.add(scoreDoc);
                    if (shardIndex == 0) {
                        expectedScoreDocs.add(scoreDoc);
                    }
                }

                ScoreDocQueryBuilder queryBuilder = new ScoreDocQueryBuilder(allScoreDocs.toArray(new ScoreDoc[0]));
                Query query = queryBuilder.doToQuery(context);

                TopDocs topDocs = searcher.search(query, 100);
                assertEquals(expectedScoreDocs.size(), topDocs.totalHits.value);
                assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation);
                assertEquals(expectedScoreDocs.size(), topDocs.scoreDocs.length);

                Map<Integer, Float> docAndScore = expectedScoreDocs.stream()
                    .collect(Collectors.toMap(scoreDoc -> scoreDoc.doc, scoreDoc -> scoreDoc.score));
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    assertTrue(docAndScore.containsKey(scoreDoc.doc));
                    assertEquals((double) docAndScore.get(scoreDoc.doc), scoreDoc.score, 0.0001f);
                }
            }
        }
    }
}

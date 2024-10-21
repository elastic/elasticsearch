/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KnnScoreDocQueryBuilderTests extends AbstractQueryTestCase<KnnScoreDocQueryBuilder> {

    @Override
    protected KnnScoreDocQueryBuilder doCreateTestQueryBuilder() {
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        int numDocs = randomInt(10);
        for (int doc = 0; doc < numDocs; doc++) {
            scoreDocs.add(new ScoreDoc(doc, randomFloat()));
        }
        return new KnnScoreDocQueryBuilder(
            scoreDocs.toArray(new ScoreDoc[0]),
            randomBoolean() ? "field" : null,
            randomBoolean() ? VectorData.fromFloats(randomVector(10)) : null,
            randomBoolean() ? randomFloat() : null
        );
    }

    @Override
    public void testValidOutput() {
        KnnScoreDocQueryBuilder query = new KnnScoreDocQueryBuilder(
            new ScoreDoc[] { new ScoreDoc(0, 4.25f), new ScoreDoc(5, 1.6f) },
            "field",
            VectorData.fromFloats(new float[] { 1.0f, 2.0f }),
            null
        );
        String expected = """
            {
              "knn_score_doc" : {
                "values" : [
                  {
                    "doc" : 0,
                    "score" : 4.25
                  },
                  {
                    "doc" : 5,
                    "score" : 1.6
                  }
                ],
                "field" : "field",
                "query" : [
                  1.0,
                  2.0
                ]
              }
            }""";
        assertEquals(expected, query.toString());
    }

    @Override
    protected void doAssertLuceneQuery(KnnScoreDocQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
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
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                KnnScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                Query query = queryBuilder.doToQuery(context);

                assertTrue(query instanceof KnnScoreDocQuery);
                KnnScoreDocQuery scoreDocQuery = (KnnScoreDocQuery) query;

                int expectedDocs = queryBuilder.scoreDocs().length;
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
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                KnnScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
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
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                context.setAllowUnmappedFields(true);
                KnnScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                queryBuilder.toQuery(context);
            }
        }
    }

    public void testRewriteToMatchNone() throws IOException {
        KnnScoreDocQueryBuilder queryBuilder = new KnnScoreDocQueryBuilder(
            new ScoreDoc[0],
            randomBoolean() ? "field" : null,
            randomBoolean() ? VectorData.fromFloats(randomVector(10)) : null,
            randomBoolean() ? randomFloat() : null
        );
        QueryRewriteContext context = randomBoolean()
            ? new InnerHitsRewriteContext(createSearchExecutionContext().getParserConfig(), System::currentTimeMillis)
            : createSearchExecutionContext();
        assertEquals(new MatchNoneQueryBuilder(), queryBuilder.rewrite(context));
    }

    public void testRewriteForInnerHits() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        InnerHitsRewriteContext innerHitsRewriteContext = new InnerHitsRewriteContext(context.getParserConfig(), System::currentTimeMillis);
        KnnScoreDocQueryBuilder queryBuilder = new KnnScoreDocQueryBuilder(
            new ScoreDoc[] { new ScoreDoc(0, 4.25f), new ScoreDoc(5, 1.6f) },
            randomAlphaOfLength(10),
            VectorData.fromFloats(randomVector(10)),
            randomBoolean() ? randomFloat() : null
        );
        queryBuilder.boost(randomFloat());
        queryBuilder.queryName(randomAlphaOfLength(10));
        QueryBuilder rewritten = queryBuilder.rewrite(innerHitsRewriteContext);
        assertTrue(rewritten instanceof ExactKnnQueryBuilder);
        ExactKnnQueryBuilder exactKnnQueryBuilder = (ExactKnnQueryBuilder) rewritten;
        assertEquals(queryBuilder.queryVector(), exactKnnQueryBuilder.getQuery());
        assertEquals(queryBuilder.fieldName(), exactKnnQueryBuilder.getField());
        assertEquals(queryBuilder.boost(), exactKnnQueryBuilder.boost(), 0.0001f);
        assertEquals(queryBuilder.queryName(), exactKnnQueryBuilder.queryName());
        assertEquals(queryBuilder.vectorSimilarity(), exactKnnQueryBuilder.vectorSimilarity());
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

    public void testScoreDocQueryWeightCount() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < 30; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "value" + i, Field.Store.NO));
                iw.addDocument(doc);
                if (i % 10 == 0) {
                    iw.flush();
                }
            }
            try (IndexReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = createSearchExecutionContext(searcher);

                List<ScoreDoc> scoreDocsList = new ArrayList<>();
                for (int doc = 0; doc < 10; doc++) {
                    ScoreDoc scoreDoc = new ScoreDoc(doc, randomFloat());
                    scoreDocsList.add(scoreDoc);
                }
                ScoreDoc[] scoreDocs = scoreDocsList.toArray(new ScoreDoc[0]);

                KnnScoreDocQueryBuilder queryBuilder = new KnnScoreDocQueryBuilder(
                    scoreDocs,
                    "field",
                    VectorData.fromFloats(randomVector(10)),
                    null
                );
                Query query = queryBuilder.doToQuery(context);
                final Weight w = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);
                for (LeafReaderContext leafReaderContext : searcher.getLeafContexts()) {
                    int count = w.count(leafReaderContext);
                    final Scorer scorer = w.scorer(leafReaderContext);
                    if (count > 0) {
                        assertThat(scorer, is(notNullValue()));
                        int iteratorCount = 0;
                        while (scorer.iterator().nextDoc() != NO_MORE_DOCS) {
                            iteratorCount++;
                        }
                        assertThat(count, equalTo(iteratorCount));
                    } else {
                        assertThat(scorer, is(nullValue()));
                    }
                }
            }
        }
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
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = createSearchExecutionContext(searcher);

                List<ScoreDoc> scoreDocsList = new ArrayList<>();
                for (int doc = 0; doc < 50; doc += 1 + random().nextInt(5)) {
                    ScoreDoc scoreDoc = new ScoreDoc(doc, randomFloat());
                    scoreDocsList.add(scoreDoc);
                }
                ScoreDoc[] scoreDocs = scoreDocsList.toArray(new ScoreDoc[0]);

                KnnScoreDocQueryBuilder queryBuilder = new KnnScoreDocQueryBuilder(
                    scoreDocs,
                    "field",
                    VectorData.fromFloats(randomVector(10)),
                    null
                );
                final Query query = queryBuilder.doToQuery(context);
                final Weight w = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);

                TopDocs topDocs = searcher.search(query, 100);
                assertEquals(scoreDocs.length, topDocs.totalHits.value());
                assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation());

                Arrays.sort(topDocs.scoreDocs, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
                assertEquals(scoreDocs.length, topDocs.scoreDocs.length);
                for (int i = 0; i < scoreDocs.length; i++) {
                    assertEquals(scoreDocs[i].doc, topDocs.scoreDocs[i].doc);
                    assertEquals(scoreDocs[i].score, topDocs.scoreDocs[i].score, 0.0001f);
                    assertTrue(searcher.explain(query, scoreDocs[i].doc).isMatch());
                }

                for (LeafReaderContext leafReaderContext : searcher.getLeafContexts()) {
                    Scorer scorer = w.scorer(leafReaderContext);
                    // If we have matching docs, the score should always be greater than 0 for that segment
                    if (scorer != null) {
                        assertThat(leafReaderContext.toString(), scorer.getMaxScore(NO_MORE_DOCS), greaterThan(0.0f));
                    }
                }
            }
        }
    }
}

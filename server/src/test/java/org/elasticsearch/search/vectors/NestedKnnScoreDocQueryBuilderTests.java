/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NestedKnnScoreDocQueryBuilderTests extends AbstractQueryTestCase<NestedKnnScoreDocQueryBuilder> {

    private static final String VECTOR_FIELD = "vector";
    private static final String NESTED_FIELD = "nested";
    private static final int VECTOR_DIMENSION = 10;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestGeoShapeFieldMapperPlugin.class);
    }

    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(NESTED_FIELD)
            .field("type", "nested")
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(Strings.toString(builder)),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected NestedKnnScoreDocQueryBuilder doCreateTestQueryBuilder() {
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        int numDocs = randomInt(10);
        for (int doc = 0; doc < numDocs; doc++) {
            scoreDocs.add(new ScoreDoc(doc, randomFloat()));
        }
        float[] query = new float[VECTOR_DIMENSION];
        for (int i = 0; i < VECTOR_DIMENSION; i++) {
            query[i] = randomFloat();
        }
        KnnScoreDocQueryBuilder queryBuilder = new KnnScoreDocQueryBuilder(scoreDocs.toArray(new ScoreDoc[0]));
        return new NestedKnnScoreDocQueryBuilder(queryBuilder, query, NESTED_FIELD + "." + VECTOR_FIELD);
    }

    @Override
    public void testValidOutput() {
        NestedKnnScoreDocQueryBuilder query = new NestedKnnScoreDocQueryBuilder(
            new KnnScoreDocQueryBuilder(new ScoreDoc[] { new ScoreDoc(0, 4.25f), new ScoreDoc(5, 1.6f) }),
            new float[] { 1.0f, 2.0f },
            "field"
        );
        String expected = """
            {
              "nested_knn_score_doc" : {
                "knn_query" : {
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
                    ]
                  }
                },
                "query" : [
                  1.0,
                  2.0
                ],
                "field" : "field"
              }
            }""";
        assertEquals(expected, query.toString());
    }

    @Override
    protected void doAssertLuceneQuery(NestedKnnScoreDocQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
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
                NestedKnnScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
                    NESTED_FIELD,
                    queryBuilder,
                    org.apache.lucene.search.join.ScoreMode.Max
                );
                Query query = nestedQueryBuilder.toQuery(context);

                assertTrue(query instanceof ESToParentBlockJoinQuery);
                ESToParentBlockJoinQuery nestedQuery = (ESToParentBlockJoinQuery) query;

                ESDiversifyingChildrenKnnVectorQuery knnQuery = null;
                Query childQuery = nestedQuery.getChildQuery();
                if (childQuery instanceof ESDiversifyingChildrenKnnVectorQuery esDiversifyingChildrenKnnVectorQuery) {
                    knnQuery = esDiversifyingChildrenKnnVectorQuery;
                } else if (childQuery instanceof BooleanQuery booleanQuery) {
                    assertEquals(2, booleanQuery.clauses().size());
                    for (BooleanClause clause : booleanQuery.clauses()) {
                        if (clause.getQuery() instanceof ESDiversifyingChildrenKnnVectorQuery) {
                            knnQuery = (ESDiversifyingChildrenKnnVectorQuery) clause.getQuery();
                            break;
                        }
                        if (clause.getQuery() instanceof BoostQuery boostQuery) {
                            if (boostQuery.getQuery() instanceof ESDiversifyingChildrenKnnVectorQuery) {
                                knnQuery = (ESDiversifyingChildrenKnnVectorQuery) boostQuery.getQuery();
                                break;
                            }
                        }
                    }
                    if (knnQuery == null) {
                        fail("Unable to find ESDiversifyingChildrenKnnVectorQuery in BooleanQuery");
                    }
                } else {
                    fail("Unexpected child query type: " + childQuery.getClass());
                }

                assertTrue(knnQuery.getNearestChildren() instanceof KnnScoreDocQuery);
                KnnScoreDocQuery knnScoreDocQuery = (KnnScoreDocQuery) knnQuery.getNearestChildren();
                int expectedDocs = ((KnnScoreDocQueryBuilder) (queryBuilder.getKNNQuery())).scoreDocs().length;
                assertEquals(expectedDocs, knnScoreDocQuery.docs().length);
                assertEquals(expectedDocs, knnScoreDocQuery.scores().length);

                assertTrue(knnQuery.getExactKnnQuery() instanceof ExactKnnQuery);
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
                NestedKnnScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
                    NESTED_FIELD,
                    queryBuilder,
                    org.apache.lucene.search.join.ScoreMode.Max
                );
                QueryBuilder rewriteQuery = rewriteAndFetch(nestedQueryBuilder, new SearchExecutionContext(context));
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
                NestedKnnScoreDocQueryBuilder queryBuilder = createTestQueryBuilder();
                queryBuilder.toQuery(context);
            }
        }
    }

    public void testRewriteToMatchNone() throws IOException {
        KnnScoreDocQueryBuilder queryBuilder = new KnnScoreDocQueryBuilder(new ScoreDoc[0]);
        NestedKnnScoreDocQueryBuilder nestedQueryBuilder = new NestedKnnScoreDocQueryBuilder(
            queryBuilder,
            new float[VECTOR_DIMENSION],
            NESTED_FIELD + "." + VECTOR_FIELD
        );
        SearchExecutionContext context = createSearchExecutionContext();
        assertEquals(new MatchNoneQueryBuilder(), nestedQueryBuilder.rewrite(context));
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

}

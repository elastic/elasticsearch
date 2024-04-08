/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class ExactKnnQueryBuilderTests extends AbstractQueryTestCase<ExactKnnQueryBuilder> {

    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIMENSION = 3;

    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "cosine")
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
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected ExactKnnQueryBuilder doCreateTestQueryBuilder() {
        float[] query = new float[VECTOR_DIMENSION];
        for (int i = 0; i < VECTOR_DIMENSION; i++) {
            query[i] = randomFloat();
        }
        return new ExactKnnQueryBuilder(query, VECTOR_FIELD);
    }

    @Override
    public void testValidOutput() {
        ExactKnnQueryBuilder query = new ExactKnnQueryBuilder(new float[] { 1.0f, 2.0f, 3.0f }, "field");
        String expected = """
            {
              "exact_knn" : {
                "query" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "field" : "field"
              }
            }""";
        assertEquals(expected, query.toString());
    }

    @Override
    protected void doAssertLuceneQuery(ExactKnnQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertTrue(query instanceof BooleanQuery);
        BooleanQuery booleanQuery = (BooleanQuery) query;
        boolean foundFunction = false;
        for (BooleanClause clause : booleanQuery) {
            if (clause.getQuery() instanceof FunctionQuery functionQuery) {
                foundFunction = true;
                assertTrue(functionQuery.getValueSource() instanceof FloatVectorSimilarityFunction);
                String description = functionQuery.getValueSource().description().toLowerCase(Locale.ROOT);
                if (context.getIndexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.NORMALIZED_VECTOR_COSINE)) {
                    assertTrue(description, description.contains("dot_product"));
                } else {
                    assertTrue(description, description.contains("cosine"));
                }
            }
        }
        assertTrue("Unable to find FloatVectorSimilarityFunction in created BooleanQuery", foundFunction);
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

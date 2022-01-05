/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.vectors.DenseVectorPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class KnnVectorQueryBuilderTests extends AbstractQueryTestCase<KnnVectorQueryBuilder> {
    private static final String VECTOR_FIELD = "vector";
    private static final String VECTOR_ALIAS_FIELD = "vector_alias";
    private static final int VECTOR_DIMENSION = 3;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(DenseVectorPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject(VECTOR_ALIAS_FIELD)
            .field("type", "alias")
            .field("path", VECTOR_FIELD)
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
    protected KnnVectorQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomBoolean() ? VECTOR_FIELD : VECTOR_ALIAS_FIELD;

        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        int numCands = randomIntBetween(1, 1000);
        return new KnnVectorQueryBuilder(fieldName, vector, numCands);
    }

    @Override
    protected void doAssertLuceneQuery(KnnVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertTrue(query instanceof KnnVectorQuery);
        KnnVectorQuery knnVectorQuery = (KnnVectorQuery) query;

        // The field should always be resolved to the concrete field
        assertThat(knnVectorQuery, equalTo(new KnnVectorQuery(VECTOR_FIELD, queryBuilder.queryVector(), queryBuilder.numCands())));
    }

    public void testWrongDimension() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f }, 10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("the query vector has a different dimension [2] than the index vectors [3]"));
    }

    public void testNonexistentField() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("field [nonexistent] does not exist in the mapping"));
    }

    public void testWrongFieldType() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(
            AbstractBuilderTestCase.KEYWORD_FIELD_NAME,
            new float[] { 1.0f, 1.0f, 1.0f },
            10
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("[knn] queries are only supported on [dense_vector] fields"));
    }

    @Override
    public void testValidOutput() {
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, 10);
        String expected = """
            {
              "knn" : {
                "field" : "vector",
                "vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "num_candidates" : 10
              }
            }""";
        assertEquals(expected, query.toString());
    }

    @Override
    public void testUnknownObjectException() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testFromXContent() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testUnknownField() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }
}

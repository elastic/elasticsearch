/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class DenseVectorQueryBuilderTests extends AbstractQueryTestCase<DenseVectorQueryBuilder> {

    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIMENSION = 3;

    @Override
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
    protected DenseVectorQueryBuilder doCreateTestQueryBuilder() {
        float[] queryVector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < VECTOR_DIMENSION; i++) {
            queryVector[i] = randomFloat();
        }
        Float threshold = randomBoolean() ? randomFloat() : null;
        VectorSimilarity similarityFn = randomBoolean() ? null : randomFrom(VectorSimilarity.L2_NORM, VectorSimilarity.MAX_INNER_PRODUCT);
        Boolean quantized = randomBoolean() ? null : randomBoolean();
        return new DenseVectorQueryBuilder(VECTOR_FIELD, queryVector, threshold, similarityFn, quantized);
    }

    @Override
    protected void doAssertLuceneQuery(DenseVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        if (queryBuilder.getVectorSimilarity() != null) {
            assertThat(query, instanceOf(VectorSimilarityQuery.class));
            VectorSimilarityQuery vectorSimilarityQuery = (VectorSimilarityQuery) query;
            query = vectorSimilarityQuery.getInnerKnnQuery();
        }
        boolean useCodecPath = Boolean.TRUE.equals(queryBuilder.getQuantized()) && queryBuilder.getSimilarityFunction() == null;
        if (useCodecPath) {
            assertThat(query, instanceOf(DenseVectorQuery.Floats.class));
        } else {
            assertThat(query, instanceOf(DenseVectorQuery.RawFloats.class));
        }
    }

    public void testValidOutput() {
        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder("field", new float[] { 1.0f, 2.0f, 3.0f }, null, null, null);
        String expected = """
            {
              "dense_vector" : {
                "field" : "field",
                "query_vector" : [
                  1.0,
                  2.0,
                  3.0
                ]
              }
            }""";
        assertEquals(expected, query.toString());

        query = new DenseVectorQueryBuilder("field", new float[] { 1.0f, 2.0f, 3.0f }, 0.5f, VectorSimilarity.DOT_PRODUCT, true);
        expected = """
            {
              "dense_vector" : {
                "field" : "field",
                "query_vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "similarity" : 0.5,
                "similarity_function" : "dot_product",
                "quantized" : true
              }
            }""";
        assertEquals(expected, query.toString());
    }

    public void testRequiresQueryVectorOrBuilder() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DenseVectorQueryBuilder("field", (float[]) null, null, null, null)
        );
        assertThat(e.getMessage(), containsString("requires either"));
    }

    public void testRejectsBothVectorAndBuilder() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DenseVectorQueryBuilder(
                "field",
                VectorData.fromFloats(new float[] { 1f }),
                new TestQueryVectorBuilderPlugin.TestQueryVectorBuilder(new float[] { 1f }),
                null,
                null,
                null
            )
        );
        assertThat(e.getMessage(), containsString("only one of"));
    }

    public void testQuantizedTrueRewritesToExactKnn() throws IOException {
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder(VECTOR_FIELD, new float[] { 0.1f, 0.2f, 0.3f }, null, null, true);
        QueryRewriteContext rewriteContext = createSearchExecutionContext();
        var rewritten = builder.rewrite(rewriteContext);
        assertThat(rewritten, instanceOf(ExactKnnQueryBuilder.class));
    }

    public void testRawPathDoesNotRewriteToExactKnn() throws IOException {
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder(VECTOR_FIELD, new float[] { 0.1f, 0.2f, 0.3f }, null, null, false);
        QueryRewriteContext rewriteContext = createSearchExecutionContext();
        var rewritten = builder.rewrite(rewriteContext);
        assertThat(rewritten, instanceOf(DenseVectorQueryBuilder.class));
    }

    public void testSimilarityFunctionOverrideDoesNotRewriteToExactKnn() throws IOException {
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder(
            VECTOR_FIELD,
            new float[] { 0.1f, 0.2f, 0.3f },
            null,
            VectorSimilarity.L2_NORM,
            true
        );
        QueryRewriteContext rewriteContext = createSearchExecutionContext();
        var rewritten = builder.rewrite(rewriteContext);
        assertThat(rewritten, instanceOf(DenseVectorQueryBuilder.class));
    }
}

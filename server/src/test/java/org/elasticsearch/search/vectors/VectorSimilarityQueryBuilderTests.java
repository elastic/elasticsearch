/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class VectorSimilarityQueryBuilderTests extends AbstractQueryTestCase<VectorSimilarityQueryBuilder> {

    private static final String VECTOR_FIELD = "vector";
    private static final String BYTE_VECTOR_FIELD = "byte_vector";
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
            .field("similarity", "l2_norm")
            .endObject()
            .startObject(BYTE_VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("element_type", "byte")
            .field("similarity", "l2_norm")
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
    protected VectorSimilarityQueryBuilder doCreateTestQueryBuilder() {
        return switch (randomFrom(DenseVectorFieldMapper.ElementType.values())) {
            case BYTE -> new VectorSimilarityQueryBuilder(
                BYTE_VECTOR_FIELD,
                new float[] { randomByte(), randomByte(), randomByte() },
                randomIntBetween(1, 10),
                (float) randomDoubleBetween(0.1, 0.9, true)
            );
            case FLOAT -> new VectorSimilarityQueryBuilder(
                VECTOR_FIELD,
                new float[] { randomFloat(), randomFloat(), randomFloat() },
                randomIntBetween(1, 10),
                (float) randomDoubleBetween(0.1, 0.9, true)
            );
        };
    }

    @Override
    protected void doAssertLuceneQuery(
        VectorSimilarityQueryBuilder queryBuilder,
        Query query,
        SearchExecutionContext context
    ) throws IOException {
        assertThat(query, instanceOf(VectorSimilarityQuery.class));
        VectorSimilarityQuery vectorSimilarityQuery = (VectorSimilarityQuery) query;
        assertThat(vectorSimilarityQuery.getSimilarity(), equalTo(queryBuilder.getSimilarity()));
        Query innerQuery = vectorSimilarityQuery.getInnerKnnQuery();
        switch(queryBuilder.getField()) {
            case (VECTOR_FIELD) -> {
                assertThat(innerQuery, instanceOf(KnnFloatVectorQuery.class));
                KnnFloatVectorQuery floatVectorQuery = (KnnFloatVectorQuery) innerQuery;
                assertThat(floatVectorQuery.getField(), equalTo(queryBuilder.getField()));
                assertThat(floatVectorQuery.getTargetCopy(), equalTo(queryBuilder.getQueryVector()));
                assertThat(floatVectorQuery.getK(), equalTo(queryBuilder.getNumCandidates()));
            }
            case (BYTE_VECTOR_FIELD) -> {
                assertThat(innerQuery, instanceOf(KnnByteVectorQuery.class));
                KnnByteVectorQuery knnByteVectorQuery = (KnnByteVectorQuery) innerQuery;
                assertThat(knnByteVectorQuery.getField(), equalTo(queryBuilder.getField()));
                byte[] queryVector = new byte[queryBuilder.getQueryVector().length];
                for (int i = 0; i< queryVector.length;i ++) {
                    queryVector[i] = (byte)queryBuilder.getQueryVector()[i];
                }
                assertThat(knnByteVectorQuery.getTargetCopy(), equalTo(queryVector));
                assertThat(knnByteVectorQuery.getK(), equalTo(queryBuilder.getNumCandidates()));
            }
            default -> throw new AssertionError("unexpected vector field");
        }
    }
}

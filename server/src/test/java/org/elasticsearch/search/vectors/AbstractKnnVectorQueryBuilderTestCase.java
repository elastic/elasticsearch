/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

abstract class AbstractKnnVectorQueryBuilderTestCase extends AbstractQueryTestCase<KnnVectorQueryBuilder> {
    private static final String VECTOR_FIELD = "vector";
    private static final String VECTOR_ALIAS_FIELD = "vector_alias";
    static final int VECTOR_DIMENSION = 3;

    abstract DenseVectorFieldMapper.ElementType elementType();

    abstract KnnVectorQueryBuilder createKnnVectorQueryBuilder(String fieldName, Integer k, int numCands, Float similarity);

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
            .field("element_type", elementType())
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
        Integer k = randomBoolean() ? null : randomIntBetween(1, 100);
        int numCands = randomIntBetween(k == null ? DEFAULT_SIZE : k + 20, 1000);
        KnnVectorQueryBuilder queryBuilder = createKnnVectorQueryBuilder(fieldName, k, numCands, randomFloat());

        if (randomBoolean()) {
            List<QueryBuilder> filters = new ArrayList<>();
            int numFilters = randomIntBetween(1, 5);
            for (int i = 0; i < numFilters; i++) {
                String filterFieldName = randomBoolean() ? KEYWORD_FIELD_NAME : TEXT_FIELD_NAME;
                filters.add(QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10)));
            }
            queryBuilder.addFilterQueries(filters);
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(KnnVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.getVectorSimilarity() != null) {
            assertTrue(query instanceof VectorSimilarityQuery);
            Query knnQuery = ((VectorSimilarityQuery) query).getInnerKnnQuery();
            assertThat(((VectorSimilarityQuery) query).getSimilarity(), equalTo(queryBuilder.getVectorSimilarity()));
            switch (elementType()) {
                case FLOAT -> assertTrue(knnQuery instanceof ESKnnFloatVectorQuery);
                case BYTE -> assertTrue(knnQuery instanceof ESKnnByteVectorQuery);
            }
        } else {
            switch (elementType()) {
                case FLOAT -> assertTrue(query instanceof ESKnnFloatVectorQuery);
                case BYTE -> assertTrue(query instanceof ESKnnByteVectorQuery);
            }
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder qb : queryBuilder.filterQueries()) {
            builder.add(qb.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;
        // The field should always be resolved to the concrete field
        Query knnVectorQueryBuilt = switch (elementType()) {
            case BYTE, BIT -> new ESKnnByteVectorQuery(
                VECTOR_FIELD,
                queryBuilder.queryVector().asByteVector(),
                queryBuilder.k(),
                queryBuilder.numCands(),
                filterQuery
            );
            case FLOAT -> new ESKnnFloatVectorQuery(
                VECTOR_FIELD,
                queryBuilder.queryVector().asFloatVector(),
                queryBuilder.k(),
                queryBuilder.numCands(),
                filterQuery
            );
        };
        if (query instanceof VectorSimilarityQuery vectorSimilarityQuery) {
            query = vectorSimilarityQuery.getInnerKnnQuery();
        }
        assertEquals(query, knnVectorQueryBuilt);
    }

    public void testWrongDimension() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f }, 5, 10, null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(
            e.getMessage(),
            containsString("The query vector has a different number of dimensions [2] than the document vectors [3]")
        );
    }

    public void testNonexistentField() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 5, 10, null);
        context.setAllowUnmappedFields(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("No field mapping can be found for the field with name [nonexistent]"));
    }

    public void testNonexistentFieldReturnEmpty() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 5, 10, null);
        Query queryNone = query.doToQuery(context);
        assertThat(queryNone, instanceOf(MatchNoDocsQuery.class));
    }

    public void testWrongFieldType() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(
            AbstractBuilderTestCase.KEYWORD_FIELD_NAME,
            new float[] { 1.0f, 1.0f, 1.0f },
            5,
            10,
            null
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("[knn] queries are only supported on [dense_vector] fields"));
    }

    public void testNumCandsLessThanK() {
        int k = 5;
        int numCands = 3;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 1.0f, 1.0f }, k, numCands, null)
        );
        assertThat(e.getMessage(), containsString("[num_candidates] cannot be less than [k]"));
    }

    @Override
    public void testValidOutput() {
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, null, 10, null);
        String expected = """
            {
              "knn" : {
                "field" : "vector",
                "query_vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "num_candidates" : 10
              }
            }""";
        assertEquals(expected, query.toString());

        KnnVectorQueryBuilder query2 = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, 5, 10, null);
        String expected2 = """
            {
              "knn" : {
                "field" : "vector",
                "query_vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "k" : 5,
                "num_candidates" : 10
              }
            }""";
        assertEquals(expected2, query2.toString());
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder termQuery = new TermQueryBuilder("unmapped_field", 42);
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(
            VECTOR_FIELD,
            new float[] { 1.0f, 2.0f, 3.0f },
            VECTOR_DIMENSION,
            null,
            null
        );
        query.addFilterQuery(termQuery);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> query.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());

        QueryBuilder rewrittenQuery = query.rewrite(context);
        assertThat(rewrittenQuery, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testBWCVersionSerializationFilters() throws IOException {
        KnnVectorQueryBuilder query = createTestQueryBuilder();
        VectorData vectorData = VectorData.fromFloats(query.queryVector().asFloatVector());
        KnnVectorQueryBuilder queryNoFilters = new KnnVectorQueryBuilder(query.getFieldName(), vectorData, null, query.numCands(), null)
            .queryName(query.queryName())
            .boost(query.boost());
        TransportVersion beforeFilterVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_0_0,
            TransportVersions.V_8_1_0
        );
        assertBWCSerialization(query, queryNoFilters, beforeFilterVersion);
    }

    public void testBWCVersionSerializationSimilarity() throws IOException {
        KnnVectorQueryBuilder query = createTestQueryBuilder();
        VectorData vectorData = VectorData.fromFloats(query.queryVector().asFloatVector());
        KnnVectorQueryBuilder queryNoSimilarity = new KnnVectorQueryBuilder(query.getFieldName(), vectorData, null, query.numCands(), null)
            .queryName(query.queryName())
            .boost(query.boost())
            .addFilterQueries(query.filterQueries());
        assertBWCSerialization(query, queryNoSimilarity, TransportVersions.V_8_7_0);
    }

    public void testBWCVersionSerializationQuery() throws IOException {
        KnnVectorQueryBuilder query = createTestQueryBuilder();
        TransportVersion differentQueryVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_2_0,
            TransportVersions.V_8_12_0
        );
        Float similarity = differentQueryVersion.before(TransportVersions.V_8_8_0) ? null : query.getVectorSimilarity();
        VectorData vectorData = VectorData.fromFloats(query.queryVector().asFloatVector());
        KnnVectorQueryBuilder queryOlderVersion = new KnnVectorQueryBuilder(
            query.getFieldName(),
            vectorData,
            null,
            query.numCands(),
            similarity
        ).queryName(query.queryName()).boost(query.boost()).addFilterQueries(query.filterQueries());
        assertBWCSerialization(query, queryOlderVersion, differentQueryVersion);
    }

    private void assertBWCSerialization(QueryBuilder newQuery, QueryBuilder bwcQuery, TransportVersion version) throws IOException {
        assertSerialization(bwcQuery, version);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            output.writeNamedWriteable(newQuery);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry())) {
                in.setTransportVersion(version);
                KnnVectorQueryBuilder deserializedQuery = (KnnVectorQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
                assertEquals(bwcQuery, deserializedQuery);
                assertEquals(bwcQuery.hashCode(), deserializedQuery.hashCode());
            }
        }
    }

    public void testRewriteForInnerHits() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        InnerHitsRewriteContext innerHitsRewriteContext = new InnerHitsRewriteContext(context.getParserConfig(), System::currentTimeMillis);
        KnnVectorQueryBuilder queryBuilder = createTestQueryBuilder();
        queryBuilder.boost(randomFloat());
        queryBuilder.queryName(randomAlphaOfLength(10));
        QueryBuilder rewritten = queryBuilder.rewrite(innerHitsRewriteContext);
        assertTrue(rewritten instanceof ExactKnnQueryBuilder);
        ExactKnnQueryBuilder exactKnnQueryBuilder = (ExactKnnQueryBuilder) rewritten;
        assertEquals(queryBuilder.queryVector(), exactKnnQueryBuilder.getQuery());
        assertEquals(queryBuilder.getFieldName(), exactKnnQueryBuilder.getField());
        assertEquals(queryBuilder.boost(), exactKnnQueryBuilder.boost(), 0.0001f);
        assertEquals(queryBuilder.queryName(), exactKnnQueryBuilder.queryName());
        assertEquals(queryBuilder.getVectorSimilarity(), exactKnnQueryBuilder.vectorSimilarity());
    }

    public void testRewriteWithQueryVectorBuilder() throws Exception {
        int dims = randomInt(1024);
        float[] expectedArray = new float[dims];
        for (int i = 0; i < dims; i++) {
            expectedArray[i] = randomFloat();
        }
        KnnVectorQueryBuilder knnVectorQueryBuilder = new KnnVectorQueryBuilder(
            "field",
            new TestQueryVectorBuilderPlugin.TestQueryVectorBuilder(expectedArray),
            null,
            5,
            1f
        );
        knnVectorQueryBuilder.boost(randomFloat());
        List<QueryBuilder> filters = new ArrayList<>();
        int numFilters = randomIntBetween(1, 5);
        for (int i = 0; i < numFilters; i++) {
            String filterFieldName = randomBoolean() ? KEYWORD_FIELD_NAME : TEXT_FIELD_NAME;
            filters.add(QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10)));
        }
        knnVectorQueryBuilder.addFilterQueries(filters);

        QueryRewriteContext context = new QueryRewriteContext(null, null, null);
        PlainActionFuture<QueryBuilder> knnFuture = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(knnVectorQueryBuilder, context, knnFuture);
        KnnVectorQueryBuilder rewritten = (KnnVectorQueryBuilder) knnFuture.get();

        assertThat(rewritten.getFieldName(), equalTo(knnVectorQueryBuilder.getFieldName()));
        assertThat(rewritten.boost(), equalTo(knnVectorQueryBuilder.boost()));
        assertThat(rewritten.queryVector().asFloatVector(), equalTo(expectedArray));
        assertThat(rewritten.queryVectorBuilder(), nullValue());
        assertThat(rewritten.getVectorSimilarity(), equalTo(1f));
        assertThat(rewritten.filterQueries(), hasSize(numFilters));
        assertThat(rewritten.filterQueries(), equalTo(filters));
    }
}

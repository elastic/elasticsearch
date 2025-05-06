/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexVersions;
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
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

abstract class AbstractKnnVectorQueryBuilderTestCase extends AbstractQueryTestCase<KnnVectorQueryBuilder> {
    private static final String VECTOR_FIELD = "vector";
    private static final String VECTOR_ALIAS_FIELD = "vector_alias";
    protected static final Set<String> QUANTIZED_INDEX_TYPES = Set.of(
        "int8_hnsw",
        "int4_hnsw",
        "bbq_hnsw",
        "int8_flat",
        "int4_flat",
        "bbq_flat"
    );
    protected static final Set<String> NON_QUANTIZED_INDEX_TYPES = Set.of("hnsw", "flat");
    protected static final Set<String> ALL_INDEX_TYPES = Stream.concat(QUANTIZED_INDEX_TYPES.stream(), NON_QUANTIZED_INDEX_TYPES.stream())
        .collect(Collectors.toUnmodifiableSet());
    protected static String indexType;
    protected static int vectorDimensions;

    @Before
    private void checkIndexTypeAndDimensions() {
        // Check that these are initialized - should be done as part of the createAdditionalMappings method
        assertNotNull(indexType);
        assertNotEquals(0, vectorDimensions);
    }

    abstract DenseVectorFieldMapper.ElementType elementType();

    abstract KnnVectorQueryBuilder createKnnVectorQueryBuilder(
        String fieldName,
        int k,
        int numCands,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    );

    protected boolean isQuantizedElementType() {
        return QUANTIZED_INDEX_TYPES.contains(indexType);
    }

    protected abstract String randomIndexType();

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {

        // These fields are initialized here, as mappings are initialized only once per test class.
        // We want the subclasses to be able to override the index type and vector dimensions so we don't make this static / BeforeClass
        // for initialization.
        indexType = randomIndexType();
        if (indexType.contains("bbq")) {
            vectorDimensions = 64;
        } else if (indexType.contains("int4")) {
            vectorDimensions = 4;
        } else {
            vectorDimensions = 3;
        }

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", vectorDimensions)
            .field("index", true)
            .field("similarity", "l2_norm")
            .field("element_type", elementType())
            .startObject("index_options")
            .field("type", indexType)
            .endObject()
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
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k + 20, 1000);
        KnnVectorQueryBuilder queryBuilder = createKnnVectorQueryBuilder(
            fieldName,
            k,
            numCands,
            isIndextypeBBQ() ? randomBBQRescoreVectorBuilder() : randomRescoreVectorBuilder(),
            randomFloat()
        );

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

    private boolean isIndextypeBBQ() {
        return indexType.equals("bbq_hnsw") || indexType.equals("bbq_flat");
    }

    protected RescoreVectorBuilder randomBBQRescoreVectorBuilder() {
        return new RescoreVectorBuilder(randomBoolean() ? DEFAULT_OVERSAMPLE : randomFloatBetween(1.0f, 10.0f, false));
    }

    protected RescoreVectorBuilder randomRescoreVectorBuilder() {
        if (randomBoolean()) {
            return null;
        }

        return new RescoreVectorBuilder(randomBoolean() ? 0f : randomFloatBetween(1.0f, 10.0f, false));
    }

    @Override
    protected void doAssertLuceneQuery(KnnVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.getVectorSimilarity() != null) {
            assertTrue(query instanceof VectorSimilarityQuery);
            assertThat(((VectorSimilarityQuery) query).getSimilarity(), equalTo(queryBuilder.getVectorSimilarity()));
            query = ((VectorSimilarityQuery) query).getInnerKnnQuery();
        }
        Integer k = queryBuilder.k();
        if (k == null) {
            k = context.requestSize() == null || context.requestSize() < 0 ? DEFAULT_SIZE : context.requestSize();
        }
        if (queryBuilder.rescoreVectorBuilder() != null && isQuantizedElementType()) {
            if (queryBuilder.rescoreVectorBuilder().oversample() > 0) {
                RescoreKnnVectorQuery rescoreQuery = (RescoreKnnVectorQuery) query;
                assertEquals(k.intValue(), (rescoreQuery.k()));
                query = rescoreQuery.innerQuery();
            } else {
                assertFalse(query instanceof RescoreKnnVectorQuery);
            }
        }
        switch (elementType()) {
            case FLOAT -> assertTrue(query instanceof ESKnnFloatVectorQuery);
            case BYTE -> assertTrue(query instanceof ESKnnByteVectorQuery);
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder qb : queryBuilder.filterQueries()) {
            builder.add(qb.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;
        Integer numCands = queryBuilder.numCands();
        if (queryBuilder.rescoreVectorBuilder() != null && isQuantizedElementType()) {
            Float oversample = queryBuilder.rescoreVectorBuilder().oversample();
            k = Math.min(OVERSAMPLE_LIMIT, (int) Math.ceil(k * oversample));
            numCands = Math.max(numCands, k);
        }

        final KnnSearchStrategy expectedStrategy = context.getIndexSettings()
            .getIndexVersionCreated()
            .onOrAfter(IndexVersions.DEFAULT_TO_ACORN_HNSW_FILTER_HEURISTIC)
                ? DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy()
                : DenseVectorFieldMapper.FilterHeuristic.FANOUT.getKnnSearchStrategy();

        Query knnVectorQueryBuilt = switch (elementType()) {
            case BYTE, BIT -> new ESKnnByteVectorQuery(
                VECTOR_FIELD,
                queryBuilder.queryVector().asByteVector(),
                k,
                numCands,
                filterQuery,
                expectedStrategy
            );
            case FLOAT -> new ESKnnFloatVectorQuery(
                VECTOR_FIELD,
                queryBuilder.queryVector().asFloatVector(),
                k,
                numCands,
                filterQuery,
                expectedStrategy
            );
        };
        if (query instanceof VectorSimilarityQuery vectorSimilarityQuery) {
            query = vectorSimilarityQuery.getInnerKnnQuery();
        }
        assertEquals(query, knnVectorQueryBuilt);
    }

    public void testWrongDimension() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f }, 5, 10, null, null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(
            e.getMessage(),
            containsString("The query vector has a different number of dimensions [2] than the document vectors [" + vectorDimensions + "]")
        );
    }

    public void testNonexistentField() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 5, 10, null, null);
        context.setAllowUnmappedFields(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("No field mapping can be found for the field with name [nonexistent]"));
    }

    public void testNonexistentFieldReturnEmpty() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 5, 10, null, null);
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
            null,
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
            () -> new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 1.0f, 1.0f }, k, numCands, null, null)
        );
        assertThat(e.getMessage(), containsString("[num_candidates] cannot be less than [k]"));
    }

    @Override
    public void testValidOutput() {
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, null, 10, null, null);
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

        KnnVectorQueryBuilder query2 = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, 5, 10, null, null);
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
            vectorDimensions,
            null,
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
        KnnVectorQueryBuilder queryNoFilters = new KnnVectorQueryBuilder(
            query.getFieldName(),
            vectorData,
            null,
            query.numCands(),
            null,
            null
        ).queryName(query.queryName()).boost(query.boost());
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
        KnnVectorQueryBuilder queryNoSimilarity = new KnnVectorQueryBuilder(
            query.getFieldName(),
            vectorData,
            null,
            query.numCands(),
            null,
            null
        ).queryName(query.queryName()).boost(query.boost()).addFilterQueries(query.filterQueries());
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
            null,
            similarity
        ).queryName(query.queryName()).boost(query.boost()).addFilterQueries(query.filterQueries());
        assertBWCSerialization(query, queryOlderVersion, differentQueryVersion);
    }

    public void testBWCVersionSerializationRescoreVector() throws IOException {
        KnnVectorQueryBuilder query = createTestQueryBuilder();
        TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_8_1,
            TransportVersionUtils.getPreviousVersion(TransportVersions.KNN_QUERY_RESCORE_OVERSAMPLE)
        );
        VectorData vectorData = version.onOrAfter(TransportVersions.V_8_14_0)
            ? query.queryVector()
            : VectorData.fromFloats(query.queryVector().asFloatVector());
        Integer k = version.before(TransportVersions.V_8_15_0) ? null : query.k();
        KnnVectorQueryBuilder queryNoRescoreVector = new KnnVectorQueryBuilder(
            query.getFieldName(),
            vectorData,
            k,
            query.numCands(),
            null,
            query.getVectorSimilarity()
        ).queryName(query.queryName()).boost(query.boost()).addFilterQueries(query.filterQueries());
        assertBWCSerialization(query, queryNoRescoreVector, version);
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

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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

abstract class AbstractKnnVectorQueryBuilderTestCase extends AbstractQueryTestCase<KnnVectorQueryBuilder> {
    private static final TransportVersion QUERY_VECTOR_BASE64 = TransportVersion.fromName("knn_query_vector_base64");
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

    protected boolean rescoreVectorAllowZero = true;

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
        Float visitPercentage,
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
        Float visitPercentage = randomBoolean() ? null : randomFloatBetween(0.0f, 100.0f, true);
        KnnVectorQueryBuilder queryBuilder = createKnnVectorQueryBuilder(
            fieldName,
            k,
            numCands,
            visitPercentage,
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

        return new RescoreVectorBuilder((rescoreVectorAllowZero && randomBoolean()) ? 0f : randomFloatBetween(1.0f, 10.0f, false));
    }

    @Override
    protected void doAssertLuceneQuery(KnnVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        DenseVectorFieldType fieldType = (DenseVectorFieldType) context.getFieldType(VECTOR_FIELD);
        VectorData resolvedVector = fieldType.resolveQueryVector(queryBuilder.queryVector());

        if (queryBuilder.getVectorSimilarity() != null) {
            assertTrue(query instanceof VectorSimilarityQuery);
            assertThat(((VectorSimilarityQuery) query).getSimilarity(), equalTo(queryBuilder.getVectorSimilarity()));
            query = ((VectorSimilarityQuery) query).getInnerKnnQuery();
        }
        int k;
        if (queryBuilder.k() == null) {
            k = context.requestSize() == null || context.requestSize() < 0 ? DEFAULT_SIZE : context.requestSize();
        } else {
            k = queryBuilder.k();
        }
        if (queryBuilder.rescoreVectorBuilder() != null && isQuantizedElementType()) {
            if (queryBuilder.rescoreVectorBuilder().oversample() > 0) {
                RescoreKnnVectorQuery rescoreQuery = (RescoreKnnVectorQuery) query;
                assertEquals(k, (rescoreQuery.k()));
                query = rescoreQuery.innerQuery();
            } else {
                assertFalse(query instanceof RescoreKnnVectorQuery);
            }
        }
        switch (elementType()) {
            case FLOAT -> assertThat(
                query,
                anyOf(instanceOf(ESKnnFloatVectorQuery.class), instanceOf(DenseVectorQuery.Floats.class), instanceOf(BooleanQuery.class))
            );
            case BYTE -> assertThat(
                query,
                anyOf(instanceOf(ESKnnByteVectorQuery.class), instanceOf(DenseVectorQuery.Bytes.class), instanceOf(BooleanQuery.class))
            );
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder qb : queryBuilder.filterQueries()) {
            builder.add(qb.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;
        Query approxFilterQuery = filterQuery != null ? new CachingEnableFilterQuery(filterQuery) : null;
        Integer numCands = queryBuilder.numCands();
        if (queryBuilder.rescoreVectorBuilder() != null && isQuantizedElementType()) {
            float oversample = queryBuilder.rescoreVectorBuilder().oversample();
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
                resolvedVector.asByteVector(),
                k,
                numCands,
                approxFilterQuery,
                expectedStrategy
            );
            case FLOAT, BFLOAT16 -> new ESKnnFloatVectorQuery(
                VECTOR_FIELD,
                resolvedVector.asFloatVector(),
                k,
                numCands,
                approxFilterQuery,
                expectedStrategy
            );
        };

        Query bruteForceVectorQueryBuilt = switch (elementType()) {
            case BIT, BYTE -> {
                if (filterQuery != null) {
                    yield new BooleanQuery.Builder().add(
                        new DenseVectorQuery.Bytes(resolvedVector.asByteVector(), VECTOR_FIELD),
                        BooleanClause.Occur.SHOULD
                    ).add(filterQuery, BooleanClause.Occur.FILTER).build();
                } else {
                    yield new DenseVectorQuery.Bytes(resolvedVector.asByteVector(), VECTOR_FIELD);
                }
            }
            case FLOAT, BFLOAT16 -> {
                if (filterQuery != null) {
                    yield new BooleanQuery.Builder().add(
                        new DenseVectorQuery.Floats(resolvedVector.asFloatVector(), VECTOR_FIELD),
                        BooleanClause.Occur.SHOULD
                    ).add(filterQuery, BooleanClause.Occur.FILTER).build();
                } else {
                    yield new DenseVectorQuery.Floats(resolvedVector.asFloatVector(), VECTOR_FIELD);
                }
            }
        };

        if (query instanceof VectorSimilarityQuery vectorSimilarityQuery) {
            query = vectorSimilarityQuery.getInnerKnnQuery();
        }
        assertThat(query, anyOf(equalTo(knnVectorQueryBuilt), equalTo(bruteForceVectorQueryBuilt)));
    }

    public void testWrongDimension() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f }, 5, 10, 10f, null, null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(
            e.getMessage(),
            containsString("The query vector has a different number of dimensions [2] than the document vectors [" + vectorDimensions + "]")
        );
    }

    public void testNonexistentField() {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 5, 10, 10f, null, null);
        context.setAllowUnmappedFields(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("No field mapping can be found for the field with name [nonexistent]"));
    }

    public void testNonexistentFieldReturnEmpty() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("nonexistent", new float[] { 1.0f, 1.0f, 1.0f }, 5, 10, 10f, null, null);
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
            10f,
            null,
            null
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.doToQuery(context));
        assertThat(e.getMessage(), containsString("[knn] queries are only supported on [dense_vector] fields"));
    }

    public void testQueryVectorBase64RewritesToFloatVector() throws Exception {
        String encoded;
        float[] expectedVector;

        if (elementType() == DenseVectorFieldMapper.ElementType.BYTE) {
            // For byte vectors, encode as bytes
            byte[] byteVector = randomByteVector(vectorDimensions);
            encoded = encodeToBase64(byteVector);
            // Convert to float for comparison (keep as signed byte values)
            expectedVector = new float[byteVector.length];
            for (int i = 0; i < byteVector.length; i++) {
                expectedVector[i] = byteVector[i];
            }
        } else {
            // For float vectors, encode as floats
            expectedVector = randomVector(vectorDimensions);
            encoded = encodeToBase64(expectedVector);
        }

        int k = randomIntBetween(1, Math.max(1, vectorDimensions));
        int numCands = randomIntBetween(k, Math.max(k, vectorDimensions + 10));

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(KnnVectorQueryBuilder.NAME)
            .field(KnnVectorQueryBuilder.FIELD_FIELD.getPreferredName(), VECTOR_FIELD)
            .field(KnnVectorQueryBuilder.QUERY_VECTOR_FIELD.getPreferredName(), encoded)
            .field(KnnVectorQueryBuilder.K_FIELD.getPreferredName(), k)
            .field(KnnVectorQueryBuilder.NUM_CANDS_FIELD.getPreferredName(), numCands)
            .endObject()
            .endObject();

        try (XContentParser parser = createParser(builder)) {
            SearchExecutionContext context = createSearchExecutionContext();
            KnnVectorQueryBuilder parsed = (KnnVectorQueryBuilder) parseQuery(parser);
            KnnVectorQueryBuilder rewritten = (KnnVectorQueryBuilder) parsed.rewrite(context);

            DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) context.getFieldType(VECTOR_FIELD);
            VectorData resolved = vectorFieldType.resolveQueryVector(rewritten.queryVector());
            assertArrayEquals(expectedVector, resolved.asFloatVector(), 0f);
            assertNull("base64 should be resolved without a query_vector_builder", rewritten.queryVectorBuilder());
        }
    }

    public void testQueryVectorBase64InvalidEncoding() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(KnnVectorQueryBuilder.NAME)
            .field(KnnVectorQueryBuilder.FIELD_FIELD.getPreferredName(), VECTOR_FIELD)
            .field(KnnVectorQueryBuilder.QUERY_VECTOR_FIELD.getPreferredName(), "not-base64###")
            .field(KnnVectorQueryBuilder.K_FIELD.getPreferredName(), 3)
            .field(KnnVectorQueryBuilder.NUM_CANDS_FIELD.getPreferredName(), 5)
            .endObject()
            .endObject();

        try (XContentParser parser = createParser(builder)) {
            SearchExecutionContext context = createSearchExecutionContext();
            KnnVectorQueryBuilder parsed = (KnnVectorQueryBuilder) parseQuery(parser);
            DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) context.getFieldType(VECTOR_FIELD);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> vectorFieldType.resolveQueryVector(parsed.queryVector())
            );
            assertThat(e.getMessage(), containsString("query_vector"));
            assertThat(e.getMessage(), containsString("base64"));
        }
    }

    public void testQueryVectorBase64WrongDimensions() throws Exception {
        String encoded;
        if (elementType() == DenseVectorFieldMapper.ElementType.BYTE) {
            // For byte vectors, encode as bytes with wrong dimensions
            byte[] vector = randomByteVector(vectorDimensions + 1);
            encoded = encodeToBase64(vector);
        } else {
            // For float vectors, encode as floats with wrong dimensions
            float[] vector = randomVector(vectorDimensions + 1);
            encoded = encodeToBase64(vector);
        }

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(KnnVectorQueryBuilder.NAME)
            .field(KnnVectorQueryBuilder.FIELD_FIELD.getPreferredName(), VECTOR_FIELD)
            .field(KnnVectorQueryBuilder.QUERY_VECTOR_FIELD.getPreferredName(), encoded)
            .field(KnnVectorQueryBuilder.K_FIELD.getPreferredName(), 3)
            .field(KnnVectorQueryBuilder.NUM_CANDS_FIELD.getPreferredName(), 5)
            .endObject()
            .endObject();

        try (XContentParser parser = createParser(builder)) {
            SearchExecutionContext context = createSearchExecutionContext();
            KnnVectorQueryBuilder parsed = (KnnVectorQueryBuilder) parseQuery(parser);
            DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) context.getFieldType(VECTOR_FIELD);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> vectorFieldType.resolveQueryVector(parsed.queryVector())
            );
            assertThat(
                e.getMessage(),
                anyOf(
                    containsString("different number of dimensions"),
                    containsString("Base64-encoded byte vector"),
                    containsString("Base64-encoded float vector")
                )
            );
        }
    }

    public void testNumCandsLessThanK() {
        int k = 5;
        int numCands = 3;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 1.0f, 1.0f }, k, numCands, 10f, null, null)
        );
        assertThat(e.getMessage(), containsString("[num_candidates] cannot be less than [k]"));
    }

    @Override
    public void testValidOutput() {
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, null, 10, 10f, null, null);

        String expected = """
            {
              "knn" : {
                "field" : "vector",
                "query_vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "num_candidates" : 10,
                "visit_percentage" : 10.0
              }
            }""";

        assertEquals(expected, query.toString());

        KnnVectorQueryBuilder query2 = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, 5, 10, 10f, null, null);
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
                "num_candidates" : 10,
                "visit_percentage" : 10.0
              }
            }""";
        assertEquals(expected2, query2.toString());

        KnnVectorQueryBuilder query3 = new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 1.0f, 2.0f, 3.0f }, 5, 10, null, null, null);
        String expected3 = """
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
        assertEquals(expected3, query3.toString());
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
            null,
            null
        );
        query.addFilterQuery(termQuery);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> query.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());

        QueryBuilder rewrittenQuery = query.rewrite(context);
        assertThat(rewrittenQuery, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testBWCVersionSerialization_GivenStringVector() throws IOException {
        if (elementType() != DenseVectorFieldMapper.ElementType.BYTE && elementType() != DenseVectorFieldMapper.ElementType.BIT) {
            return;
        }

        TransportVersion version = TransportVersion.fromId(QUERY_VECTOR_BASE64.id() - 1000);
        for (int i = 0; i < NUMBER_OF_TESTQUERIES; i++) {
            byte[] bytes = new byte[vectorDimensions];
            for (int j = 0; j < bytes.length; j++) {
                bytes[j] = randomByte();
            }
            String hexString = HexFormat.of().formatHex(bytes);

            KnnVectorQueryBuilder queryWithString = new KnnVectorQueryBuilder(
                VECTOR_FIELD,
                VectorData.fromStringVector(hexString),
                5,
                10,
                null,
                null,
                null
            );

            KnnVectorQueryBuilder expectedBwc = new KnnVectorQueryBuilder(
                VECTOR_FIELD,
                VectorData.fromBytes(bytes),
                5,
                10,
                null,
                null,
                null
            );

            assertBWCSerialization(queryWithString, expectedBwc, version);
        }
    }

    public void testBWCVersionSerialization_GivenAutoPrefiltering() throws IOException {
        for (int i = 0; i < NUMBER_OF_TESTQUERIES; i++) {

            TransportVersion version = TransportVersion.fromId(KnnVectorQueryBuilder.AUTO_PREFILTERING.id() - 1000);
            rescoreVectorAllowZero = version.supports(RescoreVectorBuilder.RESCORE_VECTOR_ALLOW_ZERO);
            KnnVectorQueryBuilder query = doCreateTestQueryBuilder().setAutoPrefilteringEnabled(true);
            KnnVectorQueryBuilder queryNoAutoPrefiltering = new KnnVectorQueryBuilder(
                query.getFieldName(),
                query.queryVector(),
                query.k(),
                query.numCands(),
                version.supports(KnnVectorQueryBuilder.VISIT_PERCENTAGE) ? query.visitPercentage() : null,
                query.rescoreVectorBuilder(),
                query.getVectorSimilarity()
            ).queryName(query.queryName()).boost(query.boost()).addFilterQueries(query.filterQueries()).setAutoPrefilteringEnabled(false);
            assertBWCSerialization(query, queryNoAutoPrefiltering, version);
        }
    }

    public void testSerialization_GivenAutoPrefiltering() throws IOException {
        KnnVectorQueryBuilder query = doCreateTestQueryBuilder().setAutoPrefilteringEnabled(true);
        KnnVectorQueryBuilder serializedQuery = copyQuery(query);
        assertThat(serializedQuery, equalTo(query));
        assertThat(serializedQuery.hashCode(), equalTo(query.hashCode()));
    }

    private void assertBWCSerialization(QueryBuilder newQuery, QueryBuilder bwcQuery, TransportVersion version) throws IOException {
        assertSerialization(bwcQuery, version);
        QueryBuilder deserializedQuery = copyNamedWriteable(newQuery, namedWriteableRegistry(), QueryBuilder.class, version);
        assertEquals(bwcQuery, deserializedQuery);
        assertEquals(bwcQuery.hashCode(), deserializedQuery.hashCode());
    }

    public void testRewriteForInnerHits() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        InnerHitsRewriteContext innerHitsRewriteContext = new InnerHitsRewriteContext(context.getParserConfig(), System::currentTimeMillis);
        KnnVectorQueryBuilder queryBuilder = createTestQueryBuilder();
        queryBuilder.boost(randomFloat());
        queryBuilder.queryName(randomAlphaOfLength(10));
        QueryBuilder rewritten = queryBuilder.rewrite(innerHitsRewriteContext);
        float queryBoost = rewritten.boost();
        String queryName = rewritten.queryName();
        if (queryBuilder.filterQueries().isEmpty() == false) {
            assertTrue(rewritten instanceof BoolQueryBuilder);
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) rewritten;
            rewritten = boolQueryBuilder.must().get(0);
        }
        assertTrue(rewritten instanceof ExactKnnQueryBuilder);
        ExactKnnQueryBuilder exactKnnQueryBuilder = (ExactKnnQueryBuilder) rewritten;
        assertEquals(queryBuilder.queryVector(), exactKnnQueryBuilder.getQuery());
        assertEquals(queryBuilder.getFieldName(), exactKnnQueryBuilder.getField());
        assertEquals(queryBuilder.boost(), queryBoost, 0.0001f);
        assertEquals(queryBuilder.queryName(), queryName);
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
            10f,
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
        knnVectorQueryBuilder.setAutoPrefilteringEnabled(randomBoolean());

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
        assertThat(rewritten.isAutoPrefilteringEnabled(), equalTo(knnVectorQueryBuilder.isAutoPrefilteringEnabled()));
    }

    public void testSetFilterQueries() {
        KnnVectorQueryBuilder knnQueryBuilder = doCreateTestQueryBuilder();
        List<QueryBuilder> newFilters = randomList(5, () -> RandomQueryBuilder.createQuery(random()));
        knnQueryBuilder.setFilterQueries(newFilters);
        assertThat(knnQueryBuilder.filterQueries(), equalTo(newFilters));
    }

    protected String encodeToBase64(float[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES * vector.length).order(ByteOrder.BIG_ENDIAN);
        buffer.asFloatBuffer().put(vector);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    protected String encodeToBase64(byte[] vector) {
        return Base64.getEncoder().encodeToString(vector);
    }

    private float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    private byte[] randomByteVector(int dims) {
        byte[] vector = new byte[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomByte();
        }
        return vector;
    }
}

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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.lucene.search.Queries.NO_DOCS_INSTANCE;
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
        VectorSimilarity similarityFn = randomBoolean() ? null : randomFrom(VectorSimilarity.L2_NORM, VectorSimilarity.MAX_INNER_PRODUCT);
        Boolean quantized = randomBoolean() ? null : randomBoolean();
        // quantized=true is incompatible with similarity_function
        if (Boolean.TRUE.equals(quantized) && similarityFn != null) {
            quantized = null;
        }
        return new DenseVectorQueryBuilder(VECTOR_FIELD, queryVector, similarityFn, quantized);
    }

    @Override
    protected void doAssertLuceneQuery(DenseVectorQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        assertThat(query, instanceOf(DenseVectorQuery.Floats.class));
        DenseVectorQuery.Floats floats = (DenseVectorQuery.Floats) query;
        boolean useCodecPath = Boolean.TRUE.equals(queryBuilder.getQuantized()) && queryBuilder.getSimilarityFunction() == null;
        if (useCodecPath) {
            assertNull("codec path should not carry an explicit function", floats.getFunction());
        } else {
            assertNotNull("raw path must carry an explicit function", floats.getFunction());
        }
    }

    public void testValidOutput() {
        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder("field", new float[] { 1.0f, 2.0f, 3.0f }, null, null);
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

        query = new DenseVectorQueryBuilder("field", new float[] { 1.0f, 2.0f, 3.0f }, VectorSimilarity.DOT_PRODUCT, null);
        expected = """
            {
              "dense_vector" : {
                "field" : "field",
                "query_vector" : [
                  1.0,
                  2.0,
                  3.0
                ],
                "similarity_function" : "dot_product"
              }
            }""";
        assertEquals(expected, query.toString());
    }

    public void testRequiresQueryVectorOrBuilder() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DenseVectorQueryBuilder("field", (float[]) null, null, null)
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
                null
            )
        );
        assertThat(e.getMessage(), containsString("only one of"));
    }

    public void testQuantizedTrueRewritesToExactKnn() throws IOException {
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder(VECTOR_FIELD, new float[] { 0.1f, 0.2f, 0.3f }, null, true);
        QueryRewriteContext rewriteContext = createSearchExecutionContext();
        var rewritten = builder.rewrite(rewriteContext);
        assertThat(rewritten, instanceOf(ExactKnnQueryBuilder.class));
    }

    public void testRawPathDoesNotRewriteToExactKnn() throws IOException {
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder(VECTOR_FIELD, new float[] { 0.1f, 0.2f, 0.3f }, null, false);
        QueryRewriteContext rewriteContext = createSearchExecutionContext();
        var rewritten = builder.rewrite(rewriteContext);
        assertThat(rewritten, instanceOf(DenseVectorQueryBuilder.class));
    }

    /**
     * A search may target an index pattern where only some indices map the vector field. The shards that
     * don't map it must match no documents so the mapped indices still return results, matching the
     * behaviour of the [knn] and internal [exact_knn] queries.
     */
    public void testMissingFieldReturnsNoDocs() throws IOException {
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder("missing", new float[] { 0.1f, 0.2f, 0.3f }, null, false);
        assertEquals(NO_DOCS_INSTANCE, builder.toQuery(createSearchExecutionContext()));
    }

    /**
     * The [quantized] shortcut only rewrites to [exact_knn] when the field resolves, so on a shard that
     * doesn't map the field the query stays a [dense_vector] query. It must match no documents there too.
     */
    public void testMissingFieldReturnsNoDocsWhenQuantized() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder("missing", new float[] { 0.1f, 0.2f, 0.3f }, null, true);
        var rewritten = builder.rewrite(context);
        assertThat(rewritten, instanceOf(DenseVectorQueryBuilder.class));
        assertEquals(NO_DOCS_INSTANCE, rewritten.toQuery(context));
    }

    /**
     * An index that sets [index.query.parse.allow_unmapped_fields: false] rejects the missing field in
     * {@link org.elasticsearch.index.query.QueryRewriteContext#getFieldType}, before the query builder can
     * turn it into a no-docs query. [dense_vector] must fail there exactly like [knn] does, on both the raw
     * and the quantized path — the latter resolves the field type during rewrite rather than in doToQuery.
     */
    public void testMissingFieldWithUnmappedFieldsDisallowed() {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(false);
        float[] queryVector = new float[] { 0.1f, 0.2f, 0.3f };

        QueryShardException knnException = expectThrows(
            QueryShardException.class,
            () -> new KnnVectorQueryBuilder("missing", queryVector, 5, 10, 10f, null, null).toQuery(context)
        );
        assertThat(knnException.getMessage(), containsString("No field mapping can be found for the field with name [missing]"));

        for (Boolean quantized : new Boolean[] { false, true }) {
            DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder("missing", queryVector, null, quantized);
            QueryShardException e = expectThrows(QueryShardException.class, () -> builder.rewrite(context).toQuery(context));
            assertEquals(knnException.getMessage(), e.getMessage());
        }
    }

    /**
     * A field that exists but is mapped as another type is a mapping conflict rather than a missing field,
     * so it stays an error — again matching [knn].
     */
    public void testWrongFieldType() {
        SearchExecutionContext context = createSearchExecutionContext();
        DenseVectorQueryBuilder builder = new DenseVectorQueryBuilder(KEYWORD_FIELD_NAME, new float[] { 0.1f, 0.2f, 0.3f }, null, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.toQuery(context));
        assertThat(e.getMessage(), containsString("[dense_vector] queries are only supported on [dense_vector] fields"));
    }

    public void testSimilarityFunctionAndQuantizedTrueIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DenseVectorQueryBuilder(VECTOR_FIELD, new float[] { 0.1f, 0.2f, 0.3f }, VectorSimilarity.L2_NORM, true)
        );
        assertThat(e.getMessage(), containsString("similarity_function"));
        assertThat(e.getMessage(), containsString("quantized"));
    }

    public void testQueryVectorBuilderTextBreakerEstimate() {
        // When queryVector is absent, parseTimeBreakerEstimate() delegates to queryVectorBuilder.
        QueryVectorBuilder stub = new QueryVectorBuilder() {
            @Override
            public void buildVector(Client c, ActionListener<float[]> l) {}

            @Override
            public String getWriteableName() {
                return "stub";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.minimumCompatible();
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public XContentBuilder toXContent(XContentBuilder b, ToXContent.Params p) {
                return b;
            }

            @Override
            public long parseTimeBreakerEstimate() {
                return 500L;
            }
        };
        DenseVectorQueryBuilder q = new DenseVectorQueryBuilder(VECTOR_FIELD, null, stub, null, null);
        assertEquals(AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + 500L, q.parseTimeBreakerEstimate());
    }

    public void testVectorBreakerEstimate() throws IOException {
        // small: 2-element float vector -> cost = 256 + 2*4 = 264; large: 100-element float vector -> cost = 256 + 100*4 = 656
        DenseVectorQueryBuilder small = new DenseVectorQueryBuilder(VECTOR_FIELD, new float[] { 1f, 2f }, null, null);
        DenseVectorQueryBuilder large = new DenseVectorQueryBuilder(VECTOR_FIELD, new float[100], null, null);
        long limit = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + 2 * 4L; // 264
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            for (XContentType type : new XContentType[] { XContentType.JSON, XContentType.SMILE }) {
                BytesReference bytes = XContentHelper.toXContent(small, type, false);
                try (XContentParser parser = createParser(type.xContent(), bytes)) {
                    parseQuery(parser); // must not throw
                }
            }
            for (XContentType type : new XContentType[] { XContentType.JSON, XContentType.SMILE }) {
                BytesReference bytes = XContentHelper.toXContent(large, type, false);
                try (XContentParser parser = createParser(type.xContent(), bytes)) {
                    expectThrows(CircuitBreakingException.class, () -> parseQuery(parser));
                }
            }
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }
}

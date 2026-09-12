/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Base class for integration tests that fetch embeddings from {@code dense_vector} and {@code sparse_vector} fields via
 * {@link SearchSourceBuilder#fetchEmbeddingsField}.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
abstract class AbstractVectorFieldEmbeddingsFieldIT<C extends AbstractVectorFieldEmbeddingsFieldIT.VectorFieldConfig<?>> extends
    ESIntegTestCase {

    /**
     * A vector field to fetch embeddings from. Encapsulates the field's mapping fragment and the randomly generated value that will be
     * indexed into the field. Concrete subclasses of the enclosing IT implement this class for each vector type.
     */
    abstract static class VectorFieldConfig<V> {
        private final String fieldName;
        private final V value;

        VectorFieldConfig(String fieldName, V value) {
            this.fieldName = fieldName;
            this.value = value;
        }

        /** The field name. */
        public final String fieldName() {
            return fieldName;
        }

        /** The randomly generated value that is indexed into this field and used for assertion. */
        public final V value() {
            return value;
        }

        /**
         * The value written into {@code _source} for this field. Defaults to {@link #value()}; subclasses override this to index an
         * alternate representation of the same vector, such as an encoded string.
         */
        public Object sourceValue() {
            return value();
        }

        /** The {@link VectorType} this field produces. */
        public abstract VectorType vectorType();

        /**
         * Writes this field's full mapping fragment into {@code builder}, including the surrounding
         * {@code startObject(fieldName())} / {@code endObject()} pair.
         */
        public abstract void writeMapping(XContentBuilder builder) throws IOException;
    }

    static final String NON_VECTOR_FIELD = "non_vector_field";

    String indexName = null;
    List<C> vectorFields = new ArrayList<>();
    final boolean excludeSourceVectors;

    AbstractVectorFieldEmbeddingsFieldIT(@Name("excludeSourceVectors") boolean excludeSourceVectors) {
        this.excludeSourceVectors = excludeSourceVectors;
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), excludeSourceVectors)
            .build();
    }

    @Override
    protected int minimumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    /**
     * Sets the number of vector fields created for each test.
     */
    abstract int vectorFieldCount();

    /**
     * Creates one {@link VectorFieldConfig} for a field named {@code fieldName}.
     */
    abstract C createVectorFieldConfig(String fieldName);

    /**
     * The oldest index version that this test's randomly generated field configurations are valid on.
     */
    abstract IndexVersion minIndexVersion();

    /**
     * The oldest index version that this test can create an index on, accounting for both the randomly generated field configurations
     * ({@link #minIndexVersion()}) and the index settings under test.
     * {@link #testFetchEmbeddingsFieldsOldIndexVersions()} picks a random version between this and {@link IndexVersion#current()},
     * exclusive of current.
     */
    private IndexVersion effectiveMinIndexVersion() {
        // index.mapping.exclude_source_vectors was settable only on/after this index version
        return excludeSourceVectors ? IndexVersion.max(minIndexVersion(), IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT) : minIndexVersion();
    }

    /**
     * @param message a description of the assertion context for failure messages
     * @param field   the config of the field being asserted
     * @param actual  the {@link DocumentField} returned in the search hit
     */
    abstract void assertEmbeddingsFieldValue(String message, C field, DocumentField actual);

    /**
     * Returns the assertion applied to a search response for a document that has no value for any requested vector field.
     * The two fetch paths disagree here: {@code DOC_VALUES} always adds an empty {@link DocumentField} for each requested
     * field, while {@code FIELDS} omits the field entirely, so each subclass states the expectation for its own path.
     */
    abstract Consumer<SearchResponse> noFieldValueResponse(String message, Set<String> requestedFieldNames);

    @Before
    private void createVectorFields() {
        int numFields = vectorFieldCount();
        for (int i = 0; i < numFields; i++) {
            vectorFields.add(createVectorFieldConfig("vector_field_" + i));
        }
    }

    /**
     * Returns a {@link DocumentField}'s single value after asserting that the field contains exactly one value.
     *
     * @param message a description of the assertion context for failure messages
     * @param field   the {@link DocumentField} returned in the search hit
     * @param <V>     the expected type of the fetched value; the caller's declared type drives the cast
     * @return the single value
     */
    static <V> V singleValue(String message, DocumentField field) {
        assertThat(message + ": values", field.getValues(), hasSize(1));
        @SuppressWarnings("unchecked")
        V value = (V) field.getValues().getFirst();
        return value;
    }

    public void testFetchEmbeddingsFields() throws Exception {
        fetchEmbeddingsFieldsTestCase(IndexVersion.current());
    }

    public void testFetchEmbeddingsFieldsOldIndexVersions() throws Exception {
        for (int i = 0; i < 20; i++) {
            fetchEmbeddingsFieldsTestCase(
                IndexVersionUtils.randomVersionBetween(effectiveMinIndexVersion(), IndexVersionUtils.getPreviousVersion())
            );
        }
    }

    /**
     * When the search request returns no documents, the fetch phase is skipped. However, {@code embeddingsField} is called in
     * {@code SearchService#parseSource} at search-context creation, before any document matching, so a mismatched vector type
     * still fails even against an empty index.
     */
    public void testFetchEmbeddingsFieldsNoDocuments() throws Exception {
        indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setMapping(generateMapping()));
        ensureGreen(indexName);

        for (C field : vectorFields) {
            String fieldName = field.fieldName();
            String message = field.toString();

            assertEmbeddingsFieldsSuccess(singletonMap(fieldName, null), noHitsResponse(message));
            assertEmbeddingsFieldsSuccess(Map.of(fieldName, field.vectorType()), noHitsResponse(message));

            VectorType mismatched = randomValueOtherThan(field.vectorType(), () -> randomFrom(VectorType.values()));
            assertEmbeddingsFieldsFailure(
                Map.of(fieldName, mismatched),
                RestStatus.BAD_REQUEST,
                "Field [" + fieldName + "] of type [" + field.vectorType() + "] does not support [" + mismatched + "] embeddings"
            );
        }

        Map<String, VectorType> allRequested = new HashMap<>();
        vectorFields.forEach(f -> allRequested.put(f.fieldName(), null));
        assertEmbeddingsFieldsSuccess(allRequested, noHitsResponse("Fetching all vector fields at once"));
    }

    /**
     * When a document is indexed with no value for any vector field, the fetch phase runs. A mismatched vector type is rejected in
     * {@code SearchService#parseSource} at search-context creation, before any document matching, so it still fails here.
     * When the type matches (or is inferred), the response contains one hit whose fields depend on the fetch path: see
     * {@link #noFieldValueResponse}.
     */
    public void testFetchEmbeddingsFieldsNoFieldValue() throws Exception {
        indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setMapping(generateMapping()));

        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        bulk.add(client().prepareIndex(indexName).setSource(Map.of(NON_VECTOR_FIELD, randomAlphaOfLength(10))));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertNoFailures(bulk.get(TEST_REQUEST_TIMEOUT));
        ensureGreen(indexName);

        for (C field : vectorFields) {
            String fieldName = field.fieldName();
            String message = field.toString();

            assertEmbeddingsFieldsSuccess(singletonMap(fieldName, null), noFieldValueResponse(message, Set.of(fieldName)));
            assertEmbeddingsFieldsSuccess(Map.of(fieldName, field.vectorType()), noFieldValueResponse(message, Set.of(fieldName)));

            VectorType mismatched = randomValueOtherThan(field.vectorType(), () -> randomFrom(VectorType.values()));
            assertEmbeddingsFieldsFailure(
                Map.of(fieldName, mismatched),
                RestStatus.BAD_REQUEST,
                "Field [" + fieldName + "] of type [" + field.vectorType() + "] does not support [" + mismatched + "] embeddings"
            );
        }

        Map<String, VectorType> allRequested = new HashMap<>();
        vectorFields.forEach(f -> allRequested.put(f.fieldName(), null));
        assertEmbeddingsFieldsSuccess(allRequested, noFieldValueResponse("Fetching all vector fields at once", allRequested.keySet()));
    }

    void fetchEmbeddingsFieldsTestCase(IndexVersion indexVersion) throws Exception {
        if (excludeSourceVectors) {
            // index.mapping.exclude_source_vectors was settable only on/after this index version
            assertTrue(
                "Cannot set [" + INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey() + "] on index version [" + indexVersion + "]",
                indexVersion.onOrAfter(IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT)
            );
        }

        indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName, Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)).setMapping(
                generateMapping()
            )
        );

        Map<String, Object> source = new HashMap<>();
        for (C field : vectorFields) {
            source.put(field.fieldName(), field.sourceValue());
        }

        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        bulk.add(client().prepareIndex(indexName).setSource(source));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertNoFailures(bulk.get(TEST_REQUEST_TIMEOUT));
        ensureGreen(indexName);

        for (C field : vectorFields) {
            String fieldName = field.fieldName();
            String message = field.toString();

            // Inferred vector type: the field decides which type to return.
            assertEmbeddingsFieldsSuccess(singletonMap(fieldName, null), expectedFieldsResponse(message, Map.of(fieldName, field)));

            // Explicit matching vector type: same result.
            assertEmbeddingsFieldsSuccess(Map.of(fieldName, field.vectorType()), expectedFieldsResponse(message, Map.of(fieldName, field)));

            // Mismatched vector type: error
            VectorType mismatched = randomValueOtherThan(field.vectorType(), () -> randomFrom(VectorType.values()));
            assertEmbeddingsFieldsFailure(
                Map.of(fieldName, mismatched),
                RestStatus.BAD_REQUEST,
                "Field [" + fieldName + "] of type [" + field.vectorType() + "] does not support [" + mismatched + "] embeddings"
            );
        }

        // All fields at once.
        Map<String, VectorType> allRequested = new HashMap<>();
        Map<String, C> allExpected = new HashMap<>();
        for (C field : vectorFields) {
            allRequested.put(field.fieldName(), null);
            allExpected.put(field.fieldName(), field);
        }
        assertEmbeddingsFieldsSuccess(allRequested, expectedFieldsResponse("Fetching all vector fields at once", allExpected));
    }

    private XContentBuilder generateMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (C field : vectorFields) {
            field.writeMapping(builder);
        }
        builder.startObject(NON_VECTOR_FIELD).field("type", "keyword").endObject();
        return builder.endObject().endObject();
    }

    /**
     * Issues a {@link SearchSourceBuilder#fetchEmbeddingsField} search and passes the response to {@code responseConsumer}.
     * Uses the coordinating-only node so that fetched embedding field values are serialized over the wire (data node → coordinating
     * node), exercising transport serialization for both the FIELDS and DOC_VALUES fetch paths.
     *
     * @param requestedFields map of field name to requested {@link VectorType} (may be {@code null} to infer the type)
     * @param responseConsumer assertion to run against the successful response
     */
    private void assertEmbeddingsFieldsSuccess(Map<String, VectorType> requestedFields, Consumer<SearchResponse> responseConsumer) {
        SearchSourceBuilder source = new SearchSourceBuilder();
        requestedFields.forEach(source::fetchEmbeddingsField);
        assertNoFailuresAndResponse(internalCluster().coordOnlyNodeClient().prepareSearch(indexName).setSource(source), responseConsumer);
    }

    /**
     * Issues a {@link SearchSourceBuilder#fetchEmbeddingsField} search and asserts that it fails with the given status and reason.
     *
     * @param requestedFields map of field name to requested {@link VectorType} (may be {@code null} to infer the type)
     * @param expectedStatus  the expected HTTP status of each shard failure
     * @param expectedReason  a substring expected to appear in each shard failure reason
     */
    private void assertEmbeddingsFieldsFailure(Map<String, VectorType> requestedFields, RestStatus expectedStatus, String expectedReason) {
        SearchSourceBuilder source = new SearchSourceBuilder();
        requestedFields.forEach(source::fetchEmbeddingsField);
        assertFailures(
            internalCluster().coordOnlyNodeClient().prepareSearch(indexName).setSource(source),
            expectedStatus,
            containsString(expectedReason)
        );
    }

    /**
     * Returns a consumer that asserts no hits are returned.
     *
     * @param message a description of the assertion context for failure messages
     */
    private Consumer<SearchResponse> noHitsResponse(String message) {
        return response -> assertThat(message, response.getHits().getTotalHits().value(), equalTo(0L));
    }

    /**
     * Returns a consumer that asserts exactly one hit is returned containing the expected fields with the expected values.
     *
     * @param message        a description of the assertion context for failure messages
     * @param expectedFields map of field name to its {@link VectorFieldConfig}; empty when no fields are expected in the hit
     */
    private Consumer<SearchResponse> expectedFieldsResponse(String message, Map<String, C> expectedFields) {
        return response -> {
            assertThat(message, response.getHits().getTotalHits().value(), equalTo(1L));
            SearchHit hit = response.getHits().getAt(0);
            assertThat(message, hit.getDocumentFields().size(), equalTo(expectedFields.size()));
            for (Map.Entry<String, C> entry : expectedFields.entrySet()) {
                String fieldName = entry.getKey();
                C fieldConfig = entry.getValue();
                DocumentField documentField = hit.getDocumentFields().get(fieldName);
                assertThat(message + ": expected field [" + fieldName + "] in hit", documentField, notNullValue());
                assertEmbeddingsFieldValue(fieldConfig.toString(), fieldConfig, documentField);
            }
        };
    }
}

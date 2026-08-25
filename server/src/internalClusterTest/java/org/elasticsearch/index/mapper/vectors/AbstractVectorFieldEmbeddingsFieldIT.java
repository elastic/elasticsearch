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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.inference.VectorType;
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

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
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
     * {@link #testFetchEmbeddingsFieldsOldIndexVersions()} picks a random version between this and
     * {@link IndexVersion#current()}, exclusive of current.
     */
    abstract IndexVersion minIndexVersion();

    /**
     * @param message a description of the assertion context for failure messages
     * @param field   the config of the field being asserted
     * @param actual  the {@link DocumentField} returned in the search hit
     */
    abstract void assertEmbeddingsFieldValue(String message, C field, DocumentField actual);

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
            IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(minIndexVersion(), IndexVersionUtils.getPreviousVersion());
            while (indexVersion.before(IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT) && excludeSourceVectors) {
                // index.mapping.exclude_source_vectors was settable only on/after this index version
                indexVersion = IndexVersionUtils.randomVersionBetween(minIndexVersion(), IndexVersionUtils.getPreviousVersion());
            }

            fetchEmbeddingsFieldsTestCase(indexVersion);
        }
    }

    /**
     * When the search request returns no documents, the fetch phase is skipped and the vector type match check in {@code embeddingsField}
     * isn't executed.
     */
    public void testFetchEmbeddingsFieldsNoDocuments() throws Exception {
        indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setMapping(generateMapping()));
        ensureGreen(indexName);

        for (C field : vectorFields) {
            String fieldName = field.fieldName();
            String message = field.toString();

            assertEmbeddingsFieldsNoHits(message, singletonMap(fieldName, null));
            assertEmbeddingsFieldsNoHits(message, Map.of(fieldName, field.vectorType()));
            assertEmbeddingsFieldsNoHits(
                message,
                Map.of(fieldName, randomValueOtherThan(field.vectorType(), () -> randomFrom(VectorType.values())))
            );
        }

        Map<String, VectorType> allRequested = new HashMap<>();
        vectorFields.forEach(f -> allRequested.put(f.fieldName(), null));
        assertEmbeddingsFieldsNoHits("Fetching all vector fields at once", allRequested);
    }

    // TODO: Add no field value test

    void fetchEmbeddingsFieldsTestCase(IndexVersion indexVersion) throws Exception {
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
            assertEmbeddingsFieldsHit(message, singletonMap(fieldName, null), Map.of(fieldName, field));

            // Explicit matching vector type: same result.
            assertEmbeddingsFieldsHit(message, Map.of(fieldName, field.vectorType()), Map.of(fieldName, field));

            // Mismatched vector type: embeddingsField() returns null, so the field is skipped and the hit has no fields.
            assertEmbeddingsFieldsHit(
                message,
                Map.of(fieldName, randomValueOtherThan(field.vectorType(), () -> randomFrom(VectorType.values()))),
                Map.of()
            );
        }

        // All fields at once.
        Map<String, VectorType> allRequested = new HashMap<>();
        Map<String, C> allExpected = new HashMap<>();
        for (C field : vectorFields) {
            allRequested.put(field.fieldName(), null);
            allExpected.put(field.fieldName(), field);
        }
        assertEmbeddingsFieldsHit("Fetching all vector fields at once", allRequested, allExpected);
    }

    private XContentBuilder generateMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (C field : vectorFields) {
            field.writeMapping(builder);
        }
        return builder.endObject().endObject();
    }

    /**
     * Issues a {@link SearchSourceBuilder#fetchEmbeddingsField} search and asserts that exactly one hit is returned containing the
     * expected fields with the expected values.
     *
     * @param message        a description of the assertion context for failure messages
     * @param requestedFields map of field name to requested {@link VectorType} (may be {@code null} to infer the type)
     * @param expectedFields  map of field name to its {@link VectorFieldConfig}; empty when no fields are expected in the hit
     */
    private void assertEmbeddingsFieldsHit(String message, Map<String, VectorType> requestedFields, Map<String, C> expectedFields)
        throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder();
        requestedFields.forEach(source::fetchEmbeddingsField);

        // Use the coordinating-only node so that fetched embedding field values are serialized over the wire (data node → coordinating
        // node), exercising transport serialization for both the FIELDS and DOC_VALUES fetch paths.
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { indexName }, source)),
            response -> {
                assertThat(message, response.getHits().getTotalHits().value(), equalTo(1L));
                SearchHit hit = response.getHits().getAt(0);
                assertThat(message, hit.getFields().size(), equalTo(expectedFields.size()));
                for (Map.Entry<String, C> entry : expectedFields.entrySet()) {
                    String fieldName = entry.getKey();
                    C fieldConfig = entry.getValue();
                    DocumentField documentField = hit.field(fieldName);
                    assertThat(message + ": expected field [" + fieldName + "] in hit", documentField, notNullValue());
                    assertEmbeddingsFieldValue(fieldConfig.toString(), fieldConfig, documentField);
                }
            }
        );
    }

    /**
     * Issues a {@link SearchSourceBuilder#fetchEmbeddingsField} search against an empty index and asserts that no hits are returned.
     *
     * @param message        a description of the assertion context for failure messages
     * @param requestedFields map of field name to requested {@link VectorType} (may be {@code null} to infer the type)
     */
    private void assertEmbeddingsFieldsNoHits(String message, Map<String, VectorType> requestedFields) throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder();
        requestedFields.forEach(source::fetchEmbeddingsField);

        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { indexName }, source)),
            response -> assertThat(message, response.getHits().getTotalHits().value(), equalTo(0L))
        );
    }
}

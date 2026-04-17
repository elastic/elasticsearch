/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.Utils.storeDenseModel;
import static org.elasticsearch.xpack.inference.Utils.storeSparseModel;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests ported from {@code 10_semantic_text_field_mapping_bwc.yml}, covering
 * mapping population, field_caps, index options, and inference_id updates for legacy-format
 * semantic_text indices.
 */
public class SemanticTextFieldMappingLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    /**
     * Ported from "Indexes sparse vector document" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Creates a legacy index with a semantic_text field that has no initial model_settings,
     * confirms the settings are absent before indexing, then indexes a document and confirms
     * that model_settings are subsequently populated in the mapping.
     */
    public void testLegacyFormatMappingPopulatedAfterFirstSparseDocument() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(SPARSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Before indexing: model_settings should be absent
        assertMappingModelSettings(indexName, SPARSE_FIELD, false);

        // Index a document to trigger inference and mapping population
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "mapping test")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: model_settings should be present
        assertMappingModelSettings(indexName, SPARSE_FIELD, true);
    }

    /**
     * Ported from "Indexes dense vector document" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Creates a legacy index with a dense semantic_text field that has no initial model_settings,
     * confirms the settings are absent before indexing, then indexes a document and confirms
     * that model_settings (including dimensions, similarity, and element_type) are subsequently
     * populated in the mapping.
     */
    public void testLegacyFormatMappingPopulatedAfterFirstDenseDocument() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Before indexing: model_settings should be absent
        assertMappingModelSettings(indexName, DENSE_FIELD, false);

        // Index a document to trigger inference and mapping population
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "dense mapping test")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: model_settings should be present with dense-specific fields
        assertMappingModelSettings(indexName, DENSE_FIELD, true);
        assertDenseMappingModelSettings(indexName, DENSE_FIELD);
    }

    /**
     * Ported from "Field caps with sparse embedding" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies field_caps behaviour for a legacy index containing a sparse semantic_text field.
     * Before indexing, {@code include_empty_fields=false} returns neither field. After indexing a
     * sparse document, {@code include_empty_fields=false} returns the sparse field but not the
     * dense field. The text sub-field of the sparse field should be searchable.
     */
    public void testLegacyFormatFieldCapsWithSparseEmbedding() throws Exception {
        createLegacyIndex();

        // Before indexing: include_empty_fields=true → both fields present
        FieldCapabilitiesResponse withEmpty = fieldCaps(true);
        assertNotNull(withEmpty.getField(SPARSE_FIELD));
        assertNotNull(withEmpty.getField(DENSE_FIELD));

        // Before indexing: include_empty_fields=false → neither field present
        FieldCapabilitiesResponse withoutEmpty = fieldCaps(false);
        assertNull(withoutEmpty.getField(SPARSE_FIELD));
        assertNull(withoutEmpty.getField(DENSE_FIELD));

        // Index one sparse document and refresh
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "these are not the droids you're looking for")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: include_empty_fields=true → both fields present, sparse text sub-field searchable
        FieldCapabilitiesResponse withEmptyAfter = fieldCaps(true);
        assertNotNull(withEmptyAfter.getField(SPARSE_FIELD));
        assertNotNull(withEmptyAfter.getField(DENSE_FIELD));
        assertThat(withEmptyAfter.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
        assertThat(withEmptyAfter.getField(DENSE_FIELD).get("text").isSearchable(), equalTo(true));

        // After indexing: include_empty_fields=false → sparse_field present, dense_field absent
        FieldCapabilitiesResponse withoutEmptyAfter = fieldCaps(false);
        assertNotNull(withoutEmptyAfter.getField(SPARSE_FIELD));
        assertNull(withoutEmptyAfter.getField(DENSE_FIELD));
        assertThat(withoutEmptyAfter.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
    }

    /**
     * Ported from "Indexes dense vector document with bbq compatible model" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that indexing a document into a legacy-format index backed by a BBQ-compatible model
     * does NOT auto-populate {@code index_options} in the mapping. In the new (non-legacy) format
     * the mapper would inject default index_options from the inference service; the legacy format
     * must not do so.
     */
    public void testLegacyFormatBbqCompatibleModelHasNoAutoIndexOptions() throws Exception {
        storeDenseModel(DENSE_BBQ_INFERENCE_ID, modelRegistry, 64, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);

        String bbqIndexName = indexName + "_bbq";
        assertAcked(
            prepareCreate(bbqIndexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_BBQ_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            // Before indexing: no model_settings and no index_options
            assertMappingModelSettings(bbqIndexName, DENSE_FIELD, false);
            assertMappingHasNoIndexOptions(bbqIndexName, DENSE_FIELD);

            // Index one document
            client().prepareIndex(bbqIndexName).setSource(Map.of(DENSE_FIELD, "these are not the droids you're looking for")).get();
            client().admin().indices().prepareRefresh(bbqIndexName).get();

            // After indexing: model_settings present, but still no index_options (legacy does not auto-populate)
            assertMappingModelSettings(bbqIndexName, DENSE_FIELD, true);
            assertMappingHasNoIndexOptions(bbqIndexName, DENSE_FIELD);
        } finally {
            IntegrationTestUtils.deleteIndex(client(), bbqIndexName);
        }
    }

    /**
     * Ported from "Field caps with text embedding" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies field_caps behaviour for a legacy index containing a dense semantic_text field.
     * After indexing a dense document, {@code include_empty_fields=false} returns the dense field
     * but not the sparse field.
     */
    public void testLegacyFormatFieldCapsWithTextEmbedding() throws Exception {
        createLegacyIndex();

        // Before indexing: include_empty_fields=false → neither field present
        FieldCapabilitiesResponse withoutEmpty = fieldCaps(false);
        assertNull(withoutEmpty.getField(SPARSE_FIELD));
        assertNull(withoutEmpty.getField(DENSE_FIELD));

        // Index one dense document and refresh
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "these are not the droids you're looking for")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: include_empty_fields=true → both fields present
        FieldCapabilitiesResponse withEmptyAfter = fieldCaps(true);
        assertNotNull(withEmptyAfter.getField(SPARSE_FIELD));
        assertNotNull(withEmptyAfter.getField(DENSE_FIELD));
        assertThat(withEmptyAfter.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
        assertThat(withEmptyAfter.getField(DENSE_FIELD).get("text").isSearchable(), equalTo(true));

        // After indexing: include_empty_fields=false → dense_field present, sparse_field absent
        FieldCapabilitiesResponse withoutEmptyAfter = fieldCaps(false);
        assertNull(withoutEmptyAfter.getField(SPARSE_FIELD));
        assertNotNull(withoutEmptyAfter.getField(DENSE_FIELD));
        assertThat(withoutEmptyAfter.getField(DENSE_FIELD).get("text").isSearchable(), equalTo(true));
    }

    /**
     * Ported from "Field caps exclude chunks embedding and text fields" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that field_caps does not expose the internal sub-fields of semantic_text
     * ({@code inference}, {@code inference.chunks}, {@code inference.chunks.embeddings}, etc.).
     */
    public void testLegacyFormatFieldCapsExcludesSubFields() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "test text", DENSE_FIELD, "test text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        FieldCapabilitiesResponse response = fieldCaps(true);
        assertNotNull(response.getField(SPARSE_FIELD));
        assertNotNull(response.getField(DENSE_FIELD));

        // Internal sub-fields must NOT be exposed
        assertNull(response.getField(SPARSE_FIELD + ".inference"));
        assertNull(response.getField(SPARSE_FIELD + ".inference.chunks"));
        assertNull(response.getField(SPARSE_FIELD + ".inference.chunks.embeddings"));
        assertNull(response.getField(DENSE_FIELD + ".inference"));
        assertNull(response.getField(DENSE_FIELD + ".inference.chunks"));
        assertNull(response.getField(DENSE_FIELD + ".inference.chunks.embeddings"));
    }

    /**
     * Ported from "Field caps with semantic query does not fail" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that using a {@code SemanticQueryBuilder} as an index filter in a field_caps request
     * does not cause a failure and returns the expected fields.
     */
    public void testLegacyFormatFieldCapsWithSemanticQueryFilter() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "This is a story about a cat and a dog.")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices(indexName);
        request.fields("*");
        request.indexFilter(new SemanticQueryBuilder(SPARSE_FIELD, "test"));

        FieldCapabilitiesResponse response = client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
        assertNotNull(response.getField(SPARSE_FIELD));
        assertThat(response.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
    }

    /**
     * Ported from "Users can set dense vector index options and index documents using those options" in
     * {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Creates a legacy index with explicit {@code int8_hnsw} dense index options, verifies the
     * options are persisted in the mapping before and after indexing a document.
     */
    public void testLegacyFormatDenseIndexOptionsPreserved() throws Exception {
        String optionsIndex = indexName + "_options";
        assertAcked(
            prepareCreate(optionsIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .field("m", 20)
                        .field("ef_construction", 100)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            assertDenseIndexOptions(optionsIndex, DENSE_FIELD, "int8_hnsw", 20, 100);

            client().prepareIndex(optionsIndex).setSource(Map.of(DENSE_FIELD, "mapping test")).get();
            client().admin().indices().prepareRefresh(optionsIndex).get();

            assertDenseIndexOptions(optionsIndex, DENSE_FIELD, "int8_hnsw", 20, 100);
        } finally {
            IntegrationTestUtils.deleteIndex(client(), optionsIndex);
        }
    }

    /**
     * Ported from "Updating index options" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Creates a legacy index with dense index options and verifies they can be updated (m and
     * ef_construction) but not with an incompatible type change.
     */
    public void testLegacyFormatDenseIndexOptionsUpdate() throws Exception {
        String optionsIndex = indexName + "_options_update";
        assertAcked(
            prepareCreate(optionsIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .field("m", 16)
                        .field("ef_construction", 100)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            assertDenseIndexOptions(optionsIndex, "semantic_field", "int8_hnsw", 16, 100);

            // Update m and ef_construction (compatible change)
            indicesAdmin().preparePutMapping(optionsIndex)
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .field("m", 20)
                        .field("ef_construction", 90)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get();

            assertDenseIndexOptions(optionsIndex, "semantic_field", "int8_hnsw", 20, 90);

            // Changing the type is incompatible and must fail
            assertThrows(
                Exception.class,
                () -> indicesAdmin().preparePutMapping(optionsIndex)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("properties")
                            .startObject("semantic_field")
                            .field("type", "semantic_text")
                            .field("inference_id", DENSE_INFERENCE_ID)
                            .startObject("index_options")
                            .startObject("dense_vector")
                            .field("type", "int8_flat")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                    .get()
            );
        } finally {
            IntegrationTestUtils.deleteIndex(client(), optionsIndex);
        }
    }

    /**
     * Ported from "Can't be used as a multifield" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that a {@code semantic_text} field cannot be configured as a multi-field of another field.
     */
    public void testLegacyFormatCantBeUsedAsMultifield() throws Exception {
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_multi").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("text_field")
                        .field("type", "text")
                        .startObject("fields")
                        .startObject("semantic")
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    /**
     * Ported from "Can't have multifields" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that a {@code semantic_text} field cannot itself have multi-fields.
     */
    public void testLegacyFormatCantHaveMultifields() throws Exception {
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_multi").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic")
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .startObject("fields")
                        .startObject("keyword_field")
                        .field("type", "keyword")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    /**
     * Ported from "Can't configure copy_to in semantic_text" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that a {@code semantic_text} field cannot have a {@code copy_to} parameter.
     */
    public void testLegacyFormatCantConfigureCopyTo() throws Exception {
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_copy_to").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic")
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .field("copy_to", "another_field")
                        .endObject()
                        .startObject("another_field")
                        .field("type", "keyword")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    /**
     * Ported from "Specifying incompatible dense vector index options will fail" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that providing incompatible dense vector index options (e.g. {@code bbq_flat} with
     * {@code ef_construction}) is rejected when creating a legacy-format index.
     */
    public void testLegacyFormatIncompatibleDenseIndexOptionsFails() throws Exception {
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_incompat_opts").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "bbq_flat")
                        .field("ef_construction", 100)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    /**
     * Ported from "Specifying unsupported index option types will fail" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that an unknown dense-vector index options type is rejected, and that sparse-vector
     * index options with an invalid structure are also rejected.
     */
    public void testLegacyFormatUnsupportedIndexOptionTypeFails() throws Exception {
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_invalid_dense").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "foo")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_invalid_sparse").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .startObject("index_options")
                        .startObject("sparse_vector")
                        .field("type", "int8_hnsw")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    /**
     * Ported from "Index option type is required when specifying more than element_type" in
     * {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that specifying additional dense-vector index option parameters beyond
     * {@code element_type} without also providing {@code type} is rejected.
     */
    public void testLegacyFormatIndexOptionTypeRequiredWhenMoreThanElementType() throws Exception {
        Exception e = expectThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_type_required").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("foo", "bar")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThat(
            ExceptionsHelper.unwrapCause(e).getMessage(),
            containsString("[type] is required when specifying more params than [element_type]")
        );
    }

    /**
     * Ported from "Index option element_type or type is required" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that specifying an empty {@code dense_vector} block (neither {@code element_type}
     * nor {@code type}) is rejected.
     */
    public void testLegacyFormatIndexOptionElementTypeOrTypeRequired() throws Exception {
        Exception e = expectThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_elem_type_req").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        // empty — neither element_type nor type
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("Must specify at least [element_type] or [type]"));
    }

    /**
     * Ported from "Invalid element type" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that specifying an unknown {@code element_type} value is rejected.
     */
    public void testLegacyFormatInvalidElementType() throws Exception {
        Exception e = expectThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_invalid_elem").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("element_type", "foo")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("Invalid element_type [foo]"));
    }

    /**
     * Ported from "Specifying index options requires model information" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies two things:
     * <ol>
     *   <li>Specifying {@code index_options} when the inference endpoint does not exist fails with
     *       a meaningful error.</li>
     *   <li>Creating a field without {@code index_options} using a nonexistent endpoint succeeds.</li>
     * </ol>
     */
    public void testLegacyFormatIndexOptionsRequiresModelInformation() throws Exception {
        // Part 1: providing index_options with a nonexistent inference ID must fail
        assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "_model_info_fail").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", "nonexistent-inference-id")
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Part 2: creating without index_options using a nonexistent endpoint must succeed
        String noOptionsIndex = indexName + "_model_info_ok";
        assertAcked(
            prepareCreate(noOptionsIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", "nonexistent-inference-id")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        try {
            assertMappingHasNoIndexOptions(noOptionsIndex, "semantic_field");
        } finally {
            IntegrationTestUtils.deleteIndex(client(), noOptionsIndex);
        }
    }

    /**
     * Ported from "Cannot update element type in index options" in {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that once an {@code element_type} is set in the dense-vector index options it cannot
     * be changed to a different explicit value (e.g. {@code bfloat16}).
     */
    public void testLegacyFormatCannotUpdateElementTypeInIndexOptions() throws Exception {
        String optionsIndex = indexName + "_elem_type_update";
        assertAcked(
            prepareCreate(optionsIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("element_type", "float")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        try {
            // Explicit update to bfloat16 must fail
            Exception e = expectThrows(
                Exception.class,
                () -> indicesAdmin().preparePutMapping(optionsIndex)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("properties")
                            .startObject("semantic_field")
                            .field("type", "semantic_text")
                            .field("inference_id", DENSE_INFERENCE_ID)
                            .startObject("index_options")
                            .startObject("dense_vector")
                            .field("element_type", "bfloat16")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                    .get()
            );
            assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("Cannot update parameter [element_type]"));
        } finally {
            IntegrationTestUtils.deleteIndex(client(), optionsIndex);
        }
    }

    /**
     * Ported from "Updating inference_id to compatible endpoint should succeed given model settings" in
     * {@code 10_semantic_text_field_mapping_bwc.yml}.
     * Verifies that the inference_id of a semantic_text field can be updated to a compatible
     * endpoint. After updating, documents indexed before the change retain the old inference_id
     * in their stored source, and documents indexed after the change use the new inference_id.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatInferenceIdUpdate() throws Exception {
        createLegacyIndex();

        // Index first document with the original inference_id
        client().prepareIndex(indexName).setId("doc_1").setSource(Map.of(SPARSE_FIELD, "This is a story about a cat and a dog.")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Verify mapping shows original inference_id and model_settings
        assertMappingInferenceId(indexName, SPARSE_FIELD, SPARSE_INFERENCE_ID);
        assertMappingModelSettings(indexName, SPARSE_FIELD, true);

        // Register the second sparse model and update the inference_id
        storeSparseModel(SPARSE_INFERENCE_ID_2, modelRegistry);
        indicesAdmin().preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(SPARSE_FIELD)
                    .field("type", "semantic_text")
                    .field("inference_id", SPARSE_INFERENCE_ID_2)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        // Verify mapping shows updated inference_id
        assertMappingInferenceId(indexName, SPARSE_FIELD, SPARSE_INFERENCE_ID_2);

        // Index second document with the updated inference_id
        client().prepareIndex(indexName).setId("doc_2").setSource(Map.of(SPARSE_FIELD, "One day they started playing the piano.")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Search and verify per-doc inference_id in stored source
        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(SPARSE_FIELD, "piano"))
                        .excludeVectors(false)
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 2L);
                // Both docs are returned; locate each by _id and verify inference_id
                for (int i = 0; i < 2; i++) {
                    String docId = response.getHits().getAt(i).getId();
                    Map<String, Object> source = response.getHits().getAt(i).getSourceAsMap();
                    Map<String, Object> fieldMap = (Map<String, Object>) source.get(SPARSE_FIELD);
                    assertNotNull("source for " + docId + " should have " + SPARSE_FIELD, fieldMap);
                    Map<String, Object> inferenceMap = (Map<String, Object>) fieldMap.get("inference");
                    assertNotNull("inference should be present for " + docId, inferenceMap);
                    String expectedInferenceId = "doc_1".equals(docId) ? SPARSE_INFERENCE_ID : SPARSE_INFERENCE_ID_2;
                    assertThat("inference_id for " + docId, inferenceMap.get("inference_id"), equalTo(expectedInferenceId));
                }
            }
        );
    }
}

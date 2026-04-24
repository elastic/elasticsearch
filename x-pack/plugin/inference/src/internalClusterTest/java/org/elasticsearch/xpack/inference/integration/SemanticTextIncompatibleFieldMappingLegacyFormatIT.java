/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.index.mapper.DocumentParsingException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests ported from {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml},
 * covering rejection of legacy-format documents that have incompatible embedding metadata.
 */
public class SemanticTextIncompatibleFieldMappingLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    /**
     * Ported from "Fails for non-compatible dimensions" in {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a dense embedding whose dimensions differ from the
     * model_settings stored in the mapping, and asserts that the indexing fails.
     */
    public void testLegacyFormatIncompatibleDimensionsFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> source = buildLegacyDenseDoc(
            DENSE_FIELD,
            "other text",
            DENSE_INFERENCE_ID,
            5,
            "cosine",
            "float",
            List.of(0.1, 0.2, 0.3, 0.4, 0.5)
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
        assertThat(fullExceptionChain(e), containsString("Incompatible model settings for field [dense_field]"));
    }

    /**
     * Ported from "Fails for non-compatible inference id" in {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a dense embedding whose inference_id does not match
     * the field's configured inference_id, and asserts that the indexing fails.
     */
    public void testLegacyFormatIncompatibleInferenceIdFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> source = buildLegacyDenseDoc(
            DENSE_FIELD,
            "other text",
            "a-different-inference-id",
            10,
            "cosine",
            "float",
            List.of(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
        assertThat(
            fullExceptionChain(e),
            containsString(
                "The configured inference_id [a-different-inference-id] for field [dense_field]"
                    + " doesn't match the inference_id [dense-inference-id]"
            )
        );
    }

    /**
     * Ported from "Fails for non-compatible similarity" in {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a dense embedding whose similarity does not match the
     * model_settings, and asserts that the indexing fails.
     */
    public void testLegacyFormatIncompatibleSimilarityFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> source = buildLegacyDenseDoc(
            DENSE_FIELD,
            "other text",
            DENSE_INFERENCE_ID,
            10,
            "dot_product",
            "float",
            List.of(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
        assertThat(fullExceptionChain(e), containsString("Incompatible model settings for field [dense_field]"));
    }

    /**
     * Ported from "Fails for non-compatible element type" in {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a dense embedding whose element_type does not match
     * the model_settings, and asserts that the indexing fails.
     */
    public void testLegacyFormatIncompatibleElementTypeFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> source = buildLegacyDenseDoc(
            DENSE_FIELD,
            "other text",
            DENSE_INFERENCE_ID,
            10,
            "cosine",
            "byte",
            List.of(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
        assertThat(fullExceptionChain(e), containsString("Incompatible model settings for field [dense_field]"));
    }

    /**
     * Ported from "Fails for non-compatible task type for dense vectors" in
     * {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document into a dense field using {@code sparse_embedding} as the
     * task_type (wrong task type for a dense field), and asserts that the indexing fails.
     */
    public void testLegacyFormatIncompatibleTaskTypeDenseFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> source = buildLegacySparseDoc(
            DENSE_FIELD,
            "other text",
            DENSE_INFERENCE_ID,
            "sparse_embedding",
            Map.of("feature_0", 1.0f, "feature_1", 2.0f, "feature_2", 3.0f, "feature_3", 4.0f)
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
        assertThat(fullExceptionChain(e), containsString("Incompatible model settings for field [dense_field]"));
    }

    /**
     * Ported from "Fails for non-compatible task type for sparse vectors" in
     * {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document into a sparse field using {@code text_embedding} as the
     * task_type (wrong task type for a sparse field), and asserts that the indexing fails.
     */
    public void testLegacyFormatIncompatibleTaskTypeSparseFails() throws Exception {
        createLegacyIndex();
        // Index the setup doc using the full legacy format so that model_settings are explicitly
        // written to the mapping (plain-string indexing via the fake sparse model does not update
        // model_settings in the same way).
        client().prepareIndex(indexName)
            .setSource(buildLegacySparseDoc(SPARSE_FIELD, "setup text", SPARSE_INFERENCE_ID, "sparse_embedding", Map.of("feature_0", 1.0f)))
            .get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> source = buildLegacyDenseDoc(
            SPARSE_FIELD,
            "other text",
            SPARSE_INFERENCE_ID,
            4,
            "cosine",
            "float",
            List.of(0.1, 0.2, 0.3, 0.4)
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
        assertThat(fullExceptionChain(e), containsString("Incompatible model settings for field [sparse_field]"));
    }

    /**
     * Ported from "Fails for missing dense vector inference results in chunks" in
     * {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a dense chunk that has no embeddings, and asserts
     * that the indexing fails with a parse error.
     */
    public void testLegacyFormatMissingDenseEmbeddingsFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> ms = new HashMap<>();
        ms.put("task_type", "text_embedding");
        ms.put("dimensions", 10);
        ms.put("similarity", "cosine");
        ms.put("element_type", "float");

        Map<String, Object> inference = new HashMap<>();
        inference.put("inference_id", DENSE_INFERENCE_ID);
        inference.put("model_settings", ms);
        inference.put("chunks", List.of(Map.of("text", "other text")));  // no embeddings

        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("text", "other text");
        fieldMap.put("inference", inference);

        Map<String, Object> source = Map.of(DENSE_FIELD, fieldMap);
        expectThrows(
            DocumentParsingException.class,
            containsString("failed to parse field [dense_field] of type [semantic_text]"),
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
    }

    /**
     * Ported from "Fails for missing sparse vector inference results in chunks" in
     * {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a sparse chunk that has no embeddings, and asserts
     * that the indexing fails with a parse error.
     */
    public void testLegacyFormatMissingSparseEmbeddingsFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> ms = new HashMap<>();
        ms.put("task_type", "sparse_embedding");

        Map<String, Object> inference = new HashMap<>();
        inference.put("inference_id", SPARSE_INFERENCE_ID);
        inference.put("model_settings", ms);
        inference.put("chunks", List.of(Map.of("text", "other text")));  // no embeddings

        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("text", "other text");
        fieldMap.put("inference", inference);

        Map<String, Object> source = Map.of(SPARSE_FIELD, fieldMap);
        expectThrows(
            DocumentParsingException.class,
            containsString("failed to parse field [sparse_field] of type [semantic_text]"),
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
    }

    /**
     * Ported from "Fails for missing text in chunks" in {@code 20_semantic_text_field_mapping_incompatible_field_mapping_bwc.yml}.
     * Indexes a legacy-format document with a dense chunk that has no text field, and asserts
     * that the indexing fails with a parse error.
     */
    public void testLegacyFormatMissingChunkTextFails() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "setup text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        Map<String, Object> ms = new HashMap<>();
        ms.put("task_type", "text_embedding");
        ms.put("dimensions", 10);
        ms.put("similarity", "cosine");
        ms.put("element_type", "float");

        Map<String, Object> inference = new HashMap<>();
        inference.put("inference_id", DENSE_INFERENCE_ID);
        inference.put("model_settings", ms);
        // chunk with embeddings but no text
        inference.put("chunks", List.of(Map.of("embeddings", List.of(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0))));

        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("text", "other text");
        fieldMap.put("inference", inference);

        Map<String, Object> source = Map.of(DENSE_FIELD, fieldMap);
        expectThrows(
            DocumentParsingException.class,
            containsString("failed to parse field [dense_field] of type [semantic_text]"),
            () -> client().prepareIndex(indexName).setSource(source).get()
        );
    }

    /**
     * Returns a concatenation of every message in the given exception's cause chain, separated by
     * {@code " | "}. This mirrors how the YAML REST test framework evaluates {@code catch} regex
     * patterns — against the full serialised error body, which includes all {@code caused_by}
     * entries — so that Java and YAML tests assert the same strings.
     */
    private static String fullExceptionChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable cause = t;
        while (cause != null) {
            if (cause.getMessage() != null) {
                if (sb.length() > 0) {
                    sb.append(" | ");
                }
                sb.append(cause.getMessage());
            }
            cause = cause.getCause();
        }
        return sb.toString();
    }
}

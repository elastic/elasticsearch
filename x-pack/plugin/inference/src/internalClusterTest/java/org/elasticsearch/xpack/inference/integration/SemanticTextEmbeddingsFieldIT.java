/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT;
import static org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion;

public class SemanticTextEmbeddingsFieldIT extends AbstractEmbeddingsFieldIT {
    private static final String SPARSE_EMBEDDING_INFERENCE_ID = "sparse-embedding-test-endpoint";
    private static final String TEXT_EMBEDDING_INFERENCE_ID = "text-embedding-test-endpoint";
    private static final String EMBEDDING_INFERENCE_ID = "embedding-test-endpoint";

    private final boolean useLegacyFormat;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { false }, new Object[] { true });
    }

    public SemanticTextEmbeddingsFieldIT(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @Before
    public void setUpInferenceEndpoint() throws IOException {
        createInferenceEndpoint(TaskType.SPARSE_EMBEDDING, SPARSE_EMBEDDING_INFERENCE_ID);
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, TEXT_EMBEDDING_INFERENCE_ID);
        createInferenceEndpoint(TaskType.EMBEDDING, EMBEDDING_INFERENCE_ID);
    }

    @Override
    Map<String, String> getFields() {
        return Map.of(
            "sparse_embedding_field",
            SPARSE_EMBEDDING_INFERENCE_ID,
            "text_embedding_field",
            TEXT_EMBEDDING_INFERENCE_ID,
            "embedding_field",
            EMBEDDING_INFERENCE_ID
        );
    }

    @Override
    XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException {
        return IntegrationTestUtils.generateSemanticTextMapping(fieldNameToInferenceIdMap);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    public Settings indexSettings() {
        Settings settings = super.indexSettings();
        if (useLegacyFormat) {
            IndexVersion indexVersion = getRandomCompatibleIndexVersion(true);
            settings = Settings.builder()
                .put(settings)
                .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
                .put(USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true)
                .build();
        }

        return settings;
    }
}

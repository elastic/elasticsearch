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
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT;
import static org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion;

public class SemanticTextFieldEmbeddingsFieldIT extends AbstractInferenceFieldEmbeddingsFieldIT {
    private static final Set<TaskType> SUPPORTED_TASK_TYPES = Set.of(
        TaskType.SPARSE_EMBEDDING,
        TaskType.TEXT_EMBEDDING,
        TaskType.EMBEDDING
    );

    private final boolean useLegacyFormat;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { false }, new Object[] { true });
    }

    public SemanticTextFieldEmbeddingsFieldIT(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @Override
    Set<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    String fieldTypeName() {
        return SemanticTextFieldMapper.CONTENT_TYPE;
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

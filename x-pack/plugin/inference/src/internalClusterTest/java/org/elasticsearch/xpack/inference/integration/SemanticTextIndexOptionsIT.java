/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.inference.mapper.ExtendedDenseVectorIndexOptions;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.util.Map;

import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_USES_DENSE_VECTOR_DEFAULT_INDEX_OPTIONS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;

public class SemanticTextIndexOptionsIT extends SemanticFieldIndexOptionsTestCase {

    @Override
    protected String fieldType() {
        return SemanticTextFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
    }

    public void testSetDefaultBBQIndexOptionsWithBasicLicense() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, FLOAT_SERVICE_SETTINGS);
        downgradeLicenseAndRestartCluster();

        for (int i = 0; i < 20; i++) {
            IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(
                SEMANTIC_TEXT_DEFAULTS_TO_BBQ,
                IndexVersionUtils.getPreviousVersion(SEMANTIC_TEXT_USES_DENSE_VECTOR_DEFAULT_INDEX_OPTIONS)
            );
            assertAcked(
                safeGet(
                    prepareCreate(INDEX_NAME).setSettings(indexSettingsWithVersion(indexVersion))
                        .setMapping(generateMapping(inferenceFieldName, inferenceId, null))
                        .execute()
                )
            );

            final Map<String, Object> expectedFieldMapping = generateExpectedFieldMapping(
                inferenceFieldName,
                inferenceId,
                indexVersion.onOrAfter(SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16)
                    ? new ExtendedDenseVectorIndexOptions(
                        SemanticTextFieldMapper.defaultBbqHnswDenseVectorIndexOptions(),
                        DenseVectorFieldMapper.ElementType.BFLOAT16
                    )
                    : SemanticTextFieldMapper.defaultBbqHnswDenseVectorIndexOptions()
            );

            Map<String, Object> actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
            assertThat("indexVersion = " + indexVersion, actualFieldMappings, equalTo(expectedFieldMapping));

            assertAcked(
                safeGet(
                    client().admin()
                        .indices()
                        .prepareDelete(INDEX_NAME)
                        .setIndicesOptions(
                            IndicesOptions.builder().concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true)).build()
                        )
                        .execute()
                )
            );
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class SemanticTextIndexOptionsIT extends SemanticFieldIndexOptionsTestCase {
    private static final Map<String, Object> SPARSE_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");

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

    /**
     * Default sparse-vector {@code index_options} can only be resolved once the inference endpoint exists.
     * Before the endpoint is created, {@code index_options} serializes as an explicit {@code null} under
     * {@code include_defaults}. Once a sparse-embedding endpoint is created, {@code include_defaults=true}
     * resolves the default pruning index options, while {@code include_defaults=false} omits
     * {@code index_options} entirely.
     */
    public void testSparseVectorIndexOptionsDefaults() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";

        // Create the index before the inference endpoint exists. Default index options cannot be determined yet.
        assertAcked(safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, null)).execute()));
        Map<String, Object> actualFieldMappings = getFieldMappings(inferenceFieldName, true);

        Map<String, Object> inferenceFieldMappings = XContentMapValues.nodeMapValue(
            actualFieldMappings.get(inferenceFieldName),
            inferenceFieldName
        );
        assertThat(inferenceFieldMappings.containsKey("index_options"), is(true));
        assertThat(inferenceFieldMappings.get("index_options"), nullValue());

        // Create the inference endpoint
        createInferenceEndpoint(TaskType.SPARSE_EMBEDDING, inferenceId, SPARSE_SERVICE_SETTINGS);

        // We should now be able to get the default sparse vector index options
        final Map<String, Object> expectedFieldMappingWithDefaults = generateExpectedFieldMapping(
            inferenceFieldName,
            inferenceId,
            SparseVectorFieldMapper.SparseVectorIndexOptions.DEFAULT_PRUNING_INDEX_OPTIONS
        );

        actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMappingWithDefaults));

        // If we exclude defaults, index options should not be returned
        final Map<String, Object> expectedFieldMappingWithoutDefaults = generateExpectedFieldMapping(inferenceFieldName, inferenceId, null);

        actualFieldMappings = getFieldMappings(inferenceFieldName, false);
        assertThat(actualFieldMappings, equalTo(expectedFieldMappingWithoutDefaults));
    }

    /**
     * Explicit user-specified sparse-vector {@code index_options} are preserved unchanged and are never
     * replaced by the default pruning options. A non-default {@code sparse_vector} configuration round-trips
     * identically for both {@code include_defaults=false} and {@code include_defaults=true}.
     */
    public void testExplicitSparseVectorIndexOptionsPreserved() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(TaskType.SPARSE_EMBEDDING, inferenceId, SPARSE_SERVICE_SETTINGS);

        // Explicit, non-default sparse vector index options.
        SparseVectorFieldMapper.SparseVectorIndexOptions explicitIndexOptions = new SparseVectorFieldMapper.SparseVectorIndexOptions(
            true,
            new TokenPruningConfig(2.0f, 0.5f, true)
        );
        assertAcked(
            safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, explicitIndexOptions)).execute())
        );

        final Map<String, Object> expectedFieldMapping = generateExpectedFieldMapping(
            inferenceFieldName,
            inferenceId,
            explicitIndexOptions
        );

        // Preserved unchanged when defaults are excluded...
        Map<String, Object> actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, false));
        assertThat(actualFieldMappings, equalTo(expectedFieldMapping));

        // ...and not overwritten by the default pruning options when defaults are included.
        actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMapping));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.license.PutLicenseRequest;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.ExtendedDenseVectorIndexOptions;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_USES_DENSE_VECTOR_DEFAULT_INDEX_OPTIONS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class SemanticTextIndexOptionsIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-index";

    private static final Map<String, Object> BBQ_COMPATIBLE_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    private static final Map<String, Object> BFLOAT16_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key",
        "element_type",
        "bfloat16"
    );

    private final Map<String, TaskType> inferenceIds = new HashMap<>();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class);
    }

    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void resetLicense() throws Exception {
        setLicense(License.LicenseType.TRIAL);
    }

    @After
    public void cleanUp() {
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

        for (var entry : inferenceIds.entrySet()) {
            assertAcked(
                safeGet(
                    client().execute(
                        DeleteInferenceEndpointAction.INSTANCE,
                        new DeleteInferenceEndpointAction.Request(entry.getKey(), entry.getValue(), true, false)
                    )
                )
            );
        }
    }

    public void testValidateIndexOptionsWithBasicLicense() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BBQ_COMPATIBLE_SERVICE_SETTINGS);
        downgradeLicenseAndRestartCluster();

        IndexOptions indexOptions = new DenseVectorFieldMapper.Int8HnswIndexOptions(
            randomIntBetween(1, 100),
            randomIntBetween(1, 10_000),
            randomBoolean(),
            null,
            -1
        );
        assertAcked(
            safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, indexOptions)).execute())
        );

        final Map<String, Object> expectedFieldMapping = generateExpectedFieldMapping(inferenceFieldName, inferenceId, indexOptions);
        assertThat(getFieldMappings(inferenceFieldName, false), equalTo(expectedFieldMapping));
    }

    public void testSetDefaultBBQIndexOptionsWithBasicLicense() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BBQ_COMPATIBLE_SERVICE_SETTINGS);
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

    private Settings indexSettingsWithVersion(IndexVersion version) {
        return Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
    }

    public void testGetDefaultIndexOptionsWithElementTypeOverride() throws Exception {
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
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BBQ_COMPATIBLE_SERVICE_SETTINGS);

        // We should now be able to get the default index options
        final Map<String, Object> expectedFieldMappingWithDefaults = generateExpectedFieldMapping(
            inferenceFieldName,
            inferenceId,
            new ExtendedDenseVectorIndexOptions(null, DenseVectorFieldMapper.ElementType.BFLOAT16)
        );

        actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMappingWithDefaults));

        // If we exclude defaults, index options should not be returned
        final Map<String, Object> expectedFieldMappingWithoutDefaults = generateExpectedFieldMapping(inferenceFieldName, inferenceId, null);

        actualFieldMappings = getFieldMappings(inferenceFieldName, false);
        assertThat(actualFieldMappings, equalTo(expectedFieldMappingWithoutDefaults));
    }

    public void testSerializeDefaultToBfloat16WithExplicitType() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BBQ_COMPATIBLE_SERVICE_SETTINGS);

        DenseVectorFieldMapper.DenseVectorIndexOptions baseIndexOptions = new DenseVectorFieldMapper.Int4HnswIndexOptions(
            20,
            90,
            false,
            null,
            -1
        );
        assertAcked(
            safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, baseIndexOptions)).execute())
        );

        final Map<String, Object> expectedFieldMappingWithoutDefaults = generateExpectedFieldMapping(
            inferenceFieldName,
            inferenceId,
            baseIndexOptions
        );
        final Map<String, Object> expectedFieldMappingWithDefaults = generateExpectedFieldMapping(
            inferenceFieldName,
            inferenceId,
            new ExtendedDenseVectorIndexOptions(baseIndexOptions, DenseVectorFieldMapper.ElementType.BFLOAT16)
        );

        // When include_defaults == false, the BFLOAT16 default should not be serialized
        Map<String, Object> actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, false));
        assertThat(actualFieldMappings, equalTo(expectedFieldMappingWithoutDefaults));

        // When include_defaults == true, the BFLOAT16 default should be serialized
        actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMappingWithDefaults));
    }

    public void testElementTypeExcludedFromDefaultIndexOptionsWhenNoOverride() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BFLOAT16_SERVICE_SETTINGS);
        assertAcked(safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, null)).execute()));

        // If we didn't default to bfloat16, element_type should be excluded from index options even when include_defaults is true
        final Map<String, Object> expectedFieldMapping = generateExpectedFieldMapping(inferenceFieldName, inferenceId, null);

        Map<String, Object> actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMapping));
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    private static XContentBuilder generateMapping(String inferenceFieldName, String inferenceId, @Nullable IndexOptions indexOptions)
        throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        mapping.field("properties");
        generateFieldMapping(mapping, inferenceFieldName, inferenceId, indexOptions);
        mapping.endObject();

        return mapping;
    }

    private static void generateFieldMapping(
        XContentBuilder builder,
        String inferenceFieldName,
        String inferenceId,
        @Nullable IndexOptions indexOptions
    ) throws IOException {
        builder.startObject();
        builder.startObject(inferenceFieldName);
        builder.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        builder.field("inference_id", inferenceId);
        if (indexOptions != null) {
            builder.startObject("index_options");
            if (indexOptions instanceof DenseVectorFieldMapper.DenseVectorIndexOptions
                || indexOptions instanceof ExtendedDenseVectorIndexOptions) {
                builder.field("dense_vector");
                indexOptions.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
    }

    private static Map<String, Object> generateExpectedFieldMapping(
        String inferenceFieldName,
        String inferenceId,
        @Nullable IndexOptions indexOptions
    ) throws IOException {
        Map<String, Object> expectedFieldMapping;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            generateFieldMapping(builder, inferenceFieldName, inferenceId, indexOptions);
            expectedFieldMapping = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }

        return expectedFieldMapping;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> filterNullOrEmptyValues(Map<String, Object> map) {
        Map<String, Object> filteredMap = new HashMap<>();
        for (var entry : map.entrySet()) {
            Object value = entry.getValue();
            if (entry.getValue() instanceof Map<?, ?> mapValue) {
                if (mapValue.isEmpty()) {
                    continue;
                }

                value = filterNullOrEmptyValues((Map<String, Object>) mapValue);
            }

            if (value != null) {
                filteredMap.put(entry.getKey(), value);
            }
        }

        return filteredMap;
    }

    private static Map<String, Object> getFieldMappings(String fieldName, boolean includeDefaults) {
        var request = new GetFieldMappingsRequest().indices(INDEX_NAME).fields(fieldName).includeDefaults(includeDefaults);
        return safeGet(client().execute(GetFieldMappingsAction.INSTANCE, request)).fieldMappings(INDEX_NAME, fieldName).sourceAsMap();
    }

    private static void setLicense(License.LicenseType type) throws Exception {
        if (type == License.LicenseType.BASIC) {
            assertAcked(
                safeGet(
                    client().execute(
                        PostStartBasicAction.INSTANCE,
                        new PostStartBasicRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).acknowledge(true)
                    )
                )
            );
        } else {
            License license = TestUtils.generateSignedLicense(
                type.getTypeName(),
                License.VERSION_CURRENT,
                -1,
                TimeValue.timeValueHours(24)
            );
            assertAcked(
                safeGet(
                    client().execute(
                        PutLicenseAction.INSTANCE,
                        new PutLicenseRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).license(license)
                    )
                )
            );
        }
    }

    private static void assertLicense(License.LicenseType type) {
        var getLicenseResponse = safeGet(client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)));
        assertThat(getLicenseResponse.license().type(), equalTo(type.getTypeName()));
    }

    private void downgradeLicenseAndRestartCluster() throws Exception {
        // Downgrade the license and restart the cluster to force the model registry to rebuild
        setLicense(License.LicenseType.BASIC);
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback());
        ensureGreen(InferenceIndex.INDEX_NAME);
        assertLicense(License.LicenseType.BASIC);
    }
}

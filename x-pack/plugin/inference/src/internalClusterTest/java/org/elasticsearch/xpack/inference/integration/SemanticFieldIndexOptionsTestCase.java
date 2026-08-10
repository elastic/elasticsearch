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
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.ExtendedDenseVectorIndexOptions;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * Base class for integration tests covering {@code index_options} serialization (including
 * {@code include_defaults} and element-type defaulting behavior) for field types
 * ({@code semantic} and {@code semantic_text}). Provides shared cluster setup, license management,
 * inference-endpoint bookkeeping, and mapping/field-mapping helpers; subclasses supply the field
 * type under test and the actual test methods.
 */
@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public abstract class SemanticFieldIndexOptionsTestCase extends ESIntegTestCase {
    protected static final String INDEX_NAME = "test-index";

    protected static final Map<String, Object> FLOAT_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    protected static final Map<String, Object> BFLOAT16_SERVICE_SETTINGS = Map.of(
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

    /**
     * The {@code type} value ({@code semantic} or {@code semantic_text}) used when generating field mappings.
     */
    protected abstract String fieldType();

    /**
     * The {@link TaskType} used when creating the dense-embedding inference endpoint used by shared tests.
     */
    protected abstract TaskType taskType();

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

    protected void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    protected Settings indexSettingsWithVersion(IndexVersion version) {
        return Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
    }

    protected XContentBuilder generateMapping(String inferenceFieldName, String inferenceId, @Nullable IndexOptions indexOptions)
        throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        mapping.field("properties");
        generateFieldMapping(mapping, inferenceFieldName, inferenceId, indexOptions);
        mapping.endObject();

        return mapping;
    }

    protected void generateFieldMapping(
        XContentBuilder builder,
        String inferenceFieldName,
        String inferenceId,
        @Nullable IndexOptions indexOptions
    ) throws IOException {
        builder.startObject();
        builder.startObject(inferenceFieldName);
        builder.field("type", fieldType());
        builder.field("inference_id", inferenceId);
        if (indexOptions != null) {
            builder.startObject("index_options");
            if (indexOptions instanceof DenseVectorFieldMapper.DenseVectorIndexOptions
                || indexOptions instanceof ExtendedDenseVectorIndexOptions) {
                builder.field("dense_vector");
                indexOptions.toXContent(builder, ToXContent.EMPTY_PARAMS);
            } else if (indexOptions instanceof SparseVectorFieldMapper.SparseVectorIndexOptions) {
                builder.field("sparse_vector");
                indexOptions.toXContent(builder, ToXContent.EMPTY_PARAMS);
            } else {
                throw new IllegalArgumentException("Unsupported index options type: " + indexOptions.getClass());
            }
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
    }

    protected Map<String, Object> generateExpectedFieldMapping(
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
    protected static Map<String, Object> filterNullOrEmptyValues(Map<String, Object> map) {
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

    protected static Map<String, Object> getFieldMappings(String fieldName, boolean includeDefaults) {
        var request = new GetFieldMappingsRequest().indices(INDEX_NAME).fields(fieldName).includeDefaults(includeDefaults);
        return safeGet(client().execute(GetFieldMappingsAction.INSTANCE, request)).fieldMappings(INDEX_NAME, fieldName).sourceAsMap();
    }

    protected static void setLicense(License.LicenseType type) throws Exception {
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

    protected static void assertLicense(License.LicenseType type) {
        var getLicenseResponse = safeGet(client().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)));
        assertThat(getLicenseResponse.license().type(), equalTo(type.getTypeName()));
    }

    protected void downgradeLicenseAndRestartCluster() throws Exception {
        // Downgrade the license and restart the cluster to force the model registry to rebuild
        setLicense(License.LicenseType.BASIC);
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback());
        ensureGreen(InferenceIndex.INDEX_NAME);
        assertLicense(License.LicenseType.BASIC);
    }

    /**
     * User-specified {@code index_options} are accepted and preserved even under a basic license. After
     * downgrading to a basic license (which rebuilds the model registry), an index created with an
     * explicit {@code int8_hnsw} dense-vector index type round-trips those options unchanged when
     * serialized with {@code include_defaults=false}.
     */
    public void testValidateIndexOptionsWithBasicLicense() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(taskType(), inferenceId, FLOAT_SERVICE_SETTINGS);
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

    /**
     * Default {@code index_options} can only be resolved once the inference endpoint exists. Before the
     * endpoint is created, {@code index_options} serializes as an explicit {@code null} under
     * {@code include_defaults}. Once a float-model endpoint is created, {@code include_defaults=true}
     * resolves the defaults and applies the {@code bfloat16} element-type default, while
     * {@code include_defaults=false} omits {@code index_options} entirely.
     */
    public void testGetDefaultIndexOptionsAppliesBfloat16ElementType() throws Exception {
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
        createInferenceEndpoint(taskType(), inferenceId, FLOAT_SERVICE_SETTINGS);

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

    /**
     * The {@code bfloat16} element-type default is layered on top of a user-specified vector index type.
     * With a float model and an explicit {@code int4_hnsw} index type (but no explicit element type),
     * {@code include_defaults=false} serializes only the user's index options, and
     * {@code include_defaults=true} additionally serializes the {@code bfloat16} element-type default.
     */
    public void testSerializeDefaultToBfloat16WithExplicitVectorIndexType() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(taskType(), inferenceId, FLOAT_SERVICE_SETTINGS);

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

    /**
     * No element-type default is added when the model already produces {@code bfloat16}. With no
     * user-specified index options, {@code element_type} is excluded from the serialized default
     * {@code index_options} even under {@code include_defaults=true}, because there is no
     * float-to-bfloat16 defaulting to apply.
     */
    public void testElementTypeNotDefaultedToBfloat16WhenModelIsBfloat16() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        createInferenceEndpoint(taskType(), inferenceId, BFLOAT16_SERVICE_SETTINGS);
        assertAcked(safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, null)).execute()));

        // If we didn't default to bfloat16, element_type should be excluded from index options even when include_defaults is true
        final Map<String, Object> expectedFieldMapping = generateExpectedFieldMapping(inferenceFieldName, inferenceId, null);

        Map<String, Object> actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMapping));
    }

    /**
     * An explicit user element type is never overwritten by the default. With a float model that
     * would otherwise default to {@code bfloat16}, a user-specified {@code element_type=float} is
     * preserved unchanged for both {@code include_defaults=false} and {@code include_defaults=true}.
     */
    public void testExplicitElementTypePreservedOverBfloat16Default() throws Exception {
        final String inferenceId = randomIdentifier();
        final String inferenceFieldName = "inference_field";
        // FLOAT model settings would normally cause element_type to default to BFLOAT16
        createInferenceEndpoint(taskType(), inferenceId, FLOAT_SERVICE_SETTINGS);

        // Explicitly request FLOAT, opting out of the BFLOAT16 default
        ExtendedDenseVectorIndexOptions explicitIndexOptions = new ExtendedDenseVectorIndexOptions(
            null,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        assertAcked(
            safeGet(prepareCreate(INDEX_NAME).setMapping(generateMapping(inferenceFieldName, inferenceId, explicitIndexOptions)).execute())
        );

        final Map<String, Object> expectedFieldMapping = generateExpectedFieldMapping(
            inferenceFieldName,
            inferenceId,
            explicitIndexOptions
        );

        Map<String, Object> actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, false));
        assertThat(actualFieldMappings, equalTo(expectedFieldMapping));

        // The explicit FLOAT element type must survive include_defaults == true unchanged, not replaced by the BFLOAT16 default
        actualFieldMappings = filterNullOrEmptyValues(getFieldMappings(inferenceFieldName, true));
        assertThat(actualFieldMappings, equalTo(expectedFieldMapping));
    }
}

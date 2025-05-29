/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemasTests;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.inference.ModelConfigurations.USE_ID_FOR_INDEX;
import static org.hamcrest.Matchers.equalTo;

public class SageMakerModelBuilderTests extends ESTestCase {
    private static final String inferenceId = "inferenceId";
    private static final TaskType taskType = TaskType.ANY;
    private static final String service = "service";
    private SageMakerModelBuilder builder;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        builder = new SageMakerModelBuilder(SageMakerSchemasTests.mockSchemas());
    }

    public void testFromRequestWithRequiredFields() {
        var model = fromRequest("""
            {
                "service_settings": {
                    "access_key": "test-access-key",
                    "secret_key": "test-secret-key",
                    "region": "us-east-1",
                    "api": "test-api",
                    "endpoint_name": "test-endpoint"
                }
            }
            """);

        assertNotNull(model);
        assertTrue(model.awsSecretSettings().isPresent());
        assertThat(model.awsSecretSettings().get().accessKey().toString(), equalTo("test-access-key"));
        assertThat(model.awsSecretSettings().get().secretKey().toString(), equalTo("test-secret-key"));
        assertThat(model.region(), equalTo("us-east-1"));
        assertThat(model.api(), equalTo("test-api"));
        assertThat(model.endpointName(), equalTo("test-endpoint"));

        assertTrue(model.customAttributes().isEmpty());
        assertTrue(model.enableExplanations().isEmpty());
        assertTrue(model.inferenceComponentName().isEmpty());
        assertTrue(model.inferenceIdForDataCapture().isEmpty());
        assertTrue(model.sessionId().isEmpty());
        assertTrue(model.targetContainerHostname().isEmpty());
        assertTrue(model.targetModel().isEmpty());
        assertTrue(model.targetVariant().isEmpty());
        assertTrue(model.batchSize().isEmpty());
    }

    public void testFromRequestWithOptionalFields() {
        var model = fromRequest("""
            {
                "service_settings": {
                    "access_key": "test-access-key",
                    "secret_key": "test-secret-key",
                    "region": "us-east-1",
                    "api": "test-api",
                    "endpoint_name": "test-endpoint",
                    "target_model": "test-target",
                    "target_container_hostname": "test-target-container",
                    "inference_component_name": "test-inference-component",
                    "batch_size": 1234
                },
                "task_settings": {
                    "custom_attributes": "test-custom-attributes",
                    "enable_explanations": "test-enable-explanations",
                    "inference_id": "test-inference-id",
                    "session_id": "test-session-id",
                    "target_variant": "test-target-variant"
                }
            }
            """);

        assertNotNull(model);
        assertTrue(model.awsSecretSettings().isPresent());
        assertThat(model.awsSecretSettings().get().accessKey().toString(), equalTo("test-access-key"));
        assertThat(model.awsSecretSettings().get().secretKey().toString(), equalTo("test-secret-key"));
        assertThat(model.region(), equalTo("us-east-1"));
        assertThat(model.api(), equalTo("test-api"));
        assertThat(model.endpointName(), equalTo("test-endpoint"));

        assertPresent(model.customAttributes(), "test-custom-attributes");
        assertPresent(model.enableExplanations(), "test-enable-explanations");
        assertPresent(model.inferenceComponentName(), "test-inference-component");
        assertPresent(model.inferenceIdForDataCapture(), "test-inference-id");
        assertPresent(model.sessionId(), "test-session-id");
        assertPresent(model.targetContainerHostname(), "test-target-container");
        assertPresent(model.targetModel(), "test-target");
        assertPresent(model.targetVariant(), "test-target-variant");
        assertPresent(model.batchSize(), 1234);
    }

    public void testFromRequestWithoutAccessKey() {
        testExceptionFromRequest("""
            {
                "service_settings": {
                    "secret_key": "test-secret-key",
                    "region": "us-east-1",
                    "api": "test-api",
                    "endpoint_name": "test-endpoint"
                }
            }
            """, ValidationException.class, "Validation Failed: 1: [secret_settings] does not contain the required setting [access_key];");
    }

    public void testFromRequestWithoutSecretKey() {
        testExceptionFromRequest("""
            {
                "service_settings": {
                    "access_key": "test-access-key",
                    "region": "us-east-1",
                    "api": "test-api",
                    "endpoint_name": "test-endpoint"
                }
            }
            """, ValidationException.class, "Validation Failed: 1: [secret_settings] does not contain the required setting [secret_key];");
    }

    public void testFromRequestWithoutRegion() {
        testExceptionFromRequest("""
            {
                "service_settings": {
                    "access_key": "test-access-key",
                    "secret_key": "test-secret-key",
                    "api": "test-api",
                    "endpoint_name": "test-endpoint"
                }
            }
            """, ValidationException.class, "Validation Failed: 1: [service_settings] does not contain the required setting [region];");
    }

    public void testFromRequestWithoutApi() {
        testExceptionFromRequest("""
            {
                "service_settings": {
                    "access_key": "test-access-key",
                    "secret_key": "test-secret-key",
                    "region": "us-east-1",
                    "endpoint_name": "test-endpoint"
                }
            }
            """, ValidationException.class, "Validation Failed: 1: [service_settings] does not contain the required setting [api];");
    }

    public void testFromRequestWithoutEndpointName() {
        testExceptionFromRequest(
            """
                {
                    "service_settings": {
                        "access_key": "test-access-key",
                        "secret_key": "test-secret-key",
                        "region": "us-east-1",
                        "api": "test-api"
                    }
                }
                """,
            ValidationException.class,
            "Validation Failed: 1: [service_settings] does not contain the required setting [endpoint_name];"
        );
    }

    public void testFromRequestWithExtraServiceKeys() {
        testExceptionFromRequest(
            """
                {
                    "service_settings": {
                        "access_key": "test-access-key",
                        "secret_key": "test-secret-key",
                        "region": "us-east-1",
                        "api": "test-api",
                        "endpoint_name": "test-endpoint",
                        "hello": "there"
                    }
                }
                """,
            ElasticsearchStatusException.class,
            "Configuration contains settings [{hello=there}] unknown to the [service] service"
        );
    }

    public void testFromRequestWithExtraTaskKeys() {
        testExceptionFromRequest(
            """
                {
                    "service_settings": {
                        "access_key": "test-access-key",
                        "secret_key": "test-secret-key",
                        "region": "us-east-1",
                        "api": "test-api",
                        "endpoint_name": "test-endpoint"
                    },
                    "task_settings": {
                        "hello": "there"
                    }
                }
                """,
            ElasticsearchStatusException.class,
            "Configuration contains settings [{hello=there}] unknown to the [service] service"
        );
    }

    public void testRoundTrip() throws IOException {
        var expectedModel = fromRequest("""
            {
                "service_settings": {
                    "access_key": "test-access-key",
                    "secret_key": "test-secret-key",
                    "region": "us-east-1",
                    "api": "test-api",
                    "endpoint_name": "test-endpoint",
                    "target_model": "test-target",
                    "target_container_hostname": "test-target-container",
                    "inference_component_name": "test-inference-component",
                    "batch_size": 1234
                },
                "task_settings": {
                    "custom_attributes": "test-custom-attributes",
                    "enable_explanations": "test-enable-explanations",
                    "inference_id": "test-inference-id",
                    "session_id": "test-session-id",
                    "target_variant": "test-target-variant"
                }
            }
            """);

        var unparsedModelWithSecrets = unparsedModel(expectedModel.getConfigurations(), expectedModel.getSecrets());
        var modelWithSecrets = builder.fromStorage(
            unparsedModelWithSecrets.inferenceEntityId(),
            unparsedModelWithSecrets.taskType(),
            unparsedModelWithSecrets.service(),
            unparsedModelWithSecrets.settings(),
            unparsedModelWithSecrets.secrets()
        );
        assertThat(modelWithSecrets, equalTo(expectedModel));
        assertNotNull(modelWithSecrets.getSecrets().getSecretSettings());

        var unparsedModelWithoutSecrets = unparsedModel(expectedModel.getConfigurations(), null);
        var modelWithoutSecrets = builder.fromStorage(
            unparsedModelWithoutSecrets.inferenceEntityId(),
            unparsedModelWithoutSecrets.taskType(),
            unparsedModelWithoutSecrets.service(),
            unparsedModelWithoutSecrets.settings(),
            unparsedModelWithoutSecrets.secrets()
        );
        assertThat(modelWithoutSecrets.getConfigurations(), equalTo(expectedModel.getConfigurations()));
        assertNull(modelWithoutSecrets.getSecrets().getSecretSettings());
    }

    private SageMakerModel fromRequest(String json) {
        return builder.fromRequest(inferenceId, taskType, service, map(json));
    }

    private void testExceptionFromRequest(String json, Class<? extends Exception> exceptionClass, String message) {
        var exception = assertThrows(exceptionClass, () -> fromRequest(json));
        assertThat(exception.getMessage(), equalTo(message));
    }

    private static <T> void assertPresent(Optional<T> optional, T expectedValue) {
        assertTrue(optional.isPresent());
        assertThat(optional.get(), equalTo(expectedValue));
    }

    private static Map<String, Object> map(String json) {
        try (
            var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.getBytes(StandardCharsets.UTF_8))
        ) {
            return parser.map();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static UnparsedModel unparsedModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) throws IOException {
        var modelConfigMap = new ModelRegistry.ModelConfigMap(
            toJsonMap(modelConfigurations),
            modelSecrets != null ? toJsonMap(modelSecrets) : null
        );

        return ModelRegistry.unparsedModelFromMap(modelConfigMap);
    }

    private static Map<String, Object> toJsonMap(ToXContent toXContent) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            toXContent.toXContent(builder, new ToXContent.MapParams(Map.of(USE_ID_FOR_INDEX, "true")));
            return map(Strings.toString(builder));
        }
    }
}

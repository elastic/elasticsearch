/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.EndpointMetadataTests;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettingsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension.TestServiceSettings.HIDDEN_FIELD_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ModelConfigurationsTests extends AbstractBWCWireSerializationTestCase<ModelConfigurations> {

    public static ModelConfigurations createRandomInstance() {
        var taskType = randomFrom(TaskType.values());
        var endpointMetadata = randomBoolean() ? null : EndpointMetadataTests.randomInstance();
        return new ModelConfigurations(
            randomAlphaOfLength(6),
            taskType,
            randomAlphaOfLength(6),
            randomServiceSettings(),
            randomTaskSettings(),
            randomBoolean() ? ChunkingSettingsTests.createRandomChunkingSettings() : null,
            endpointMetadata
        );
    }

    public static ModelConfigurations mutateTestInstance(ModelConfigurations instance) {
        return switch (randomIntBetween(0, 6)) {
            case 0 -> new ModelConfigurations(
                instance.getInferenceEntityId() + "foo",
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings(),
                instance.getEndpointMetadata()
            );
            case 1 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length],
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings(),
                instance.getEndpointMetadata()
            );
            case 2 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService() + "bar",
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings(),
                instance.getEndpointMetadata()
            );
            case 3 -> {
                var endpointMetadata = instance.getEndpointMetadata();
                if (endpointMetadata.equals(EndpointMetadata.EMPTY_INSTANCE)) {
                    endpointMetadata = EndpointMetadataTests.randomNonEmptyInstance();
                } else {
                    endpointMetadata = EndpointMetadata.EMPTY_INSTANCE;
                }
                yield new ModelConfigurations(
                    instance.getInferenceEntityId(),
                    instance.getTaskType(),
                    instance.getService(),
                    instance.getServiceSettings(),
                    instance.getTaskSettings(),
                    instance.getChunkingSettings(),
                    endpointMetadata
                );
            }
            case 4 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService(),
                randomValueOtherThan(instance.getServiceSettings(), ModelConfigurationsTests::randomServiceSettings),
                instance.getTaskSettings(),
                instance.getChunkingSettings(),
                instance.getEndpointMetadata()
            );
            case 5 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                randomValueOtherThan(instance.getTaskSettings(), ModelConfigurationsTests::randomTaskSettings),
                instance.getChunkingSettings(),
                instance.getEndpointMetadata()
            );
            case 6 -> {
                var chunkingSettings = instance.getChunkingSettings();
                // Mutate chunkingSettings: if null, create a random one; if non-null, toggle to null or create a different one
                if (chunkingSettings == null) {
                    chunkingSettings = ChunkingSettingsTests.createRandomChunkingSettings();
                } else {
                    chunkingSettings = randomBoolean()
                        ? null
                        : randomValueOtherThan(chunkingSettings, ChunkingSettingsTests::createRandomChunkingSettings);
                }
                yield new ModelConfigurations(
                    instance.getInferenceEntityId(),
                    instance.getTaskType(),
                    instance.getService(),
                    instance.getServiceSettings(),
                    instance.getTaskSettings(),
                    chunkingSettings,
                    instance.getEndpointMetadata()
                );
            }
            default -> throw new IllegalStateException();
        };
    }

    private static ServiceSettings randomServiceSettings() {
        return ElserInternalServiceSettingsTests.createRandom();
    }

    private static TaskSettings randomTaskSettings() {
        return JinaAIRerankTaskSettingsTests.createRandom();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(InferenceNamedWriteablesProvider.getNamedWriteables());
        namedWriteables.addAll(XPackClientPlugin.getChunkingSettingsNamedWriteables());

        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected Writeable.Reader<ModelConfigurations> instanceReader() {
        return ModelConfigurations::new;
    }

    @Override
    protected ModelConfigurations createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected ModelConfigurations mutateInstance(ModelConfigurations instance) {
        return mutateTestInstance(instance);
    }

    @Override
    protected ModelConfigurations mutateInstanceForVersion(ModelConfigurations instance, TransportVersion version) {
        if (version.supports(EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED)) {
            return instance;
        } else {
            return new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings()
            );
        }
    }

    public void testToXContentDoesNotIncludeEmptyEndpointMetadata() throws IOException {
        ModelConfigurations modelConfigurations = new ModelConfigurations(
            "test_entity_id",
            TaskType.SPARSE_EMBEDDING,
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            ElserInternalServiceSettingsTests.createRandom(),
            ElserMlNodeTaskSettings.DEFAULT,
            null,
            EndpointMetadata.EMPTY_INSTANCE
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        modelConfigurations.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, not(containsString(EndpointMetadata.METADATA_FIELD_NAME)));
    }

    public void testToXContentIncludesNonEmptyEndpointMetadata() throws IOException {
        ModelConfigurations modelConfigurations = new ModelConfigurations(
            "test_entity_id",
            TaskType.SPARSE_EMBEDDING,
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            new TestSparseInferenceServiceExtension.TestServiceSettings("model", "hidden_value", false),
            EmptyTaskSettings.INSTANCE,
            null,
            new EndpointMetadata(
                new EndpointMetadata.Heuristics(List.of("heuristic1", "heuristic2"), StatusHeuristic.BETA, "2025-01-01", "2025-12-31"),
                new EndpointMetadata.Internal("fingerprint", 1L),
                new EndpointMetadata.Display("name")
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        modelConfigurations.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace("""
            {
              "inference_id": "test_entity_id",
              "task_type": "sparse_embedding",
              "service": "test_service",
              "service_settings": {
                "model": "model",
                "hidden_field": "hidden_value",
                "should_return_hidden_field": false
              },
              "metadata": {
                "heuristics": {
                  "properties": ["heuristic1", "heuristic2"],
                  "status": "beta",
                  "release_date": "2025-01-01",
                  "end_of_life_date": "2025-12-31"
                },
                "internal": {
                  "fingerprint": "fingerprint",
                  "version": 1
                },
                "display": {
                  "name": "name"
                }
              }
            }
            """)));
    }

    public void testToXContentIncludesHiddenFieldsInServiceSettings() throws IOException {
        String hiddenValue = "secret_value";
        ServiceSettings serviceSettings = new TestSparseInferenceServiceExtension.TestServiceSettings("test_model", hiddenValue, false);
        ModelConfigurations modelConfigurations = new ModelConfigurations(
            "test_entity_id",
            TaskType.SPARSE_EMBEDDING,
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            serviceSettings,
            ElserMlNodeTaskSettings.DEFAULT,
            null,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        modelConfigurations.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, containsString(ModelConfigurations.SERVICE_SETTINGS));
        assertThat(json, containsString(HIDDEN_FIELD_KEY));
        assertThat(json, containsString(hiddenValue));
    }

    public void testToFilteredXContentExcludesHiddenFieldsFromServiceSettings() throws IOException {
        String hiddenValue = "secret_value";
        ServiceSettings serviceSettings = new TestSparseInferenceServiceExtension.TestServiceSettings("test_model", hiddenValue, false);
        ModelConfigurations modelConfigurations = new ModelConfigurations(
            "test_entity_id",
            TaskType.SPARSE_EMBEDDING,
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            serviceSettings,
            ElserMlNodeTaskSettings.DEFAULT,
            null,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        modelConfigurations.toFilteredXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, containsString(ModelConfigurations.SERVICE_SETTINGS));
        assertThat(json, containsString("test_model"));
        assertThat(
            "Hidden field must not appear in filtered output when shouldReturnHiddenField is false",
            json,
            not(containsString(HIDDEN_FIELD_KEY))
        );
        assertThat("Hidden value must not appear in filtered output", json, not(containsString(hiddenValue)));
    }

    public void testToFilteredXContentIncludesHiddenFieldWhenAllowedByServiceSettings() throws IOException {
        String hiddenValue = "allowed_secret";
        ServiceSettings serviceSettings = new TestSparseInferenceServiceExtension.TestServiceSettings("test_model", hiddenValue, true);
        ModelConfigurations modelConfigurations = new ModelConfigurations(
            "test_entity_id",
            TaskType.SPARSE_EMBEDDING,
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            serviceSettings,
            ElserMlNodeTaskSettings.DEFAULT,
            null,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        modelConfigurations.toFilteredXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, containsString(ModelConfigurations.SERVICE_SETTINGS));
        assertThat(json, containsString(HIDDEN_FIELD_KEY));
        assertThat(json, containsString(hiddenValue));
    }
}

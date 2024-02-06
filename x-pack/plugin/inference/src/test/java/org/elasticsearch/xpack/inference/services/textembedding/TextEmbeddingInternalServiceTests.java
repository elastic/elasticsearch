/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference.services.textembedding;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class TextEmbeddingInternalServiceTests extends ESTestCase {

    TaskType taskType = TaskType.TEXT_EMBEDDING;
    String randomInferenceEntityId = randomAlphaOfLength(10);

    public void testParseRequestConfig() {

        // Null model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS, 1, TextEmbeddingInternalServiceSettings.NUM_THREADS, 4)
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(IllegalArgumentException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }

        // Valid model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        InternalServiceSettings.MODEL_ID,
                        TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(
                1,
                4,
                TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID
            );

            service.parseRequestConfig(
                randomInferenceEntityId,
                taskType,
                settings,
                Set.of(),
                getModelVerificationActionListener(e5ServiceSettings)
            );
        }

        // Invalid config map
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS, 1, TextEmbeddingInternalServiceSettings.NUM_THREADS, 4)
                )
            );
            settings.put("not_a_valid_config_setting", randomAlphaOfLength(10));

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(IllegalArgumentException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }

        // Invalid service settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        InternalServiceSettings.MODEL_ID,
                        TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID, // we can't directly test the eland case until we mock
                                                                                     // the threadpool within the client
                        "not_a_valid_service_setting",
                        randomAlphaOfLength(10)
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }

        // Extra service settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        InternalServiceSettings.MODEL_ID,
                        TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID, // we can't directly test the eland case until we mock
                                                                                     // the threadpool within the client
                        "extra_setting_that_should_not_be_here",
                        randomAlphaOfLength(10)
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }

        // Extra settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        InternalServiceSettings.MODEL_ID,
                        TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID // we can't directly test the eland case until we mock
                        // the threadpool within the client
                    )
                )
            );
            settings.put("extra_setting_that_should_not_be_here", randomAlphaOfLength(10));

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }
    }

    private ActionListener<Model> getModelVerificationActionListener(MultilingualE5SmallInternalServiceSettings e5ServiceSettings) {
        return ActionListener.<Model>wrap(model -> {
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, TextEmbeddingInternalService.NAME, e5ServiceSettings),
                model
            );
        }, e -> { fail("Model parsing failed " + e.getMessage()); });
    }

    public void testParsePersistedConfig() {

        // Null model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS, 1, TextEmbeddingInternalServiceSettings.NUM_THREADS, 4)
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(
                1,
                4,
                TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID
            );

            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));

        }

        // Invalid model variant
        // because this is a persisted config, we assume that the model does exist, even though it doesn't. In practice, the trained models
        // API would throw an exception when the model is used
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        InternalServiceSettings.MODEL_ID,
                        "invalid"
                    )
                )
            );

            CustomElandModel parsedModel = (CustomElandModel) service.parsePersistedConfig(randomInferenceEntityId, taskType, settings);
            var elandServiceSettings = new CustomElandInternalServiceSettings(1, 4, "invalid");
            assertEquals(
                new CustomElandModel(randomInferenceEntityId, taskType, TextEmbeddingInternalService.NAME, elandServiceSettings),
                parsedModel
            );
        }

        // Valid model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        InternalServiceSettings.MODEL_ID,
                        TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(
                1,
                4,
                TextEmbeddingInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID
            );

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parsePersistedConfig(
                randomInferenceEntityId,
                taskType,
                settings
            );
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, TextEmbeddingInternalService.NAME, e5ServiceSettings),
                parsedModel
            );
        }

        // Invalid config map
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS, 1, TextEmbeddingInternalServiceSettings.NUM_THREADS, 4)
                )
            );
            settings.put("not_a_valid_config_setting", randomAlphaOfLength(10));
            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));
        }

        // Invalid service settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingInternalServiceSettings.NUM_THREADS,
                        4,
                        "not_a_valid_service_setting",
                        randomAlphaOfLength(10)
                    )
                )
            );
            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));
        }
    }

    private TextEmbeddingInternalService createService(Client client) {
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(client);
        return new TextEmbeddingInternalService(context);
    }

    public static Model randomModelConfig(String inferenceEntityId) {
        List<String> givenList = Arrays.asList("MultilingualE5SmallModel");
        Random rand = org.elasticsearch.common.Randomness.get();
        String model = givenList.get(rand.nextInt(givenList.size()));

        return switch (model) {
            case "MultilingualE5SmallModel" -> new MultilingualE5SmallModel(
                inferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                TextEmbeddingInternalService.NAME,
                MultilingualE5SmallInternalServiceSettingsTests.createRandom()
            );
            default -> throw new IllegalArgumentException("model " + model + " is not supported for testing");
        };
    }

}

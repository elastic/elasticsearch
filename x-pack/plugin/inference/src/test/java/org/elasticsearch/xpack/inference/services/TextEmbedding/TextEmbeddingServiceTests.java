/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference.services.TextEmbedding;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.MlNodeDeployedServiceSettings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class TextEmbeddingServiceTests extends ESTestCase {

    TaskType taskType = TaskType.TEXT_EMBEDDING;
    String randomInferenceEntityId = randomAlphaOfLength(10);

    public void testParseRequestConfig() {

        // Null model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(Map.of(TextEmbeddingServiceSettings.NUM_ALLOCATIONS, 1, TextEmbeddingServiceSettings.NUM_THREADS, 4))
            );

            var e5ServiceSettings = new MultilingualE5SmallServiceSettings(1, 4, TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID);

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parseRequestConfig(
                randomInferenceEntityId,
                taskType,
                settings,
                Set.of()
            );
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, TextEmbeddingService.NAME, e5ServiceSettings),
                parsedModel
            );
        }

        // Invalid model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingServiceSettings.NUM_THREADS,
                        4,
                        MlNodeDeployedServiceSettings.MODEL_VERSION,
                        "invalid"
                    )
                )
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of())
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
                        TextEmbeddingServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingServiceSettings.NUM_THREADS,
                        4,
                        MlNodeDeployedServiceSettings.MODEL_VERSION,
                        TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallServiceSettings(1, 4, TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID);

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parseRequestConfig(
                randomInferenceEntityId,
                taskType,
                settings,
                Set.of()
            );
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, TextEmbeddingService.NAME, e5ServiceSettings),
                parsedModel
            );
        }
    }

    public void testParsePersistedConfig() {

        // Null model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(Map.of(TextEmbeddingServiceSettings.NUM_ALLOCATIONS, 1, TextEmbeddingServiceSettings.NUM_THREADS, 4))
            );

            var e5ServiceSettings = new MultilingualE5SmallServiceSettings(1, 4, TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID);

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parseRequestConfig(
                randomInferenceEntityId,
                taskType,
                settings,
                Set.of()
            );
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, TextEmbeddingService.NAME, e5ServiceSettings),
                parsedModel
            );
        }

        // Invalid model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingServiceSettings.NUM_THREADS,
                        4,
                        MlNodeDeployedServiceSettings.MODEL_VERSION,
                        "invalid"
                    )
                )
            );
            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));
        }

        // Valid model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        TextEmbeddingServiceSettings.NUM_ALLOCATIONS,
                        1,
                        TextEmbeddingServiceSettings.NUM_THREADS,
                        4,
                        MlNodeDeployedServiceSettings.MODEL_VERSION,
                        TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallServiceSettings(1, 4, TextEmbeddingService.MULTILINGUAL_E5_SMALL_MODEL_ID);

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parsePersistedConfig(
                randomInferenceEntityId,
                taskType,
                settings
            );
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, TextEmbeddingService.NAME, e5ServiceSettings),
                parsedModel
            );
        }
    }

    private TextEmbeddingService createService(Client client) {
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(client);
        return new TextEmbeddingService(context);
    }

    public static Model randomModelConfig(String inferenceEntityId) {
        List<String> givenList = Arrays.asList("MultilingualE5SmallModel");
        Random rand = org.elasticsearch.common.Randomness.get();
        String model = givenList.get(rand.nextInt(givenList.size()));

        return switch (model) {
            case "MultilingualE5SmallModel" -> new MultilingualE5SmallModel(
                inferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                TextEmbeddingService.NAME,
                MultilingualE5SmallServiceSettingsTests.createRandom()
            );
            default -> throw new IllegalArgumentException("model " + model + " is not supported for testing");
        };
    }

}

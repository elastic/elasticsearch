/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class ElserMlNodeServiceTests extends ESTestCase {

    public static Model randomModelConfig(String inferenceEntityId, TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new ElserMlNodeModel(
                inferenceEntityId,
                taskType,
                ElserMlNodeService.NAME,
                ElserMlNodeServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom()
            );
            default -> throw new IllegalArgumentException("task type " + taskType + " is not supported");
        };
    }

    public void testParseConfigStrict() {
        var service = createService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(
                Map.of(
                    ElserMlNodeServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElserMlNodeServiceSettings.NUM_THREADS,
                    4,
                    "model_version",
                    ".elser_model_1"
                )
            )
        );
        settings.put(ModelConfigurations.TASK_SETTINGS, Map.of());

        var expectedModel = new ElserMlNodeModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElserMlNodeService.NAME,
            new ElserMlNodeServiceSettings(1, 4, ".elser_model_1"),
            ElserMlNodeTaskSettings.DEFAULT
        );

        var modelVerificationListener = getModelVerificationListener(expectedModel);

        service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), modelVerificationListener);

    }

    private static ActionListener<Model> getModelVerificationListener(ElserMlNodeModel expectedModel) {
        return ActionListener.<Model>wrap(
            (model) -> { assertEquals(expectedModel, model); },
            (e) -> fail("Model verification should not fail " + e.getMessage())
        );
    }

    public void testParseConfigStrictWithNoTaskSettings() {
        var service = createService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
        );

        var expectedModel = new ElserMlNodeModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElserMlNodeService.NAME,
            new ElserMlNodeServiceSettings(1, 4, ElserMlNodeService.ELSER_V2_MODEL),
            ElserMlNodeTaskSettings.DEFAULT
        );

        var modelVerificationListener = getModelVerificationListener(expectedModel);

        service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), modelVerificationListener);

    }

    public void testParseConfigStrictWithUnknownSettings() {

        var service = createService(mock(Client.class));

        for (boolean throwOnUnknown : new boolean[] { true, false }) {
            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    ModelConfigurations.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(
                            ElserMlNodeServiceSettings.NUM_ALLOCATIONS,
                            1,
                            ElserMlNodeServiceSettings.NUM_THREADS,
                            4,
                            ElserMlNodeServiceSettings.MODEL_VERSION,
                            ".elser_model_2"
                        )
                    )
                );
                settings.put(ModelConfigurations.TASK_SETTINGS, Map.of());
                settings.put("foo", "bar");

                ActionListener<Model> errorVerificationListener = ActionListener.wrap((model) -> {
                    if (throwOnUnknown) {
                        fail("Model verification should fail when throwOnUnknown is true");
                    }
                }, (e) -> {
                    if (throwOnUnknown) {
                        assertThat(
                            e.getMessage(),
                            containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser] service")
                        );
                    } else {
                        fail("Model verification should not fail when throwOnUnknown is false");
                    }
                });

                if (throwOnUnknown == false) {
                    var parsed = service.parsePersistedConfigWithSecrets(
                        "foo",
                        TaskType.SPARSE_EMBEDDING,
                        settings,
                        Collections.emptyMap()
                    );
                } else {

                    service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), errorVerificationListener);
                }
            }

            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    ModelConfigurations.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(
                            ElserMlNodeServiceSettings.NUM_ALLOCATIONS,
                            1,
                            ElserMlNodeServiceSettings.NUM_THREADS,
                            4,
                            ElserMlNodeServiceSettings.MODEL_VERSION,
                            ".elser_model_2"
                        )
                    )
                );
                settings.put(ModelConfigurations.TASK_SETTINGS, Map.of("foo", "bar"));

                ActionListener<Model> errorVerificationListener = ActionListener.wrap((model) -> {
                    if (throwOnUnknown) {
                        fail("Model verification should fail when throwOnUnknown is true");
                    }
                }, (e) -> {
                    if (throwOnUnknown) {
                        assertThat(
                            e.getMessage(),
                            containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser] service")
                        );
                    } else {
                        fail("Model verification should not fail when throwOnUnknown is false");
                    }
                });
                if (throwOnUnknown == false) {
                    var parsed = service.parsePersistedConfigWithSecrets(
                        "foo",
                        TaskType.SPARSE_EMBEDDING,
                        settings,
                        Collections.emptyMap()
                    );
                } else {
                    service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), errorVerificationListener);
                }
            }

            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    ModelConfigurations.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(
                            ElserMlNodeServiceSettings.NUM_ALLOCATIONS,
                            1,
                            ElserMlNodeServiceSettings.NUM_THREADS,
                            4,
                            ElserMlNodeServiceSettings.MODEL_VERSION,
                            ".elser_model_2",
                            "foo",
                            "bar"
                        )
                    )
                );
                settings.put(ModelConfigurations.TASK_SETTINGS, Map.of("foo", "bar"));

                ActionListener<Model> errorVerificationListener = ActionListener.wrap((model) -> {
                    if (throwOnUnknown) {
                        fail("Model verification should fail when throwOnUnknown is true");
                    }
                }, (e) -> {
                    if (throwOnUnknown) {
                        assertThat(
                            e.getMessage(),
                            containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser] service")
                        );
                    } else {
                        fail("Model verification should not fail when throwOnUnknown is false");
                    }
                });
                if (throwOnUnknown == false) {
                    var parsed = service.parsePersistedConfigWithSecrets(
                        "foo",
                        TaskType.SPARSE_EMBEDDING,
                        settings,
                        Collections.emptyMap()
                    );
                } else {
                    service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), errorVerificationListener);
                }
            }
        }
    }

    public void testParseRequestConfig_DefaultModel() {
        var service = createService(mock(Client.class));
        {
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
            );

            ActionListener<Model> modelActionListener = ActionListener.<Model>wrap((model) -> {
                assertEquals(".elser_model_2", ((ElserMlNodeModel) model).getServiceSettings().getModelVariant());
            }, (e) -> { fail("Model verification should not fail"); });

            service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), modelActionListener);
        }
        {
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
            );

            ActionListener<Model> modelActionListener = ActionListener.<Model>wrap((model) -> {
                assertEquals(".elser_model_2_linux-x86_64", ((ElserMlNodeModel) model).getServiceSettings().getModelVariant());
            }, (e) -> { fail("Model verification should not fail"); });

            service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of("linux-x86_64"), modelActionListener);
        }
    }

    private ElserMlNodeService createService(Client client) {
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(client);
        return new ElserMlNodeService(context);
    }
}

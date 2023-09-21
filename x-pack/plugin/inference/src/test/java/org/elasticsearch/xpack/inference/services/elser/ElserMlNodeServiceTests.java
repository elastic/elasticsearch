/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class ElserMlNodeServiceTests extends ESTestCase {

    public static Model randomModelConfig(String modelId, TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new ElserMlNodeModel(
                modelId,
                taskType,
                ElserMlNodeService.NAME,
                ElserMlNodeServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom()
            );
            default -> throw new IllegalArgumentException("task type " + taskType + " is not supported");
        };
    }

    public void testParseConfigStrict() {
        var service = new ElserMlNodeService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            Model.SERVICE_SETTINGS,
            new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
        );
        settings.put(Model.TASK_SETTINGS, Map.of());

        ElserMlNodeModel parsedModel = service.parseConfigStrict("foo", TaskType.SPARSE_EMBEDDING, settings);

        assertEquals(
            new ElserMlNodeModel(
                "foo",
                TaskType.SPARSE_EMBEDDING,
                ElserMlNodeService.NAME,
                new ElserMlNodeServiceSettings(1, 4),
                ElserMlNodeTaskSettings.DEFAULT
            ),
            parsedModel
        );
    }

    public void testParseConfigStrictWithNoTaskSettings() {
        var service = new ElserMlNodeService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            Model.SERVICE_SETTINGS,
            new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
        );

        ElserMlNodeModel parsedModel = service.parseConfigStrict("foo", TaskType.SPARSE_EMBEDDING, settings);

        assertEquals(
            new ElserMlNodeModel(
                "foo",
                TaskType.SPARSE_EMBEDDING,
                ElserMlNodeService.NAME,
                new ElserMlNodeServiceSettings(1, 4),
                ElserMlNodeTaskSettings.DEFAULT
            ),
            parsedModel
        );
    }

    public void testParseConfigStrictWithUnknownSettings() {

        for (boolean throwOnUnknown : new boolean[] { true, false }) {
            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    Model.SERVICE_SETTINGS,
                    new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
                );
                settings.put(Model.TASK_SETTINGS, Map.of());
                settings.put("foo", "bar");

                if (throwOnUnknown) {
                    var e = expectThrows(
                        ElasticsearchStatusException.class,
                        () -> ElserMlNodeService.parseConfig(throwOnUnknown, "foo", TaskType.SPARSE_EMBEDDING, settings)
                    );
                    assertThat(
                        e.getMessage(),
                        containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser_mlnode] service")
                    );
                } else {
                    var parsed = ElserMlNodeService.parseConfig(throwOnUnknown, "foo", TaskType.SPARSE_EMBEDDING, settings);
                }
            }

            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    Model.SERVICE_SETTINGS,
                    new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
                );
                settings.put(Model.TASK_SETTINGS, Map.of("foo", "bar"));

                if (throwOnUnknown) {
                    var e = expectThrows(
                        ElasticsearchStatusException.class,
                        () -> ElserMlNodeService.parseConfig(throwOnUnknown, "foo", TaskType.SPARSE_EMBEDDING, settings)
                    );
                    assertThat(
                        e.getMessage(),
                        containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser_mlnode] service")
                    );
                } else {
                    var parsed = ElserMlNodeService.parseConfig(throwOnUnknown, "foo", TaskType.SPARSE_EMBEDDING, settings);
                }
            }

            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    Model.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4, "foo", "bar")
                    )
                );

                if (throwOnUnknown) {
                    var e = expectThrows(
                        ElasticsearchStatusException.class,
                        () -> ElserMlNodeService.parseConfig(throwOnUnknown, "foo", TaskType.SPARSE_EMBEDDING, settings)
                    );
                    assertThat(
                        e.getMessage(),
                        containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser_mlnode] service")
                    );
                } else {
                    var parsed = ElserMlNodeService.parseConfig(throwOnUnknown, "foo", TaskType.SPARSE_EMBEDDING, settings);
                }
            }
        }
    }
}

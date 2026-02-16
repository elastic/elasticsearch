/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.junit.Assume;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Base class for testing inference services.
 * <p>
 * This class provides common unit tests for inference services, such as testing the model creation, and calling the infer method.
 *
 * To use this class, extend it and pass the constructor a configuration.
 * </p>
 */
public abstract class AbstractInferenceServiceTests extends AbstractInferenceServiceBaseTests {

    public AbstractInferenceServiceTests(TestConfiguration testConfiguration) {
        super(testConfiguration);
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws Exception {
        Assume.assumeTrue(testConfiguration.commonConfig().supportedTaskTypes().contains(TaskType.TEXT_EMBEDDING));

        var parseRequestConfigTestConfig = testConfiguration.commonConfig();

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.REQUEST),
                parseRequestConfigTestConfig.createTaskSettingsMap(TaskType.TEXT_EMBEDDING),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, listener);

            var model = listener.actionGet(TIMEOUT);
            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(Map.of());
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            parseRequestConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws Exception {
        Assume.assumeTrue(testConfiguration.commonConfig().supportedTaskTypes().contains(TaskType.TEXT_EMBEDDING));

        var parseRequestConfigTestConfig = testConfiguration.commonConfig();

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var chunkingSettingsMap = createRandomChunkingSettingsMap();
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.REQUEST),
                parseRequestConfigTestConfig.createTaskSettingsMap(TaskType.TEXT_EMBEDDING),
                chunkingSettingsMap,
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, listener);

            var model = listener.actionGet(TIMEOUT);
            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            parseRequestConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParseRequestConfig_CreatesACompletionModel() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig();
        Assume.assumeTrue(testConfiguration.commonConfig().supportedTaskTypes().contains(TaskType.COMPLETION));

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.REQUEST),
                parseRequestConfigTestConfig.createTaskSettingsMap(TaskType.COMPLETION),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.COMPLETION, config, listener);

            parseRequestConfigTestConfig.assertModel(listener.actionGet(TIMEOUT), TaskType.COMPLETION);
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig();

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.targetTaskType(),
                    ConfigurationParseContext.REQUEST
                ),
                parseRequestConfigTestConfig.createTaskSettingsMap(parseRequestConfigTestConfig.targetTaskType()),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.unsupportedTaskType(), config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                exception.getMessage(),
                containsString(
                    Strings.format("service does not support task type [%s]", parseRequestConfigTestConfig.unsupportedTaskType())
                )
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig();

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.targetTaskType(),
                    ConfigurationParseContext.REQUEST
                ),
                parseRequestConfigTestConfig.createTaskSettingsMap(parseRequestConfigTestConfig.targetTaskType()),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );
            config.put("extra_key", "value");

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.targetTaskType(), config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig();
        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var serviceSettings = parseRequestConfigTestConfig.createServiceSettingsMap(
                parseRequestConfigTestConfig.targetTaskType(),
                ConfigurationParseContext.REQUEST
            );
            serviceSettings.put("extra_key", "value");
            var config = getRequestConfigMap(
                serviceSettings,
                parseRequestConfigTestConfig.createTaskSettingsMap(parseRequestConfigTestConfig.targetTaskType()),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.targetTaskType(), config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig();
        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var taskSettings = parseRequestConfigTestConfig.createTaskSettingsMap(parseRequestConfigTestConfig.targetTaskType());
            taskSettings.put("extra_key", "value");
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.targetTaskType(),
                    ConfigurationParseContext.REQUEST
                ),
                taskSettings,
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.targetTaskType(), config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig();
        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var secretSettingsMap = parseRequestConfigTestConfig.createSecretSettingsMap();
            secretSettingsMap.put("extra_key", "value");
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.targetTaskType(),
                    ConfigurationParseContext.REQUEST
                ),
                parseRequestConfigTestConfig.createTaskSettingsMap(parseRequestConfigTestConfig.targetTaskType()),
                secretSettingsMap
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.targetTaskType(), config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotValid() throws IOException {
        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var listener = new PlainActionFuture<InferenceServiceResults>();

            service.infer(
                getInvalidModel("id", "service"),
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                exception.getMessage(),
                is("The internal model was invalid, please delete the service [service] with id [id] and add it again.")
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration().isEnabled());

        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.updateModelWithEmbeddingDetails(getInvalidModel("id", "service"), randomNonNegativeInt())
            );

            assertThat(exception.getMessage(), containsString("Can't update embedding details for model"));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration().isEnabled());

        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var embeddingSize = randomNonNegativeInt();
            var model = testConfiguration.updateModelConfiguration().createEmbeddingModel(null);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.DOT_PRODUCT, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration().isEnabled());

        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var embeddingSize = randomNonNegativeInt();
            var model = testConfiguration.updateModelConfiguration().createEmbeddingModel(SimilarityMeasure.COSINE);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.COSINE, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    // streaming tests
    public void testSupportedStreamingTasks() throws Exception {
        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            assertThat(service.supportedStreamingTasks(), is(testConfiguration.commonConfig().supportedStreamingTasks()));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }
}

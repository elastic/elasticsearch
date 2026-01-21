/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ContextualAiServiceTests extends InferenceServiceTestCase {

    private static final String INFERENCE_ENTITY_ID_VALUE = "test-inference-entity-id";
    private static final String MODEL_ID_VALUE = "some-model";
    private static final String URL_VALUE = "url-value";
    private static final String API_KEY_VALUE = "test-api-key";
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @Override
    public ContextualAiService createInferenceService() {
        return new ContextualAiService(
            mock(HttpRequestSender.Factory.class),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize(MODEL_ID_VALUE), is(5500));
    }

    public void testBuildModelFromConfigAndSecrets_Rerank() throws IOException, URISyntaxException {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID_VALUE,
            new ContextualAiRerankServiceSettings(new URI(URL_VALUE), MODEL_ID_VALUE, null),
            new ContextualAiRerankTaskSettings(null, null, null),
            new DefaultSecretSettings(new SecureString(API_KEY_VALUE.toCharArray()))
        );
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.CHAT_COMPLETION,
            ContextualAiService.NAME,
            mock(ServiceSettings.class)
        );
        try (var inferenceService = createInferenceService()) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                thrownException.getMessage(),
                CoreMatchers.is(
                    Strings.format(
                        """
                            Failed to parse stored model [%s] for [%s] service, error: [The [%s] service does not support task type [%s]]. \
                            Please delete and add the service again""",
                        INFERENCE_ENTITY_ID_VALUE,
                        ContextualAiService.NAME,
                        ContextualAiService.NAME,
                        TaskType.CHAT_COMPLETION
                    )
                )
            );
        }
    }

    private void validateModelBuilding(Model model) throws IOException {
        try (var inferenceService = createInferenceService()) {
            var resultModel = inferenceService.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
            assertThat(resultModel, CoreMatchers.is(model));
        }
    }
}

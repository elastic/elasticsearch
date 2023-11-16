/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LearnToRankServiceTests extends ESTestCase {
    public static final String GOOD_MODEL = "modelId";
    public static final String BAD_MODEL = "badModel";
    public static final TrainedModelConfig GOOD_MODEL_CONFIG = TrainedModelConfig.builder()
        .setModelId(GOOD_MODEL)
        .setInput(new TrainedModelInput(List.of("field1", "field2")))
        .setEstimatedOperations(1)
        .setModelSize(2)
        .setModelType(TrainedModelType.TREE_ENSEMBLE)
        .setInferenceConfig(
            new LearnToRankConfig(
                2,
                List.of(
                    new QueryExtractorBuilder("feature_1", new QueryProvider(Collections.emptyMap(), null, null)),
                    new QueryExtractorBuilder("feature_2", new QueryProvider(Collections.emptyMap(), null, null))
                )
            )
        )
        .build();
    public static final TrainedModelConfig BAD_MODEL_CONFIG = TrainedModelConfig.builder()
        .setModelId(BAD_MODEL)
        .setInput(new TrainedModelInput(List.of("field1", "field2")))
        .setEstimatedOperations(1)
        .setModelSize(2)
        .setModelType(TrainedModelType.TREE_ENSEMBLE)
        .setInferenceConfig(new RegressionConfig(null, null))
        .build();

    private ThreadPool threadPool;
    private Client client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = mockClient(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    public void testLoadLearnToRankConfig() throws Exception {
        LearnToRankService learnToRankService = new LearnToRankService(mockModelLoadingService(), mockScriptService(), xContentRegistry());
        ActionListener<LearnToRankConfig> listener = mock(ActionListener.class);
        learnToRankService.loadLearnToRankConfig(client, GOOD_MODEL, Collections.emptyMap(), listener);
        assertBusy(() -> { verify(listener).onResponse((LearnToRankConfig) eq(GOOD_MODEL_CONFIG.getInferenceConfig())); });
    }

    public void testLoadMissingLearnToRankConfig() {
        // TODO
    }

    public void testLoadBadLearnToRankConfig() {
        // TODO
    }

    public void testLoadLearnToRankConfigWithTemplate() {
        // TODO
    }

    private ModelLoadingService mockModelLoadingService() {
        return mock(ModelLoadingService.class);
    }

    private ScriptService mockScriptService() {
        return mock(ScriptService.class);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    private Client mockClient(ThreadPool threadPool) {
        return new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action instanceof GetTrainedModelsAction) {
                    // Ignore this, it's verified in another test
                    GetTrainedModelsAction.Request getModelsRequest = (GetTrainedModelsAction.Request) request;
                    if (getModelsRequest.getResourceId().equals(GOOD_MODEL)) {
                        listener.onResponse(
                            (Response) GetTrainedModelsAction.Response.builder().setModels(List.of(GOOD_MODEL_CONFIG)).build()
                        );
                    } else if (getModelsRequest.getResourceId().equals(BAD_MODEL)) {
                        listener.onResponse(
                            (Response) GetTrainedModelsAction.Response.builder().setModels(List.of(BAD_MODEL_CONFIG)).build()
                        );
                    } else {
                        listener.onFailure(ExceptionsHelper.missingTrainedModel(getModelsRequest.getResourceId()));
                    }
                } else {
                    fail("client called with unexpected request:" + request.toString());
                }
            }
        };
    }
}

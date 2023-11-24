/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.ltr.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.utils.QueryProviderTests;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LearnToRankServiceTests extends ESTestCase {
    public static final String GOOD_MODEL = "modelId";
    public static final String BAD_MODEL = "badModel";
    public static final String TEMPLATED_GOOD_MODEL = "templatedModelId";
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
                    new QueryExtractorBuilder("feature_1", QueryProviderTests.createRandomValidQueryProvider("field_1", "foo")),
                    new QueryExtractorBuilder("feature_2", QueryProviderTests.createRandomValidQueryProvider("field_2", "bar"))
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

    public static final TrainedModelConfig TEMPLATED_GOOD_MODEL_CONFIG = new TrainedModelConfig.Builder(GOOD_MODEL_CONFIG).setModelId(
        TEMPLATED_GOOD_MODEL
    )
        .setInferenceConfig(
            new LearnToRankConfig(
                2,
                List.of(
                    new QueryExtractorBuilder("feature_1", QueryProviderTests.createRandomValidQueryProvider("field_1", "{{foo_param}}")),
                    new QueryExtractorBuilder("feature_2", QueryProviderTests.createRandomValidQueryProvider("field_2", "{{bar_param}}"))
                )
            )
        )
        .build();

    @SuppressWarnings("unchecked")
    public void testLoadLearnToRankConfig() throws Exception {
        LearnToRankService learnToRankService = new LearnToRankService(
            mockModelLoadingService(),
            mockTrainedModelProvider(),
            mockScriptService(),
            xContentRegistry()
        );
        ActionListener<LearnToRankConfig> listener = mock(ActionListener.class);
        learnToRankService.loadLearnToRankConfig(GOOD_MODEL, Collections.emptyMap(), listener);
        assertBusy(() -> verify(listener).onResponse(eq((LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig())));
    }

    @SuppressWarnings("unchecked")
    public void testLoadMissingLearnToRankConfig() throws Exception {
        LearnToRankService learnToRankService = new LearnToRankService(
            mockModelLoadingService(),
            mockTrainedModelProvider(),
            mockScriptService(),
            xContentRegistry()
        );
        ActionListener<LearnToRankConfig> listener = mock(ActionListener.class);
        learnToRankService.loadLearnToRankConfig("non-existing-model", Collections.emptyMap(), listener);
        assertBusy(() -> verify(listener).onFailure(isA(ResourceNotFoundException.class)));
    }

    @SuppressWarnings("unchecked")
    public void testLoadBadLearnToRankConfig() throws Exception {
        LearnToRankService learnToRankService = new LearnToRankService(
            mockModelLoadingService(),
            mockTrainedModelProvider(),
            mockScriptService(),
            xContentRegistry()
        );
        ActionListener<LearnToRankConfig> listener = mock(ActionListener.class);
        learnToRankService.loadLearnToRankConfig(BAD_MODEL, Collections.emptyMap(), listener);
        assertBusy(() -> verify(listener).onFailure(isA(ElasticsearchStatusException.class)));
    }

    @SuppressWarnings("unchecked")
    public void testLoadLearnToRankConfigWithTemplate() throws Exception {
        LearnToRankService learnToRankService = new LearnToRankService(
            mockModelLoadingService(),
            mockTrainedModelProvider(),
            mockScriptService(),
            xContentRegistry()
        );

        // When no parameters are provided we expect the templated queries not being part of the retrieved config.
        ActionListener<LearnToRankConfig> noParamsListener = mock(ActionListener.class);
        learnToRankService.loadLearnToRankConfig(TEMPLATED_GOOD_MODEL, Collections.emptyMap(), noParamsListener);
        assertBusy(() -> verify(noParamsListener).onResponse(argThat(retrievedConfig -> {
            assertThat(retrievedConfig.getFeatureExtractorBuilders(), hasSize(2));
            assertEquals(retrievedConfig, TEMPLATED_GOOD_MODEL_CONFIG.getInferenceConfig());
            return true;
        })));

        // Now testing when providing all the params of the template.
        ActionListener<LearnToRankConfig> allParamsListener = mock(ActionListener.class);
        learnToRankService.loadLearnToRankConfig(
            TEMPLATED_GOOD_MODEL,
            Map.ofEntries(Map.entry("foo_param", "foo"), Map.entry("bar_param", "bar")),
            allParamsListener
        );
        assertBusy(() -> verify(allParamsListener).onResponse(argThat(retrievedConfig -> {
            assertThat(retrievedConfig.getFeatureExtractorBuilders(), hasSize(2));
            assertEquals(retrievedConfig, GOOD_MODEL_CONFIG.getInferenceConfig());
            return true;
        })));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    private ModelLoadingService mockModelLoadingService() {
        return mock(ModelLoadingService.class);
    }

    @SuppressWarnings("unchecked")
    private TrainedModelProvider mockTrainedModelProvider() {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);

        doAnswer(invocation -> {
            String modelId = invocation.getArgument(0);
            ActionListener<TrainedModelConfig> l = invocation.getArgument(3, ActionListener.class);
            switch (modelId) {
                case GOOD_MODEL -> l.onResponse(GOOD_MODEL_CONFIG);
                case TEMPLATED_GOOD_MODEL -> l.onResponse(TEMPLATED_GOOD_MODEL_CONFIG);
                case BAD_MODEL -> l.onResponse(BAD_MODEL_CONFIG);
                default -> l.onFailure(new ResourceNotFoundException("missing model"));
            }
            return null;

        }).when(trainedModelProvider).getTrainedModel(any(), any(), any(), any());

        return trainedModelProvider;
    }

    private ScriptService mockScriptService() {
        ScriptEngine scriptEngine = new MustacheScriptEngine();
        return new ScriptService(Settings.EMPTY, Map.of(DEFAULT_TEMPLATE_LANG, scriptEngine), ScriptModule.CORE_CONTEXTS, () -> 1L);
    }
}

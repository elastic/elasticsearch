/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.ltr.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.utils.QueryProviderTests;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LearningToRankServiceTests extends ESTestCase {
    public static final String GOOD_MODEL = "inference-entity-id";
    public static final String BAD_MODEL = "bad-model";
    public static final TrainedModelConfig GOOD_MODEL_CONFIG = TrainedModelConfig.builder()
        .setModelId(GOOD_MODEL)
        .setEstimatedOperations(1)
        .setModelSize(2)
        .setModelType(TrainedModelType.TREE_ENSEMBLE)
        .setInferenceConfig(
            new LearningToRankConfig(
                2,
                List.of(
                    new QueryExtractorBuilder("feature_1", QueryProviderTests.createTestQueryProvider("field_1", "foo")),
                    new QueryExtractorBuilder("feature_2", QueryProviderTests.createTestQueryProvider("field_2", "bar"))
                ),
                Map.of()
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

    @SuppressWarnings("unchecked")
    public void testLoadLearningToRankConfig() throws Exception {
        LearningToRankService learningToRankService = getTestLearningToRankService();
        ActionListener<LearningToRankConfig> listener = mock(ActionListener.class);
        learningToRankService.loadLearningToRankConfig(GOOD_MODEL, Map.of(), listener);

        verify(listener).onResponse(eq((LearningToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig()));
    }

    @SuppressWarnings("unchecked")
    public void testLoadMissingLearningToRankConfig() throws Exception {
        LearningToRankService learningToRankService = getTestLearningToRankService();
        ActionListener<LearningToRankConfig> listener = mock(ActionListener.class);
        learningToRankService.loadLearningToRankConfig("non-existing-model", Map.of(), listener);

        verify(listener).onFailure(isA(ResourceNotFoundException.class));
    }

    @SuppressWarnings("unchecked")
    public void testLoadBadLearningToRankConfig() throws Exception {
        LearningToRankService learningToRankService = getTestLearningToRankService();
        ActionListener<LearningToRankConfig> listener = mock(ActionListener.class);
        learningToRankService.loadLearningToRankConfig(BAD_MODEL, Map.of(), listener);

        verify(listener).onFailure(isA(ElasticsearchStatusException.class));
    }

    @SuppressWarnings("unchecked")
    public void testLoadLearningToRankConfigWithTemplate() throws Exception {
        LearningToRankConfig learningToRankConfig = new LearningToRankConfig(
            0,
            List.of(new QueryExtractorBuilder("feature_1", QueryProviderTests.createTestQueryProvider("field_1", "{{foo_param}}"))),
            Map.of()
        );

        LearningToRankService learningToRankService = getTestLearningToRankService(learningToRankConfig);
        ActionListener<LearningToRankConfig> listener = mock(ActionListener.class);

        learningToRankService.loadLearningToRankConfig("model-id", Map.ofEntries(Map.entry("foo_param", "foo")), listener);
        verify(listener).onResponse(argThat(retrievedConfig -> {
            assertThat(retrievedConfig.getFeatureExtractorBuilders(), hasSize(1));
            QueryExtractorBuilder queryExtractorBuilder = retrievedConfig.getQueryFeatureExtractorBuilders().get(0);
            assertEquals(queryExtractorBuilder.featureName(), "feature_1");
            assertEquals(queryExtractorBuilder.query(), QueryProviderTests.createTestQueryProvider("field_1", "foo"));
            return true;
        }));
    }

    @SuppressWarnings("unchecked")
    public void testLoadLearningToRankConfigWithMissingTemplateParams() throws Exception {
        LearningToRankConfig learningToRankConfig = new LearningToRankConfig(
            0,
            List.of(
                new QueryExtractorBuilder("feature_1", QueryProviderTests.createTestQueryProvider("field_1", "foo")),
                new QueryExtractorBuilder("feature_2", QueryProviderTests.createTestQueryProvider("field_1", "{{foo_param}}")),
                new QueryExtractorBuilder("feature_3", QueryProviderTests.createTestQueryProvider("field_1", "{{bar_param}}"), 1.5f),
                new QueryExtractorBuilder("feature_4", QueryProviderTests.createTestQueryProvider("field_1", "{{baz_param}}"))
            ),
            Map.of("baz_param", "default_value")
        );

        LearningToRankService learningToRankService = getTestLearningToRankService(learningToRankConfig);
        ActionListener<LearningToRankConfig> listener = mock(ActionListener.class);

        learningToRankService.loadLearningToRankConfig("model-id", randomBoolean() ? null : Map.of(), listener);
        verify(listener).onResponse(argThat(retrievedConfig -> {
            // Check all features are present.
            assertThat(retrievedConfig.getFeatureExtractorBuilders(), hasSize(4));

            Map<String, QueryExtractorBuilder> queryExtractorBuilders = retrievedConfig.getQueryFeatureExtractorBuilders()
                .stream()
                .collect(Collectors.toMap(QueryExtractorBuilder::featureName, Function.identity()));

            // feature_1 will be extracted using the provided query since no params are missing for it
            assertThat(queryExtractorBuilders, hasKey("feature_1"));
            assertEquals(queryExtractorBuilders.get("feature_1").query(), QueryProviderTests.createTestQueryProvider("field_1", "foo"));

            // feature_2 will be extracted using a match_none query because {{foo_params}} is missing
            assertThat(queryExtractorBuilders, hasKey("feature_2"));
            assertEquals(queryExtractorBuilders.get("feature_2").query().getParsedQuery(), new MatchNoneQueryBuilder());

            // feature_3 will be extracted using a match_all query with a boost because:
            // - {{bar_param}} is missing
            // - a default_score is provided for the query extractor
            assertThat(queryExtractorBuilders, hasKey("feature_3"));
            assertEquals(queryExtractorBuilders.get("feature_3").query().getParsedQuery(), new MatchAllQueryBuilder().boost(1.5f));

            // feature_4 will be extracted using the default value for the {{baz_param}}
            assertThat(queryExtractorBuilders, hasKey("feature_4"));
            assertEquals(
                queryExtractorBuilders.get("feature_4").query(),
                QueryProviderTests.createTestQueryProvider("field_1", "default_value")
            );

            return true;
        }));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    private ModelLoadingService mockModelLoadingService() {
        ModelLoadingService modelLoadingService = mock(ModelLoadingService.class);
        when(modelLoadingService.getModelId(anyString())).thenAnswer(i -> i.getArgument(0));

        return modelLoadingService;
    }

    @SuppressWarnings("unchecked")
    private TrainedModelProvider mockTrainedModelProvider() {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);

        doAnswer(invocation -> {
            String modelId = invocation.getArgument(0);
            ActionListener<TrainedModelConfig> l = invocation.getArgument(3, ActionListener.class);
            switch (modelId) {
                case GOOD_MODEL -> l.onResponse(GOOD_MODEL_CONFIG);
                case BAD_MODEL -> l.onResponse(BAD_MODEL_CONFIG);
                default -> l.onFailure(new ResourceNotFoundException("missing model"));
            }
            return null;

        }).when(trainedModelProvider).getTrainedModel(any(), any(), any(), any());

        return trainedModelProvider;
    }

    private LearningToRankService getTestLearningToRankService() {
        return getTestLearningToRankService(mockTrainedModelProvider());
    }

    @SuppressWarnings("unchecked")
    private LearningToRankService getTestLearningToRankService(LearningToRankConfig learningToRankConfig) {
        TrainedModelProvider trainedModelProvider = mock(TrainedModelProvider.class);

        doAnswer(invocation -> {
            String modelId = invocation.getArgument(0);
            ActionListener<TrainedModelConfig> l = invocation.getArgument(3, ActionListener.class);

            l.onResponse(
                TrainedModelConfig.builder()
                    .setModelId(modelId)
                    .setInput(new TrainedModelInput(List.of("field1", "field2")))
                    .setEstimatedOperations(1)
                    .setModelSize(2)
                    .setModelType(TrainedModelType.TREE_ENSEMBLE)
                    .setInferenceConfig(learningToRankConfig)
                    .build()
            );
            return null;

        }).when(trainedModelProvider).getTrainedModel(any(), any(), any(), any());

        return getTestLearningToRankService(trainedModelProvider);
    }

    private LearningToRankService getTestLearningToRankService(TrainedModelProvider trainedModelProvider) {
        return new LearningToRankService(mockModelLoadingService(), trainedModelProvider, getTestScriptService(), xContentRegistry());
    }

    private ScriptService getTestScriptService() {
        ScriptEngine scriptEngine = new MustacheScriptEngine(Settings.EMPTY);
        return new ScriptService(Settings.EMPTY, Map.of(DEFAULT_TEMPLATE_LANG, scriptEngine), ScriptModule.CORE_CONTEXTS, () -> 1L);
    }
}

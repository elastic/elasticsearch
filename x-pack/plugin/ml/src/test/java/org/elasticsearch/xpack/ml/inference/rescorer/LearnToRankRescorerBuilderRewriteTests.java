/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.DataRewriteContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LearnToRankRescorerBuilderRewriteTests extends AbstractBuilderTestCase {

    private static final String GOOD_MODEL = "modelId";
    private static final String BAD_MODEL = "badModel";
    private static final TrainedModelConfig GOOD_MODEL_CONFIG = TrainedModelConfig.builder()
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
    private static final TrainedModelConfig BAD_MODEL_CONFIG = TrainedModelConfig.builder()
        .setModelId(BAD_MODEL)
        .setInput(new TrainedModelInput(List.of("field1", "field2")))
        .setEstimatedOperations(1)
        .setModelSize(2)
        .setModelType(TrainedModelType.TREE_ENSEMBLE)
        .setInferenceConfig(new RegressionConfig(null, null))
        .build();

    public void testMustRewrite() {
        TestModelLoader testModelLoader = new TestModelLoader();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            GOOD_MODEL,
            testModelLoader,
            () -> LearnToRankConfigTests.randomLearnToRankConfig()
        );
        SearchExecutionContext context = createSearchExecutionContext();
        LearnToRankRescorerContext rescorerContext = rescorerBuilder.innerBuildContext(randomIntBetween(1, 30), context);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> rescorerContext.rescorer()
                .rescore(
                    new TopDocs(new TotalHits(10, TotalHits.Relation.EQUAL_TO), new ScoreDoc[10]),
                    mock(IndexSearcher.class),
                    rescorerContext
                )
        );
        assertEquals("local model reference is null, missing rewriteAndFetch before rescore phase?", e.getMessage());
    }

    public void testRewriteOnCoordinator() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        ScriptService scriptService = mock(ScriptService.class);
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(GOOD_MODEL, null, testModelLoader, scriptService);
        rescorerBuilder.windowSize(4);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        LearnToRankRescorerBuilder rewritten = rewriteAndFetch(rescorerBuilder, context);
        assertThat(rewritten.learnToRankConfigSupplier().get(), not(nullValue()));
        assertThat(rewritten.learnToRankConfigSupplier().get().getNumTopFeatureImportanceValues(), equalTo(2));
        assertThat(
            "feature_1",
            is(
                in(
                    rewritten.learnToRankConfigSupplier()
                        .get()
                        .getFeatureExtractorBuilders()
                        .stream()
                        .map(LearnToRankFeatureExtractorBuilder::featureName)
                        .toList()
                )
            )
        );
        assertThat(rewritten.windowSize(), equalTo(4));
    }

    public void testRewriteOnCoordinatorWithBadModel() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        ScriptService scriptService = mock(ScriptService.class);
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(BAD_MODEL, null, testModelLoader, scriptService);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> rewriteAndFetch(rescorerBuilder, context));
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testRewriteOnCoordinatorWithMissingModel() {
        TestModelLoader testModelLoader = new TestModelLoader();
        ScriptService scriptService = mock(ScriptService.class);
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder("missing_model", null, testModelLoader, scriptService);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        expectThrows(ResourceNotFoundException.class, () -> rewriteAndFetch(rescorerBuilder, context));
    }

    public void testSearchRewrite() throws IOException {
        LocalModel localModel = localModel();
        when(localModel.getModelId()).thenReturn(GOOD_MODEL);
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            () -> LearnToRankConfigTests.randomLearnToRankConfig(),
            () -> localModel
        );

        QueryRewriteContext context = createSearchExecutionContext();
        LearnToRankRescorerBuilder rewritten = (LearnToRankRescorerBuilder) Rewriteable.rewrite(rescorerBuilder, context, true);

        LearnToRankConfig rewrittenLearnToRankConfig = Rewriteable.rewrite(rewritten.learnToRankConfigSupplier().get(), context);
        assertThat(rewritten.localModelSupplier().get(), is(localModel));
        assertThat(rewritten.learnToRankConfigSupplier().get(), is(rewrittenLearnToRankConfig));
    }

    protected LearnToRankRescorerBuilder rewriteAndFetch(RescorerBuilder<LearnToRankRescorerBuilder> builder, QueryRewriteContext context) {
        PlainActionFuture<RescorerBuilder<LearnToRankRescorerBuilder>> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(builder, context, future);
        return (LearnToRankRescorerBuilder) future.actionGet();
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof GetTrainedModelsAction);
    }

    @Override
    protected Object simulateMethod(Method method, Object[] args) {
        GetTrainedModelsAction.Request request = (GetTrainedModelsAction.Request) args[1];
        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<GetTrainedModelsAction.Response> listener = (ActionListener<GetTrainedModelsAction.Response>) args[2];
        if (request.getResourceId().equals(GOOD_MODEL)) {
            listener.onResponse(GetTrainedModelsAction.Response.builder().setModels(List.of(GOOD_MODEL_CONFIG)).build());
            return null;
        }
        if (request.getResourceId().equals(BAD_MODEL)) {
            listener.onResponse(GetTrainedModelsAction.Response.builder().setModels(List.of(BAD_MODEL_CONFIG)).build());
            return null;
        }
        listener.onFailure(ExceptionsHelper.missingTrainedModel(request.getResourceId()));
        return null;
    }

    public void testRewriteOnShard() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            GOOD_MODEL,
            testModelLoader,
            () -> (LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig()
        );
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        LearnToRankRescorerBuilder rewritten = (LearnToRankRescorerBuilder) rescorerBuilder.rewrite(createSearchExecutionContext());
        assertSame(rescorerBuilder, rewritten);
        assertFalse(searchExecutionContext.hasAsyncActions());
    }

    public void testRewriteAndFetchOnDataNode() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            GOOD_MODEL,
            testModelLoader,
            () -> LearnToRankConfigTests.randomLearnToRankConfig()
        );
        boolean setWindowSize = randomBoolean();
        if (setWindowSize) {
            rescorerBuilder.windowSize(42);
        }
        DataRewriteContext rewriteContext = dataRewriteContext();
        LearnToRankRescorerBuilder rewritten = (LearnToRankRescorerBuilder) rescorerBuilder.rewrite(rewriteContext);
        assertNotSame(rescorerBuilder, rewritten);
        assertTrue(rewriteContext.hasAsyncActions());
        if (setWindowSize) {
            assertThat(rewritten.windowSize(), equalTo(42));
        }
    }

    public void testBuildContext() throws Exception {
        LocalModel localModel = localModel();
        List<String> inputFields = List.of(DOUBLE_FIELD_NAME, INT_FIELD_NAME);
        when(localModel.inputFields()).thenReturn(inputFields);
        SearchExecutionContext context = createSearchExecutionContext();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            () -> LearnToRankConfigTests.randomLearnToRankConfig(),
            () -> localModel
        );

        LearnToRankRescorerContext rescoreContext = rescorerBuilder.innerBuildContext(20, context);
        assertNotNull(rescoreContext);
        assertThat(rescoreContext.getWindowSize(), equalTo(20));
        List<FeatureExtractor> featureExtractors = rescoreContext.buildFeatureExtractors(context.searcher());
        assertThat(featureExtractors, hasSize(1));
        assertThat(
            featureExtractors.stream().flatMap(featureExtractor -> featureExtractor.featureNames().stream()).toList(),
            containsInAnyOrder(DOUBLE_FIELD_NAME, INT_FIELD_NAME)
        );
    }

    private static LocalModel localModel() {
        return mock(LocalModel.class);
    }

    private static QueryExtractorBuilder queryExtractorBuilder(String featureName) throws IOException {
        QueryProvider queryProvider = mock(QueryProvider.class);
        when(queryProvider.rewrite(any())).thenReturn(queryProvider);
        return new QueryExtractorBuilder(featureName, queryProvider);
    }

    private static class TestModelLoader extends ModelLoadingService {
        TestModelLoader() {
            super(
                mock(TrainedModelProvider.class),
                mock(InferenceAuditor.class),
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(TrainedModelStatsService.class),
                Settings.EMPTY,
                "test",
                mock(CircuitBreaker.class),
                new XPackLicenseState(System::currentTimeMillis)
            );
        }

        @Override
        public void getModelForLearnToRank(String modelId, ActionListener<LocalModel> modelActionListener) {
            modelActionListener.onResponse(localModel());
        }
    }
}

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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdateTests;
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
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceRescorerBuilderRewriteTests extends AbstractBuilderTestCase {

    private static final String GOOD_MODEL = "modelId";
    private static final String BAD_MODEL = "badModel";
    private static final TrainedModelConfig GOOD_MODEL_CONFIG = TrainedModelConfig.builder()
        .setModelId(GOOD_MODEL)
        .setInput(new TrainedModelInput(List.of("field1", "field2")))
        .setEstimatedOperations(1)
        .setModelSize(2)
        .setModelType(TrainedModelType.TREE_ENSEMBLE)
        .setInferenceConfig(new LearnToRankConfig(null, null))
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
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            GOOD_MODEL,
            LearnToRankConfigTests.randomLearnToRankConfig(),
            () -> testModelLoader
        );
        SearchExecutionContext context = createSearchExecutionContext();
        InferenceRescorerContext inferenceRescorerContext = inferenceRescorerBuilder.innerBuildContext(randomIntBetween(1, 30), context);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> inferenceRescorerContext.rescorer()
                .rescore(
                    new TopDocs(new TotalHits(10, TotalHits.Relation.EQUAL_TO), new ScoreDoc[10]),
                    mock(IndexSearcher.class),
                    inferenceRescorerContext
                )
        );
        assertEquals("local model reference is null, missing rewriteAndFetch before rescore phase?", e.getMessage());
    }

    public void testRewriteOnCoordinator() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        LearnToRankConfigUpdate ltru = new LearnToRankConfigUpdate(
            2,
            List.of(new QueryExtractorBuilder("all", QueryProvider.fromParsedQuery(QueryBuilders.matchAllQuery())))
        );
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(GOOD_MODEL, ltru, () -> testModelLoader);
        inferenceRescorerBuilder.windowSize(4);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        InferenceRescorerBuilder rewritten = rewriteAndFetch(inferenceRescorerBuilder, context);
        assertThat(rewritten.getInferenceConfig(), not(nullValue()));
        assertThat(rewritten.getInferenceConfig().getNumTopFeatureImportanceValues(), equalTo(2));
        assertThat(
            "all",
            is(
                in(
                    rewritten.getInferenceConfig()
                        .getFeatureExtractorBuilders()
                        .stream()
                        .map(LearnToRankFeatureExtractorBuilder::featureName)
                        .toList()
                )
            )
        );
        assertThat(rewritten.getInferenceConfigUpdate(), is(nullValue()));
        assertThat(rewritten.windowSize(), equalTo(4));
    }

    public void testRewriteOnCoordinatorWithBadModel() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            BAD_MODEL,
            randomBoolean() ? null : LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate(),
            () -> testModelLoader
        );
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> rewriteAndFetch(inferenceRescorerBuilder, context)
        );
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testRewriteOnCoordinatorWithMissingModel() {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            "missing_model",
            randomBoolean() ? null : LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate(),
            () -> testModelLoader
        );
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        expectThrows(ResourceNotFoundException.class, () -> rewriteAndFetch(inferenceRescorerBuilder, context));
    }

    public void testSearchRewrite() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            GOOD_MODEL,
            LearnToRankConfigTests.randomLearnToRankConfig(),
            () -> testModelLoader
        );
        QueryRewriteContext context = createSearchExecutionContext();
        InferenceRescorerBuilder rewritten = (InferenceRescorerBuilder) Rewriteable.rewrite(inferenceRescorerBuilder, context, true);
        assertThat(rewritten.modelLoadingServiceSupplier(), is(notNullValue()));

        inferenceRescorerBuilder = new InferenceRescorerBuilder(GOOD_MODEL, LearnToRankConfigTests.randomLearnToRankConfig(), localModel());

        rewritten = (InferenceRescorerBuilder) Rewriteable.rewrite(inferenceRescorerBuilder, context, true);
        assertThat(rewritten.modelLoadingServiceSupplier(), is(nullValue()));
        assertThat(rewritten.getInferenceDefinition(), is(notNullValue()));
    }

    protected InferenceRescorerBuilder rewriteAndFetch(RescorerBuilder<InferenceRescorerBuilder> builder, QueryRewriteContext context) {
        PlainActionFuture<RescorerBuilder<InferenceRescorerBuilder>> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(builder, context, future);
        return (InferenceRescorerBuilder) future.actionGet();
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
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            GOOD_MODEL,
            (LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            () -> testModelLoader
        );
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        InferenceRescorerBuilder rewritten = (InferenceRescorerBuilder) inferenceRescorerBuilder.rewrite(createSearchExecutionContext());
        assertSame(inferenceRescorerBuilder, rewritten);
        assertFalse(searchExecutionContext.hasAsyncActions());
    }

    public void testRewriteAndFetchOnDataNode() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            GOOD_MODEL,
            (LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            () -> testModelLoader
        );
        boolean setWindowSize = randomBoolean();
        if (setWindowSize) {
            inferenceRescorerBuilder.windowSize(42);
        }
        DataRewriteContext rewriteContext = dataRewriteContext();
        InferenceRescorerBuilder rewritten = (InferenceRescorerBuilder) inferenceRescorerBuilder.rewrite(rewriteContext);
        assertNotSame(inferenceRescorerBuilder, rewritten);
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
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder(
            GOOD_MODEL,
            (LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            localModel
        );
        InferenceRescorerContext rescoreContext = inferenceRescorerBuilder.innerBuildContext(20, context);
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

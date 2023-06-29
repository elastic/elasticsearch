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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.DataRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceRescorerBuilderRewriteTests extends AbstractBuilderTestCase {

    public void testMustRewrite() {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder("modelId", () -> testModelLoader);
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
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder("modelId", () -> testModelLoader);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        InferenceRescorerBuilder rewritten = (InferenceRescorerBuilder) inferenceRescorerBuilder.rewrite(context);
        assertSame(inferenceRescorerBuilder, rewritten);
        assertFalse(context.hasAsyncActions());
    }

    public void testRewriteOnShard() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder("modelId", () -> testModelLoader);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        InferenceRescorerBuilder rewritten = (InferenceRescorerBuilder) inferenceRescorerBuilder.rewrite(createSearchExecutionContext());
        assertSame(inferenceRescorerBuilder, rewritten);
        assertFalse(searchExecutionContext.hasAsyncActions());
    }

    public void testRewriteAndFetchOnDataNode() throws IOException {
        TestModelLoader testModelLoader = new TestModelLoader();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder("modelId", () -> testModelLoader);
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

    public void testBuildContext() {
        LocalModel localModel = localModel();
        List<String> inputFields = List.of(DOUBLE_FIELD_NAME, INT_FIELD_NAME);
        when(localModel.inputFields()).thenReturn(inputFields);
        SearchExecutionContext context = createSearchExecutionContext();
        InferenceRescorerBuilder inferenceRescorerBuilder = new InferenceRescorerBuilder("test_model", localModel);
        InferenceRescorerContext rescoreContext = inferenceRescorerBuilder.innerBuildContext(20, context);
        assertNotNull(rescoreContext);
        assertThat(rescoreContext.getWindowSize(), equalTo(20));
        List<FeatureExtractor> featureExtractors = rescoreContext.buildFeatureExtractors();
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

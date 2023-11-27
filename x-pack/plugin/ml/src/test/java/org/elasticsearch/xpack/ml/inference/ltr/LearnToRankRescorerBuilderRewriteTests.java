/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.DataRewriteContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigTests.randomLearnToRankConfig;
import static org.elasticsearch.xpack.ml.inference.ltr.LearnToRankServiceTests.BAD_MODEL;
import static org.elasticsearch.xpack.ml.inference.ltr.LearnToRankServiceTests.GOOD_MODEL;
import static org.elasticsearch.xpack.ml.inference.ltr.LearnToRankServiceTests.GOOD_MODEL_CONFIG;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LearnToRankRescorerBuilderRewriteTests extends AbstractBuilderTestCase {

    public void testMustRewrite() {
        LearnToRankService learnToRankService = learnToRankServiceMock();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            GOOD_MODEL,
            randomLearnToRankConfig(),
            null,
            learnToRankService
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
        LearnToRankService learnToRankService = learnToRankServiceMock();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(GOOD_MODEL, null, learnToRankService);
        rescorerBuilder.windowSize(4);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        LearnToRankRescorerBuilder rewritten = rewriteAndFetch(rescorerBuilder, context);
        assertThat(rewritten.learnToRankConfig(), not(nullValue()));
        assertThat(rewritten.learnToRankConfig().getNumTopFeatureImportanceValues(), equalTo(2));
        assertThat(
            "feature_1",
            is(
                in(
                    rewritten.learnToRankConfig()
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
        LearnToRankService learnToRankService = learnToRankServiceMock();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(BAD_MODEL, null, learnToRankService);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> rewriteAndFetch(rescorerBuilder, context));
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testRewriteOnCoordinatorWithMissingModel() {
        LearnToRankService learnToRankService = learnToRankServiceMock();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder("missing_model", null, learnToRankService);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        expectThrows(ResourceNotFoundException.class, () -> rewriteAndFetch(rescorerBuilder, context));
    }

    public void testRewriteOnShard() throws IOException {
        LocalModel localModel = mock(LocalModel.class);
        when(localModel.getModelId()).thenReturn(GOOD_MODEL);

        LearnToRankService learnToRankService = learnToRankServiceMock();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            localModel,
            (LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            null,
            learnToRankService
        );
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        LearnToRankRescorerBuilder rewritten = (LearnToRankRescorerBuilder) rescorerBuilder.rewrite(createSearchExecutionContext());
        assertFalse(searchExecutionContext.hasAsyncActions());
        assertSame(localModel, rewritten.localModel());
        assertEquals(localModel.getModelId(), rewritten.modelId());
    }

    public void testRewriteAndFetchOnDataNode() throws IOException {
        LearnToRankService learnToRankService = learnToRankServiceMock();
        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            GOOD_MODEL,
            randomLearnToRankConfig(),
            null,
            learnToRankService
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

    @SuppressWarnings("unchecked")
    private static LearnToRankService learnToRankServiceMock() {
        LearnToRankService learnToRankService = mock(LearnToRankService.class);

        doAnswer(invocation -> {
            String modelId = invocation.getArgument(0);
            ActionListener<InferenceConfig> l = invocation.getArgument(2, ActionListener.class);
            if (modelId.equals(GOOD_MODEL)) {
                l.onResponse(GOOD_MODEL_CONFIG.getInferenceConfig());
            } else if (modelId.equals(BAD_MODEL)) {
                l.onFailure(new ElasticsearchStatusException("bad model", RestStatus.BAD_REQUEST));
            } else {
                l.onFailure(new ResourceNotFoundException("missing model"));
            }
            return null;
        }).when(learnToRankService).loadLearnToRankConfig(anyString(), any(), any());

        doAnswer(invocation -> {
            ActionListener<LocalModel> l = invocation.getArgument(1, ActionListener.class);
            l.onResponse(mock(LocalModel.class));
            return null;
        }).when(learnToRankService).loadLocalModel(anyString(), any());

        return learnToRankService;
    }

    public void testBuildContext() throws Exception {
        LocalModel localModel = mock(LocalModel.class);
        List<String> inputFields = List.of(DOUBLE_FIELD_NAME, INT_FIELD_NAME);
        when(localModel.inputFields()).thenReturn(inputFields);

        IndexSearcher searcher = mock(IndexSearcher.class);
        doAnswer(invocation -> invocation.getArgument(0)).when(searcher).rewrite(any(Query.class));
        SearchExecutionContext context = createSearchExecutionContext(searcher);

        LearnToRankRescorerBuilder rescorerBuilder = new LearnToRankRescorerBuilder(
            localModel,
            (LearnToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            null,
            mock(LearnToRankService.class)
        );

        LearnToRankRescorerContext rescoreContext = rescorerBuilder.innerBuildContext(20, context);
        assertNotNull(rescoreContext);
        assertThat(rescoreContext.getWindowSize(), equalTo(20));
        List<FeatureExtractor> featureExtractors = rescoreContext.buildFeatureExtractors(context.searcher());
        assertThat(featureExtractors, hasSize(2));
        assertThat(
            featureExtractors.stream().flatMap(featureExtractor -> featureExtractor.featureNames().stream()).toList(),
            containsInAnyOrder("feature_1", "feature_2", DOUBLE_FIELD_NAME, INT_FIELD_NAME)
        );
    }

    private LearnToRankRescorerBuilder rewriteAndFetch(RescorerBuilder<LearnToRankRescorerBuilder> builder, QueryRewriteContext context) {
        PlainActionFuture<RescorerBuilder<LearnToRankRescorerBuilder>> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(builder, context, future);
        return (LearnToRankRescorerBuilder) future.actionGet();
    }
}

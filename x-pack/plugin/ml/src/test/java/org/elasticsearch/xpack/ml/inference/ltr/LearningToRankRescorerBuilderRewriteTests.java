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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearningToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfigTests.randomLearningToRankConfig;
import static org.elasticsearch.xpack.ml.inference.ltr.LearningToRankServiceTests.BAD_MODEL;
import static org.elasticsearch.xpack.ml.inference.ltr.LearningToRankServiceTests.GOOD_MODEL;
import static org.elasticsearch.xpack.ml.inference.ltr.LearningToRankServiceTests.GOOD_MODEL_CONFIG;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LearningToRankRescorerBuilderRewriteTests extends AbstractBuilderTestCase {

    public void testMustRewrite() {
        LearningToRankService learningToRankService = learningToRankServiceMock();
        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(
            GOOD_MODEL,
            randomLearningToRankConfig(),
            null,
            learningToRankService
        );

        SearchExecutionContext context = createSearchExecutionContext();
        LearningToRankRescorerContext rescorerContext = rescorerBuilder.innerBuildContext(randomIntBetween(1, 30), context);
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
        LearningToRankService learningToRankService = learningToRankServiceMock();
        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(GOOD_MODEL, null, learningToRankService);
        rescorerBuilder.windowSize(4);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        LearningToRankRescorerBuilder rewritten = rewriteAndFetch(rescorerBuilder, context);
        assertThat(rewritten.learningToRankConfig(), not(nullValue()));
        assertThat(rewritten.learningToRankConfig().getNumTopFeatureImportanceValues(), equalTo(2));
        assertThat(
            "feature_1",
            is(
                in(
                    rewritten.learningToRankConfig()
                        .getFeatureExtractorBuilders()
                        .stream()
                        .map(LearningToRankFeatureExtractorBuilder::featureName)
                        .toList()
                )
            )
        );
        assertThat(rewritten.windowSize(), equalTo(4));
    }

    public void testRewriteOnCoordinatorWithBadModel() throws IOException {
        LearningToRankService learningToRankService = learningToRankServiceMock();
        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(BAD_MODEL, null, learningToRankService);
        CoordinatorRewriteContext context = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType("@timestamp"),
            randomIntBetween(0, 1_100_000),
            randomIntBetween(1_500_000, Integer.MAX_VALUE)
        );
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> rewriteAndFetch(rescorerBuilder, context));
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testRewriteOnCoordinatorWithMissingModel() {
        LearningToRankService learningToRankService = learningToRankServiceMock();
        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder("missing_model", null, learningToRankService);
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

        LearningToRankService learningToRankService = learningToRankServiceMock();
        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(
            localModel,
            (LearningToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            null,
            learningToRankService
        );
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        LearningToRankRescorerBuilder rewritten = (LearningToRankRescorerBuilder) rescorerBuilder.rewrite(createSearchExecutionContext());
        assertFalse(searchExecutionContext.hasAsyncActions());
        assertSame(localModel, rewritten.localModel());
        assertEquals(localModel.getModelId(), rewritten.modelId());
    }

    public void testRewriteAndFetchOnDataNode() throws IOException {
        LearningToRankService learningToRankService = learningToRankServiceMock();
        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(
            GOOD_MODEL,
            randomLearningToRankConfig(),
            null,
            learningToRankService
        );

        boolean setWindowSize = randomBoolean();
        if (setWindowSize) {
            rescorerBuilder.windowSize(42);
        }
        DataRewriteContext rewriteContext = dataRewriteContext();
        LearningToRankRescorerBuilder rewritten = (LearningToRankRescorerBuilder) rescorerBuilder.rewrite(rewriteContext);
        assertNotSame(rescorerBuilder, rewritten);
        assertTrue(rewriteContext.hasAsyncActions());
        if (setWindowSize) {
            assertThat(rewritten.windowSize(), equalTo(42));
        }
    }

    @SuppressWarnings("unchecked")
    private static LearningToRankService learningToRankServiceMock() {
        LearningToRankService learningToRankService = mock(LearningToRankService.class);

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
        }).when(learningToRankService).loadLearningToRankConfig(anyString(), any(), any());

        doAnswer(invocation -> {
            ActionListener<LocalModel> l = invocation.getArgument(1, ActionListener.class);
            l.onResponse(mock(LocalModel.class));
            return null;
        }).when(learningToRankService).loadLocalModel(anyString(), any());

        return learningToRankService;
    }

    public void testBuildContext() throws Exception {
        LocalModel localModel = mock(LocalModel.class);
        when(localModel.inputFields()).thenReturn(GOOD_MODEL_CONFIG.getInput().getFieldNames());

        IndexSearcher searcher = mock(IndexSearcher.class);
        doAnswer(invocation -> invocation.getArgument(0)).when(searcher).rewrite(any(Query.class));
        SearchExecutionContext context = createSearchExecutionContext(searcher);

        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(
            localModel,
            (LearningToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            null,
            mock(LearningToRankService.class)
        );

        LearningToRankRescorerContext rescoreContext = rescorerBuilder.innerBuildContext(20, context);
        assertNotNull(rescoreContext);
        assertThat(rescoreContext.getWindowSize(), equalTo(20));
        List<FeatureExtractor> featureExtractors = rescoreContext.buildFeatureExtractors(context.searcher());
        assertThat(featureExtractors, hasSize(1));

        FeatureExtractor queryExtractor = featureExtractors.get(0);
        assertThat(queryExtractor, instanceOf(QueryFeatureExtractor.class));
        assertThat(queryExtractor.featureNames(), hasSize(2));
        assertThat(queryExtractor.featureNames(), containsInAnyOrder("feature_1", "feature_2"));
    }

    public void testLegacyFieldValueExtractorBuildContext() throws Exception {
        // Models created before 8.15 have been saved with input fields.
        // We check field value extractors are created and the deduplication is done correctly.
        LocalModel localModel = mock(LocalModel.class);
        when(localModel.inputFields()).thenReturn(List.of("feature_1", "field_1", "field_2"));

        IndexSearcher searcher = mock(IndexSearcher.class);
        doAnswer(invocation -> invocation.getArgument(0)).when(searcher).rewrite(any(Query.class));
        SearchExecutionContext context = createSearchExecutionContext(searcher);

        LearningToRankRescorerBuilder rescorerBuilder = new LearningToRankRescorerBuilder(
            localModel,
            (LearningToRankConfig) GOOD_MODEL_CONFIG.getInferenceConfig(),
            null,
            mock(LearningToRankService.class)
        );

        LearningToRankRescorerContext rescoreContext = rescorerBuilder.innerBuildContext(20, context);
        assertNotNull(rescoreContext);
        assertThat(rescoreContext.getWindowSize(), equalTo(20));
        List<FeatureExtractor> featureExtractors = rescoreContext.buildFeatureExtractors(context.searcher());

        assertThat(featureExtractors, hasSize(2));

        FeatureExtractor queryExtractor = featureExtractors.stream().filter(fe -> fe instanceof QueryFeatureExtractor).findFirst().get();
        assertThat(queryExtractor.featureNames(), hasSize(2));
        assertThat(queryExtractor.featureNames(), containsInAnyOrder("feature_1", "feature_2"));

        FeatureExtractor fieldValueExtractor = featureExtractors.stream()
            .filter(fe -> fe instanceof FieldValueFeatureExtractor)
            .findFirst()
            .get();
        assertThat(fieldValueExtractor.featureNames(), hasSize(2));
        assertThat(fieldValueExtractor.featureNames(), containsInAnyOrder("field_1", "field_2"));
    }

    private LearningToRankRescorerBuilder rewriteAndFetch(
        RescorerBuilder<LearningToRankRescorerBuilder> builder,
        QueryRewriteContext context
    ) {
        PlainActionFuture<RescorerBuilder<LearningToRankRescorerBuilder>> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(builder, context, future);
        return (LearningToRankRescorerBuilder) future.actionGet();
    }
}

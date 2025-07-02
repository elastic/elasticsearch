/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RerankSnippetConfig;
import org.elasticsearch.search.rank.rerank.AbstractRerankerIT;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;

/**
 * Plugin for text similarity tests. Defines a filter for modifying inference call behavior, as well as a {@code TextSimilarityRankBuilder}
 * implementation that can be configured to throw an exception at various stages of processing.
 */
public class TextSimilarityTestPlugin extends Plugin implements ActionPlugin {

    private static final String inferenceId = "inference-id";
    private static final String inferenceText = "inference-text";
    private static final float minScore = 0.0f;

    private final SetOnce<TestFilter> testFilter = new SetOnce<>();

    @Override
    public Collection<?> createComponents(PluginServices services) {
        testFilter.set(new TestFilter());
        return Collections.emptyList();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return singletonList(testFilter.get());
    }

    private static final String THROWING_REQUEST_ACTION_BASED_RANK_BUILDER_NAME = "throwing_request_action_based_rank";

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                RankBuilder.class,
                THROWING_REQUEST_ACTION_BASED_RANK_BUILDER_NAME,
                ThrowingMockRequestActionBasedRankBuilder::new
            )
        );
    }

    /**
     * Action filter that captures the inference action and injects a mock response.
     */
    static class TestFilter implements ActionFilter {

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain
        ) {
            if (action.equals(GetInferenceModelAction.INSTANCE.name())) {
                assert request instanceof GetInferenceModelAction.Request;
                handleGetInferenceModelActionRequest((GetInferenceModelAction.Request) request, listener);
            } else if (action.equals(InferenceAction.INSTANCE.name())) {
                assert request instanceof InferenceAction.Request;
                handleInferenceActionRequest((InferenceAction.Request) request, listener);
            } else {
                // For any other action than get model and inference, execute normally
                chain.proceed(task, action, request, listener);
            }
        }

        @SuppressWarnings("unchecked")
        private <Response extends ActionResponse> void handleGetInferenceModelActionRequest(
            GetInferenceModelAction.Request request,
            ActionListener<Response> listener
        ) {
            String inferenceEntityId = request.getInferenceEntityId();
            Integer topN = null;
            Matcher extractTopN = Pattern.compile(".*(task-settings-top-\\d+).*").matcher(inferenceEntityId);
            if (extractTopN.find()) {
                topN = Integer.parseInt(extractTopN.group(1).replaceAll("\\D", ""));
            }

            ActionResponse response = new GetInferenceModelAction.Response(
                List.of(
                    new ModelConfigurations(
                        request.getInferenceEntityId(),
                        request.getTaskType(),
                        CohereService.NAME,
                        new CohereRerankServiceSettings("uri", "model", null, CohereServiceSettings.CohereApiVersion.V2),
                        topN == null ? new EmptyTaskSettings() : new CohereRerankTaskSettings(topN, null, null)
                    )
                )
            );
            listener.onResponse((Response) response);
        }

        @SuppressWarnings("unchecked")
        private <Response extends ActionResponse> void handleInferenceActionRequest(
            InferenceAction.Request request,
            ActionListener<Response> listener
        ) {
            Map<String, Object> taskSettings = request.getTaskSettings();
            boolean shouldThrow = (boolean) taskSettings.getOrDefault("throwing", false);
            Integer inferenceResultCount = (Integer) taskSettings.get("inferenceResultCount");

            if (shouldThrow) {
                listener.onFailure(new UnsupportedOperationException("simulated failure"));
            } else {
                List<RankedDocsResults.RankedDoc> rankedDocsResults = new ArrayList<>();
                List<String> inputs = request.getInput();
                int resultCount = inferenceResultCount == null ? inputs.size() : inferenceResultCount;
                for (int i = 0; i < resultCount; i++) {
                    rankedDocsResults.add(new RankedDocsResults.RankedDoc(i, Float.parseFloat(inputs.get(i)), inputs.get(i)));
                }
                ActionResponse response = new InferenceAction.Response(new RankedDocsResults(rankedDocsResults));
                listener.onResponse((Response) response);
            }
        }
    }

    public static class ThrowingMockRequestActionBasedRankBuilder extends TextSimilarityRankBuilder {

        protected final AbstractRerankerIT.ThrowingRankBuilderType throwingRankBuilderType;

        public ThrowingMockRequestActionBasedRankBuilder(
            int rankWindowSize,
            String field,
            String inferenceId,
            String inferenceText,
            Float minScore,
            boolean failuresAllowed,
            String throwingType,
            RerankSnippetConfig snippets
        ) {
            super(field, inferenceId, inferenceText, rankWindowSize, minScore, failuresAllowed, snippets);
            this.throwingRankBuilderType = AbstractRerankerIT.ThrowingRankBuilderType.valueOf(throwingType);
        }

        public ThrowingMockRequestActionBasedRankBuilder(StreamInput in) throws IOException {
            super(in);
            this.throwingRankBuilderType = in.readEnum(AbstractRerankerIT.ThrowingRankBuilderType.class);
        }

        @Override
        public void doWriteTo(StreamOutput out) throws IOException {
            super.doWriteTo(out);
            out.writeEnum(throwingRankBuilderType);
        }

        @Override
        public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
            if (this.throwingRankBuilderType == AbstractRerankerIT.ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT)
                return new RankFeaturePhaseRankShardContext(field()) {
                    @Override
                    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                        throw new UnsupportedOperationException("rfs - simulated failure");
                    }
                };
            else {
                return super.buildRankFeaturePhaseShardContext();
            }
        }

        @Override
        public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
            if (this.throwingRankBuilderType == AbstractRerankerIT.ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT)
                return new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
                    size,
                    from,
                    rankWindowSize(),
                    client,
                    inferenceId,
                    inferenceText,
                    minScore,
                    failuresAllowed(),
                    null
                ) {
                    @Override
                    protected InferenceAction.Request generateRequest(List<String> docFeatures) {
                        return new InferenceAction.Request(
                            TaskType.RERANK,
                            inferenceId,
                            inferenceText,
                            null,
                            null,
                            docFeatures,
                            Map.of("throwing", true),
                            InputType.INTERNAL_SEARCH,
                            InferenceAction.Request.DEFAULT_TIMEOUT,
                            false
                        );
                    }
                };
            else {
                return super.buildRankFeaturePhaseCoordinatorContext(size, from, client);
            }
        }

        @Override
        public String getWriteableName() {
            return "throwing_request_action_based_rank";
        }
    }

}

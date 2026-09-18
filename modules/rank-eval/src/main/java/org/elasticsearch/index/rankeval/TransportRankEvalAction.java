/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.createParser;
import static org.elasticsearch.index.rankeval.RatedRequest.validateEvaluatedQuery;

/**
 * Instances of this class execute a collection of search intents (read: user
 * supplied query parameters) against a set of possible search requests (read:
 * search specifications, expressed as query/search request templates) and
 * compares the result against a set of annotated documents per search intent.
 *
 * If any documents are returned that haven't been annotated the document id of
 * those is returned per search intent.
 *
 * The resulting search quality is computed in terms of precision at n and
 * returned for each search specification for the full set of search intents as
 * averaged precision at n.
 */
public class TransportRankEvalAction extends HandledTransportAction<RankEvalRequest, RankEvalResponse> {
    private final Client client;
    private final ScriptService scriptService;
    private final NamedXContentRegistry namedXContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    @Inject
    public TransportRankEvalAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        FeatureService featureService
    ) {
        super(RankEvalPlugin.ACTION.name(), transportService, actionFilters, RankEvalRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.scriptService = scriptService;
        this.namedXContentRegistry = namedXContentRegistry;
        this.clusterSupportsFeature = f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        };
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RankEvalRequest request, ActionListener<RankEvalResponse> listener) {
        RankEvalSpec evaluationSpecification = request.getRankEvalSpec();
        EvaluationMetric metric = evaluationSpecification.getMetric();

        List<RatedRequest> ratedRequests = evaluationSpecification.getRatedRequests();
        Map<String, Exception> errors = new ConcurrentHashMap<>(ratedRequests.size());

        MultiSearchRequest msearchRequest = new MultiSearchRequest();
        List<RatedRequest> ratedRequestsInSearch = new ArrayList<>();
        boolean dispatched = false;
        try {
            // Both maxConcurrentSearchRequests and script compilation are inside the guard so that
            // validation or compile failures trigger cleanup of pre-parsed REST-time sources that
            // already hold parse-time breaker charges.
            msearchRequest.maxConcurrentSearchRequests(evaluationSpecification.getMaxConcurrentSearches());
            Map<String, TemplateScript.Factory> scriptsWithoutParams = new HashMap<>();
            for (Entry<String, Script> entry : evaluationSpecification.getTemplates().entrySet()) {
                scriptsWithoutParams.put(entry.getKey(), scriptService.compile(entry.getValue(), TemplateScript.CONTEXT));
            }

            for (RatedRequest ratedRequest : ratedRequests) {
                SearchSourceBuilder evaluationRequest = ratedRequest.getEvaluationRequest();
                if (evaluationRequest == null) {
                    Map<String, Object> params = ratedRequest.getParams();
                    String templateId = ratedRequest.getTemplateId();
                    TemplateScript.Factory templateScript = scriptsWithoutParams.get(templateId);
                    String resolvedRequest = templateScript.newInstance(params).execute();
                    try (
                        XContentParser subParser = createParser(
                            namedXContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            new BytesArray(resolvedRequest),
                            XContentType.JSON
                        )
                    ) {
                        SearchSourceBuilder ssb = new SearchSourceBuilder();
                        boolean parseOk = false;
                        try {
                            ssb.parseXContent(subParser, false, clusterSupportsFeature);
                            validateEvaluatedQuery(ssb);
                            parseOk = true;
                            evaluationRequest = ssb;
                        } finally {
                            if (parseOk == false) ssb.close();
                        }
                    } catch (IOException e) {
                        // if we fail parsing, put the exception into the errors map and continue
                        errors.put(ratedRequest.getId(), e);
                        continue;
                    } catch (IllegalArgumentException e) {
                        // validateEvaluatedQuery threw; propagate so the caller sees the validation message
                        throw e;
                    }
                }

                if (metric.forcedSearchSize().isPresent()) {
                    evaluationRequest.size(metric.forcedSearchSize().getAsInt());
                }

                ratedRequestsInSearch.add(ratedRequest);
                List<String> summaryFields = ratedRequest.getSummaryFields();
                if (summaryFields.isEmpty()) {
                    evaluationRequest.fetchSource(false);
                } else {
                    evaluationRequest.fetchSource(summaryFields.toArray(new String[summaryFields.size()]), new String[0]);
                }
                SearchRequest searchRequest = new SearchRequest(request.indices(), evaluationRequest);
                searchRequest.indicesOptions(request.indicesOptions());
                searchRequest.searchType(request.searchType());
                msearchRequest.add(searchRequest);
            }
            assert ratedRequestsInSearch.size() == msearchRequest.requests().size();
            // Guard 2: close all sources on any completion (including synchronous READ-block rejection
            // where listener.onFailure fires inside client.multiSearch before it returns).
            ActionListener<MultiSearchResponse> closingListener = ActionListener.runAfter(
                new RankEvalActionListener(
                    listener,
                    metric,
                    ratedRequestsInSearch.toArray(new RatedRequest[ratedRequestsInSearch.size()]),
                    errors
                ),
                () -> msearchRequest.requests().forEach(r -> {
                    if (r.source() != null) r.source().close();
                })
            );
            client.multiSearch(msearchRequest, closingListener);
            dispatched = true;
        } finally {
            // Guard 1: if multiSearch was never dispatched (exception in loop or in multiSearch itself),
            // close all sources accumulated in msearchRequest up to the point of failure, then close any
            // pre-parsed REST-time sources from rated requests that were not yet reached in the loop.
            // SearchSourceBuilder.close() is idempotent, so double-closing is safe.
            if (dispatched == false) {
                msearchRequest.requests().forEach(r -> { if (r.source() != null) r.source().close(); });
                ratedRequests.forEach(rr -> { if (rr.getEvaluationRequest() != null) rr.getEvaluationRequest().close(); });
            }
        }
    }

    static class RankEvalActionListener extends DelegatingActionListener<MultiSearchResponse, RankEvalResponse> {

        private final RatedRequest[] specifications;

        private final Map<String, Exception> errors;
        private final EvaluationMetric metric;

        RankEvalActionListener(
            ActionListener<RankEvalResponse> listener,
            EvaluationMetric metric,
            RatedRequest[] specifications,
            Map<String, Exception> errors
        ) {
            super(listener);
            this.metric = metric;
            this.errors = errors;
            this.specifications = specifications;
        }

        @Override
        public void onResponse(MultiSearchResponse multiSearchResponse) {
            int responsePosition = 0;
            Map<String, EvalQueryQuality> responseDetails = Maps.newMapWithExpectedSize(specifications.length);
            for (Item response : multiSearchResponse.getResponses()) {
                RatedRequest specification = specifications[responsePosition];
                if (response.isFailure() == false) {
                    SearchHit[] hits = response.getResponse().getHits().getHits();
                    EvalQueryQuality queryQuality = this.metric.evaluate(specification.getId(), hits, specification.getRatedDocs());
                    responseDetails.put(specification.getId(), queryQuality);
                } else {
                    errors.put(specification.getId(), response.getFailure());
                }
                responsePosition++;
            }
            delegate.onResponse(new RankEvalResponse(this.metric.combine(responseDetails.values()), responseDetails, this.errors));
        }
    }
}

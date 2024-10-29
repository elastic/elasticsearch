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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.index.query.TermQueryBuilder;
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

        if (evaluationSpecification.hasStoredCorpus()) {

            loadRequestsFromStoredCorpus(
                evaluationSpecification.getStoredCorpus(),
                evaluationSpecification.getTemplates().keySet().iterator().next(),
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<RatedRequest> ratedRequests) {
                        if (ratedRequests.isEmpty()) {
                            listener.onFailure(new IllegalArgumentException("No rated requests found in stored corpus"));
                            return;
                        }
                        processRatedRequests(request, evaluationSpecification, List.copyOf(ratedRequests), listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                }
            );
        } else {
            processRatedRequests(request, evaluationSpecification, evaluationSpecification.getRatedRequests(), listener);
        }

    }

    private void processRatedRequests(
        RankEvalRequest request,
        RankEvalSpec evaluationSpecification,
        List<RatedRequest> ratedRequests,
        ActionListener<RankEvalResponse> listener
    ) {
        EvaluationMetric metric = evaluationSpecification.getMetric();

        Map<String, Exception> errors = new ConcurrentHashMap<>(ratedRequests.size());

        Map<String, TemplateScript.Factory> scriptsWithoutParams = new HashMap<>();
        for (Entry<String, Script> entry : evaluationSpecification.getTemplates().entrySet()) {
            scriptsWithoutParams.put(entry.getKey(), scriptService.compile(entry.getValue(), TemplateScript.CONTEXT));
        }

        MultiSearchRequest msearchRequest = new MultiSearchRequest();
        msearchRequest.maxConcurrentSearchRequests(evaluationSpecification.getMaxConcurrentSearches());
        List<RatedRequest> ratedRequestsInSearch = new ArrayList<>();
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
                    evaluationRequest = new SearchSourceBuilder().parseXContent(subParser, false, clusterSupportsFeature);
                    // check for parts that should not be part of a ranking evaluation request
                    validateEvaluatedQuery(evaluationRequest);
                } catch (IOException e) {
                    // if we fail parsing, put the exception into the errors map and continue
                    errors.put(ratedRequest.getId(), e);
                    continue;
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
        client.multiSearch(
            msearchRequest,
            new RankEvalActionListener(
                listener,
                metric,
                ratedRequestsInSearch.toArray(new RatedRequest[ratedRequestsInSearch.size()]),
                errors
            )
        );
    }

    private void loadRequestsFromStoredCorpus(String storedCorpusId, String templateId, ActionListener<List<RatedRequest>> actionListener) {
        // load the stored corpus from the index
        client.get(new GetRequest("search_relevance_dataset", storedCorpusId), new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                if (response.isExists() == false) {
                    actionListener.onFailure(new IllegalArgumentException("Stored corpus not found"));
                    return;
                }
                @SuppressWarnings("unchecked")
                StoredCorpus corpus = new StoredCorpus(
                    response.getId(),
                    (String) response.getSourceAsMap().get("name"),
                    (String) response.getSourceAsMap().get("description"),
                    response.getSourceAsMap().get("indices") instanceof List
                        ? (List<String>) response.getSourceAsMap().get("indices")
                        : List.of((String) response.getSourceAsMap().get("indices"))
                );

                // Now that we have the corpus, identify the queries and qrels
                SearchRequest queryRequest = new SearchRequestBuilder(client).setIndices("search_relevance_evaluation_corpora")
                    .setSource(new SearchSourceBuilder().query(new TermQueryBuilder("dataset_id", corpus.getId())))
                    .setSize(250) // Arbitrary number, but dbpedia has at most 164 judgments per query
                    .request();
                client.search(queryRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        List<RatedRequest> ratedRequests = new ArrayList<>();
                        List<RatedDocument> ratedDocuments = new ArrayList<>();

                        for (SearchHit hit : response.getHits().getHits()) {
                            Map<String, Object> source = hit.getSourceAsMap();
                            String queryId = (String) source.get("query_id");
                            String queryString = (String) source.get("query_string");
                            String index = (String) source.get("index");
                            String documentId = (String) source.get("document_id");
                            Integer score = (Integer) source.get("score");

                            RatedDocument ratedDocument = new RatedDocument(index, documentId, score);
                            ratedDocuments.add(ratedDocument);

                            ratedRequests.add(new RatedRequest(queryId, ratedDocuments, Map.of("query_string", queryString), templateId));
                        }
                        actionListener.onResponse(ratedRequests);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        actionListener.onFailure(e);
                    }

                });

            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
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

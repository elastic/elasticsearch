/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Instances of this class execute a collection of search intents (read: user supplied query parameters) against a set of
 * possible search requests (read: search specifications, expressed as query/search request templates) and compares the result
 * against a set of annotated documents per search intent.
 *
 * If any documents are returned that haven't been annotated the document id of those is returned per search intent.
 *
 * The resulting search quality is computed in terms of precision at n and returned for each search specification for the full
 * set of search intents as averaged precision at n.
 * */
public class TransportRankEvalAction extends HandledTransportAction<RankEvalRequest, RankEvalResponse> {
    private SearchPhaseController searchPhaseController;
    private TransportService transportService;
    private SearchTransportService searchTransportService;
    private ClusterService clusterService;
    private ActionFilters actionFilters;

    @Inject
    public TransportRankEvalAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, ScriptService scriptService,
            AutoCreateIndex autoCreateIndex, Client client, TransportService transportService, SearchPhaseController searchPhaseController,
            SearchTransportService searchTransportService, NamedWriteableRegistry namedWriteableRegistry) {
        super(settings, RankEvalAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                RankEvalRequest::new);
        this.searchPhaseController = searchPhaseController;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.actionFilters = actionFilters;

        // TODO this should maybe move to some registry on startup
        namedWriteableRegistry.register(RankedListQualityMetric.class, PrecisionAtN.NAME, PrecisionAtN::new);
    }

    @Override
    protected void doExecute(RankEvalRequest request, ActionListener<RankEvalResponse> listener) {
        RankEvalSpec qualityTask = request.getRankEvalSpec();
        RankedListQualityMetric metric = qualityTask.getEvaluator();

        double qualitySum = 0;
        Map<String, Collection<RatedDocumentKey>> unknownDocs = new HashMap<String, Collection<RatedDocumentKey>>();
        Collection<QuerySpec> specifications = qualityTask.getSpecifications();
        for (QuerySpec spec : specifications) {
            SearchSourceBuilder specRequest = spec.getTestRequest();
            String[] indices = new String[spec.getIndices().size()];
            spec.getIndices().toArray(indices);
            SearchRequest templatedRequest = new SearchRequest(indices, specRequest);
            String[] types = new String[spec.getTypes().size()];
            spec.getTypes().toArray(types);
            templatedRequest.types(types);

            TransportSearchAction transportSearchAction = new TransportSearchAction(settings, threadPool, searchPhaseController,
                    transportService, searchTransportService, clusterService, actionFilters, indexNameExpressionResolver);
            ActionFuture<SearchResponse> searchResponse = transportSearchAction.execute(templatedRequest);
            SearchHits hits = searchResponse.actionGet().getHits();

            EvalQueryQuality intentQuality = metric.evaluate(hits.getHits(), spec.getRatedDocs());
            qualitySum += intentQuality.getQualityLevel();
            unknownDocs.put(spec.getSpecId(), intentQuality.getUnknownDocs());
        }
        RankEvalResponse response = new RankEvalResponse();
        // TODO move averaging to actual metric, also add other statistics
        RankEvalResult result = new RankEvalResult(qualityTask.getTaskId(), qualitySum / specifications.size(), unknownDocs);
        response.setRankEvalResult(result);
        listener.onResponse(response);
    }
}

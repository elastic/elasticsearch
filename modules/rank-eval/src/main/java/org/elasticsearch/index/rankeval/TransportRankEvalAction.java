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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private Client client;

    @Inject
    public TransportRankEvalAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, Client client, TransportService transportService) {
        super(settings, RankEvalAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                RankEvalRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(RankEvalRequest request, ActionListener<RankEvalResponse> listener) {
        RankEvalSpec qualityTask = request.getRankEvalSpec();

        Map<String, Collection<RatedDocumentKey>> unknownDocs = new ConcurrentHashMap<>();
        Collection<RatedRequest> specifications = qualityTask.getSpecifications();
        AtomicInteger responseCounter = new AtomicInteger(specifications.size());
        Map<String, EvalQueryQuality> partialResults = new ConcurrentHashMap<>(specifications.size());

        for (RatedRequest querySpecification : specifications) {
            final RankEvalActionListener searchListener = new RankEvalActionListener(listener, qualityTask, querySpecification,
                    partialResults, unknownDocs, responseCounter);
            SearchSourceBuilder specRequest = querySpecification.getTestRequest();
            String[] indices = new String[querySpecification.getIndices().size()];
            querySpecification.getIndices().toArray(indices);
            SearchRequest templatedRequest = new SearchRequest(indices, specRequest);
            String[] types = new String[querySpecification.getTypes().size()];
            querySpecification.getTypes().toArray(types);
            templatedRequest.types(types);
            client.search(templatedRequest, searchListener);
        }
    }

    public static class RankEvalActionListener implements ActionListener<SearchResponse> {

        private ActionListener<RankEvalResponse> listener;
        private RatedRequest specification;
        private Map<String, EvalQueryQuality> partialResults;
        private RankEvalSpec task;
        private Map<String, Collection<RatedDocumentKey>> unknownDocs;
        private AtomicInteger responseCounter;

        public RankEvalActionListener(ActionListener<RankEvalResponse> listener, RankEvalSpec task, RatedRequest specification,
                Map<String, EvalQueryQuality> partialResults, Map<String, Collection<RatedDocumentKey>> unknownDocs,
                AtomicInteger responseCounter) {
            this.listener = listener;
            this.task = task;
            this.specification = specification;
            this.partialResults = partialResults;
            this.unknownDocs = unknownDocs;
            this.responseCounter = responseCounter;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            SearchHits hits = searchResponse.getHits();
            EvalQueryQuality queryQuality = task.getEvaluator().evaluate(hits.getHits(), specification.getRatedDocs());
            partialResults.put(specification.getSpecId(), queryQuality);
            unknownDocs.put(specification.getSpecId(), queryQuality.getUnknownDocs());

            if (responseCounter.decrementAndGet() < 1) {
                // TODO add other statistics like micro/macro avg?
                listener.onResponse(
                        new RankEvalResponse(task.getTaskId(), task.getEvaluator().combine(partialResults.values()), unknownDocs));
            }
        }

        @Override
        public void onFailure(Exception exception) {
            this.listener.onFailure(exception);
        }
    }
}

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

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.PipelineExecutionService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class IngestActionFilter extends AbstractComponent implements ActionFilter {

    private final PipelineExecutionService executionService;

    @Inject
    public IngestActionFilter(Settings settings, NodeService nodeService) {
        super(settings);
        this.executionService = nodeService.getIngestService().getPipelineExecutionService();
    }

    @Override
    public <Request extends ActionRequest<Request>, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        switch (action) {
            case IndexAction.NAME:
                IndexRequest indexRequest = (IndexRequest) request;
                if (Strings.hasText(indexRequest.getPipeline())) {
                    processIndexRequest(task, action, listener, chain, (IndexRequest) request);
                } else {
                    chain.proceed(task, action, request, listener);
                }
                break;
            case BulkAction.NAME:
                BulkRequest bulkRequest = (BulkRequest) request;
                if (bulkRequest.hasIndexRequestsWithPipelines()) {
                    @SuppressWarnings("unchecked")
                    ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
                    processBulkIndexRequest(task, bulkRequest, action, chain, actionListener);
                } else {
                    chain.proceed(task, action, request, listener);
                }
                break;
            default:
                chain.proceed(task, action, request, listener);
                break;
        }
    }

    @Override
    public <Response extends ActionResponse> void apply(String action, Response response, ActionListener<Response> listener, ActionFilterChain<?, Response> chain) {
        chain.proceed(action, response, listener);
    }

    void processIndexRequest(Task task, String action, ActionListener listener, ActionFilterChain chain, IndexRequest indexRequest) {

        executionService.executeIndexRequest(indexRequest, t -> {
            logger.error("failed to execute pipeline [{}]", t, indexRequest.getPipeline());
            listener.onFailure(t);
        }, success -> {
            // TransportIndexAction uses IndexRequest and same action name on the node that receives the request and the node that
            // processes the primary action. This could lead to a pipeline being executed twice for the same
            // index request, hence we set the pipeline to null once its execution completed.
            indexRequest.setPipeline(null);
            chain.proceed(task, action, indexRequest, listener);
        });
    }

    void processBulkIndexRequest(Task task, BulkRequest original, String action, ActionFilterChain chain, ActionListener<BulkResponse> listener) {
        long ingestStartTimeInNanos = System.nanoTime();
        BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
        executionService.executeBulkRequest(() -> bulkRequestModifier, (indexRequest, throwable) -> {
            logger.debug("failed to execute pipeline [{}] for document [{}/{}/{}]", throwable, indexRequest.getPipeline(), indexRequest.index(), indexRequest.type(), indexRequest.id());
            bulkRequestModifier.markCurrentItemAsFailed(throwable);
        }, (throwable) -> {
            if (throwable != null) {
                logger.error("failed to execute pipeline for a bulk request", throwable);
                listener.onFailure(throwable);
            } else {
                long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestStartTimeInNanos);
                BulkRequest bulkRequest = bulkRequestModifier.getBulkRequest();
                ActionListener<BulkResponse> actionListener = bulkRequestModifier.wrapActionListenerIfNeeded(ingestTookInMillis, listener);
                if (bulkRequest.requests().isEmpty()) {
                    // at this stage, the transport bulk action can't deal with a bulk request with no requests,
                    // so we stop and send an empty response back to the client.
                    // (this will happen if pre-processing all items in the bulk failed)
                    actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
                } else {
                    chain.proceed(task, action, bulkRequest, actionListener);
                }
            }
        });
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    final static class BulkRequestModifier implements Iterator<ActionRequest<?>> {

        final BulkRequest bulkRequest;
        final Set<Integer> failedSlots;
        final List<BulkItemResponse> itemResponses;

        int currentSlot = -1;
        int[] originalSlots;

        BulkRequestModifier(BulkRequest bulkRequest) {
            this.bulkRequest = bulkRequest;
            this.failedSlots = new HashSet<>();
            this.itemResponses = new ArrayList<>(bulkRequest.requests().size());
        }

        @Override
        public ActionRequest next() {
            return bulkRequest.requests().get(++currentSlot);
        }

        @Override
        public boolean hasNext() {
            return (currentSlot + 1) < bulkRequest.requests().size();
        }

        BulkRequest getBulkRequest() {
            if (itemResponses.isEmpty()) {
                return bulkRequest;
            } else {
                BulkRequest modifiedBulkRequest = new BulkRequest();
                modifiedBulkRequest.setRefreshPolicy(bulkRequest.getRefreshPolicy());
                modifiedBulkRequest.consistencyLevel(bulkRequest.consistencyLevel());
                modifiedBulkRequest.timeout(bulkRequest.timeout());

                int slot = 0;
                originalSlots = new int[bulkRequest.requests().size() - failedSlots.size()];
                for (int i = 0; i < bulkRequest.requests().size(); i++) {
                    ActionRequest request = bulkRequest.requests().get(i);
                    if (failedSlots.contains(i) == false) {
                        modifiedBulkRequest.add(request);
                        originalSlots[slot++] = i;
                    }
                }
                return modifiedBulkRequest;
            }
        }

        ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
            if (itemResponses.isEmpty()) {
                return new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        actionListener.onResponse(new BulkResponse(response.getItems(), response.getTookInMillis(), ingestTookInMillis));
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        actionListener.onFailure(e);
                    }
                };
            } else {
                return new IngestBulkResponseListener(ingestTookInMillis, originalSlots, itemResponses, actionListener);
            }
        }

        void markCurrentItemAsFailed(Throwable e) {
            IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(currentSlot);
            // We hit a error during preprocessing a request, so we:
            // 1) Remember the request item slot from the bulk, so that we're done processing all requests we know what failed
            // 2) Add a bulk item failure for this request
            // 3) Continue with the next request in the bulk.
            failedSlots.add(currentSlot);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), e);
            itemResponses.add(new BulkItemResponse(currentSlot, indexRequest.opType().lowercase(), failure));
        }

    }

    final static class IngestBulkResponseListener implements ActionListener<BulkResponse> {

        private final long ingestTookInMillis;
        private final int[] originalSlots;
        private final List<BulkItemResponse> itemResponses;
        private final ActionListener<BulkResponse> actionListener;

        IngestBulkResponseListener(long ingestTookInMillis, int[] originalSlots, List<BulkItemResponse> itemResponses, ActionListener<BulkResponse> actionListener) {
            this.ingestTookInMillis = ingestTookInMillis;
            this.itemResponses = itemResponses;
            this.actionListener = actionListener;
            this.originalSlots = originalSlots;
        }

        @Override
        public void onResponse(BulkResponse response) {
            for (int i = 0; i < response.getItems().length; i++) {
                itemResponses.add(originalSlots[i], response.getItems()[i]);
            }
            actionListener.onResponse(new BulkResponse(itemResponses.toArray(new BulkItemResponse[itemResponses.size()]), response.getTookInMillis(), ingestTookInMillis));
        }

        @Override
        public void onFailure(Throwable e) {
            actionListener.onFailure(e);
        }
    }
}

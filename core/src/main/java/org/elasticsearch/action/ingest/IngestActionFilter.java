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
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
            case TransportShardBulkAction.ACTION_NAME:
                BulkShardRequest bulkRequest = (BulkShardRequest) request;
                if (bulkRequest.hasIndexRequestsWithPipelines()) {
                    @SuppressWarnings("unchecked")
                    ActionListener<BulkShardResponse> actionListener = (ActionListener<BulkShardResponse>) listener;
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

    void processBulkIndexRequest(Task task, BulkShardRequest original, String action, ActionFilterChain chain, ActionListener<BulkShardResponse> listener) {
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
                BulkShardRequest bulkShardRequest = bulkRequestModifier.getBulkShardRequest();
                ActionListener<BulkShardResponse> actionListener = bulkRequestModifier.wrapActionListenerIfNeeded(ingestTookInMillis, listener);
                if (bulkShardRequest.items().length == 0) {
                    // at this stage, the transport bulk action can't deal with a bulk request with no requests,
                    // so we stop and send an empty response back to the client.
                    // (this will happen if pre-processing all items in the bulk failed)
                    BulkShardResponse response = new BulkShardResponse(original.shardId(), new BulkItemResponse[0]);
                    response.setShardInfo(new ReplicationResponse.ShardInfo());
                    actionListener.onResponse(response);
                } else {
                    chain.proceed(task, action, bulkShardRequest, actionListener);
                }
            }
        });
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    final static class BulkRequestModifier implements Iterator<ActionRequest<?>> {

        final BulkShardRequest originalRequest;
        final List<BulkItemRequest> itemRequests;
        final Iterator<BulkItemRequest> iterator;

        final List<BulkItemResponse> itemResponses;
        BulkItemRequest current;

        BulkRequestModifier(BulkShardRequest originalRequest) {
            this.originalRequest = originalRequest;
            this.itemRequests = new ArrayList<>(Arrays.asList(originalRequest.items()));
            this.iterator = itemRequests.iterator();
            this.itemResponses = new ArrayList<>(originalRequest.items().length);
        }

        @Override
        public ActionRequest next() {
            current = iterator.next();
            return current.request();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        BulkShardRequest getBulkShardRequest() {
            BulkItemRequest[] items = itemRequests.toArray(new BulkItemRequest[itemRequests.size()]);
            return new BulkShardRequest(originalRequest.shardId(), originalRequest.refresh(), items);
        }

        ActionListener<BulkShardResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkShardResponse> actionListener) {
            return new IngestBulkResponseListener(ingestTookInMillis, itemResponses, actionListener);
        }

        void markCurrentItemAsFailed(Throwable e) {
            // We hit a error during preprocessing a request, so we:
            // 1) Remove its request item as we can't continue to actual do the write operation.
            iterator.remove();
            // 2) Add a bulk item failure for this request
            int id = current.id();
            IndexRequest indexRequest = (IndexRequest) current.request();
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(
                    indexRequest.index(), indexRequest.type(), indexRequest.id(), e
            );
            itemResponses.add(new BulkItemResponse(id, indexRequest.opType().lowercase(), failure));
            // 3) Continue with the next request in the bulk.
        }

    }

    final static class IngestBulkResponseListener implements ActionListener<BulkShardResponse> {

        private final long ingestTookInMillis;
        private final List<BulkItemResponse> itemResponses;
        private final ActionListener<BulkShardResponse> actionListener;

        IngestBulkResponseListener(long ingestTookInMillis, List<BulkItemResponse> itemResponses, ActionListener<BulkShardResponse> actionListener) {
            this.ingestTookInMillis = ingestTookInMillis;
            this.itemResponses = itemResponses;
            this.actionListener = actionListener;
        }

        @Override
        public void onResponse(BulkShardResponse response) {
            Collections.addAll(itemResponses, response.getResponses());
            BulkItemResponse[] itemResponses = this.itemResponses.toArray(new BulkItemResponse[this.itemResponses.size()]);
            // sorting response items is required to ensure that the response items match with how to request items were specified
            Arrays.sort(itemResponses, (a, b) -> Integer.compare(a.getItemId(), b.getItemId()));
            actionListener.onResponse(
                    new BulkShardResponse(
                            response,
                            itemResponses,
                            ingestTookInMillis
                    )
            );
        }

        @Override
        public void onFailure(Throwable e) {
            actionListener.onFailure(e);
        }
    }
}

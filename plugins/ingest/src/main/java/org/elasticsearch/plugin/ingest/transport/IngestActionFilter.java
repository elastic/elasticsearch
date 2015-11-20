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

package org.elasticsearch.plugin.ingest.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.plugin.ingest.IngestPlugin;
import org.elasticsearch.plugin.ingest.PipelineExecutionService;

import java.util.*;

public final class IngestActionFilter extends AbstractComponent implements ActionFilter {

    private final PipelineExecutionService executionService;

    @Inject
    public IngestActionFilter(Settings settings, PipelineExecutionService executionService) {
        super(settings);
        this.executionService = executionService;
    }

    @Override
    public void apply(String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        String pipelineId = request.getFromContext(IngestPlugin.PIPELINE_ID_PARAM_CONTEXT_KEY);
        if (pipelineId == null) {
            pipelineId = request.getHeader(IngestPlugin.PIPELINE_ID_PARAM);
            if (pipelineId == null) {
                chain.proceed(action, request, listener);
                return;
            }
        }

        if (request instanceof IndexRequest) {
            processIndexRequest(action, listener, chain, (IndexRequest) request, pipelineId);
        } else if (request instanceof BulkRequest) {
            BulkRequest bulkRequest = (BulkRequest) request;
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
            BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(bulkRequest);
            processBulkIndexRequest(bulkRequestModifier, pipelineId, action, chain, actionListener);
        } else {
            chain.proceed(action, request, listener);
        }
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

    void processIndexRequest(String action, ActionListener listener, ActionFilterChain chain, IndexRequest indexRequest, String pipelineId) {
        // The IndexRequest has the same type on the node that receives the request and the node that
        // processes the primary action. This could lead to a pipeline being executed twice for the same
        // index request, hence this check
        if (indexRequest.hasHeader(IngestPlugin.PIPELINE_ALREADY_PROCESSED)) {
            chain.proceed(action, indexRequest, listener);
            return;
        }

        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        executionService.execute(ingestDocument, pipelineId, new PipelineExecutionService.Listener() {
            @Override
            public void executed(IngestDocument ingestDocument) {
                if (ingestDocument.isModified()) {
                    indexRequest.source(ingestDocument.getSource());
                }
                indexRequest.putHeader(IngestPlugin.PIPELINE_ALREADY_PROCESSED, true);
                chain.proceed(action, indexRequest, listener);
            }

            @Override
            public void failed(Throwable e) {
                logger.error("failed to execute pipeline [{}]", e, pipelineId);
                listener.onFailure(e);
            }
        });
    }

    void processBulkIndexRequest(BulkRequestModifier bulkRequestModifier, String pipelineId, String action, ActionFilterChain chain, ActionListener<BulkResponse> listener) {
        if (!bulkRequestModifier.hasNext()) {
            BulkRequest bulkRequest = bulkRequestModifier.getBulkRequest();
            ActionListener<BulkResponse> actionListener = bulkRequestModifier.wrapActionListenerIfNeeded(listener);
            if (bulkRequest.requests().isEmpty()) {
                // in this stage, the transport bulk action can't deal with a bulk request with no requests,
                // so we stop and send a empty response back to the client.
                // (this will happen if all preprocessing all items in the bulk failed)
                actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
            } else {
                chain.proceed(action, bulkRequest, actionListener);
            }
            return;
        }

        ActionRequest actionRequest = bulkRequestModifier.next();
        if (!(actionRequest instanceof IndexRequest)) {
            processBulkIndexRequest(bulkRequestModifier, pipelineId, action, chain, listener);
            return;
        }

        IndexRequest indexRequest = (IndexRequest) actionRequest;
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(indexRequest.index(), indexRequest.type(), indexRequest.id(), sourceAsMap);
        executionService.execute(ingestDocument, pipelineId, new PipelineExecutionService.Listener() {
            @Override
            public void executed(IngestDocument ingestDocument) {
                if (ingestDocument.isModified()) {
                    indexRequest.source(ingestDocument.getSource());
                }
                processBulkIndexRequest(bulkRequestModifier, pipelineId, action, chain, listener);
            }

            @Override
            public void failed(Throwable e) {
                logger.debug("failed to execute pipeline [{}]", e, pipelineId);
                bulkRequestModifier.markCurrentItemAsFailed(e);
                processBulkIndexRequest(bulkRequestModifier, pipelineId, action, chain, listener);
            }
        });
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    final static class BulkRequestModifier implements Iterator<ActionRequest> {

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
                BulkRequest modifiedBulkRequest = new BulkRequest(bulkRequest);
                modifiedBulkRequest.refresh(bulkRequest.refresh());
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

        ActionListener<BulkResponse> wrapActionListenerIfNeeded(ActionListener<BulkResponse> actionListener) {
            if (itemResponses.isEmpty()) {
                return actionListener;
            } else {
                return new IngestBulkResponseListener(originalSlots, itemResponses, actionListener);
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

    private final static class IngestBulkResponseListener implements ActionListener<BulkResponse> {

        private final int[] originalSlots;
        private final List<BulkItemResponse> itemResponses;
        private final ActionListener<BulkResponse> actionListener;

        IngestBulkResponseListener(int[] originalSlots, List<BulkItemResponse> itemResponses, ActionListener<BulkResponse> actionListener) {
            this.itemResponses = itemResponses;
            this.actionListener = actionListener;
            this.originalSlots = originalSlots;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            for (int i = 0; i < bulkItemResponses.getItems().length; i++) {
                itemResponses.add(originalSlots[i], bulkItemResponses.getItems()[i]);
            }
            actionListener.onResponse(new BulkResponse(itemResponses.toArray(new BulkItemResponse[itemResponses.size()]), bulkItemResponses.getTookInMillis()));
        }

        @Override
        public void onFailure(Throwable e) {
            actionListener.onFailure(e);
        }
    }
}

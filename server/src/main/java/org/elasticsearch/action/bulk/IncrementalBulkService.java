/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IncrementalBulkService {

    private final Client client;
    private final IndexingPressure indexingPressure;

    public IncrementalBulkService(Client client, IndexingPressure indexingPressure) {
        this.client = client;
        this.indexingPressure = indexingPressure;
    }

    public Handler newBulkRequest() {
        return newBulkRequest(null, null, null);
    }

    public Handler newBulkRequest(@Nullable String waitForActiveShards, @Nullable TimeValue timeout, @Nullable String refresh) {
        return new Handler(client, indexingPressure, waitForActiveShards, timeout, refresh);
    }

    public static class Handler {

        public static final BulkRequest.IncrementalState EMPTY_STATE = new BulkRequest.IncrementalState(Collections.emptyMap(), true);

        private final Client client;
        private final IndexingPressure indexingPressure;
        private final ActiveShardCount waitForActiveShards;
        private final TimeValue timeout;
        private final String refresh;

        private final ArrayList<Releasable> releasables = new ArrayList<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);
        private boolean globalFailure = false;
        private boolean incrementalRequestSubmitted = false;
        private Exception bulkActionLevelFailure = null;
        private BulkRequest bulkRequest = null;

        private Handler(
            Client client,
            IndexingPressure indexingPressure,
            @Nullable String waitForActiveShards,
            @Nullable TimeValue timeout,
            @Nullable String refresh
        ) {
            this.client = client;
            this.indexingPressure = indexingPressure;
            this.waitForActiveShards = waitForActiveShards != null ? ActiveShardCount.parseString(waitForActiveShards) : null;
            this.timeout = timeout;
            this.refresh = refresh;
            createNewBulkRequest(EMPTY_STATE);
        }

        public void addItems(List<DocWriteRequest<?>> items, Releasable releasable, Runnable nextItems) {
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                nextItems.run();
            } else {
                assert bulkRequest != null;
                if (internalAddItems(items, releasable)) {
                    if (shouldBackOff()) {
                        final boolean isFirstRequest = incrementalRequestSubmitted == false;
                        incrementalRequestSubmitted = true;

                        client.bulk(bulkRequest, ActionListener.runAfter(new ActionListener<>() {

                            @Override
                            public void onResponse(BulkResponse bulkResponse) {
                                responses.add(bulkResponse);
                                releaseCurrentReferences();
                                createNewBulkRequest(
                                    new BulkRequest.IncrementalState(bulkResponse.getIncrementalState().shardLevelFailures(), true)
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handleBulkFailure(isFirstRequest, e);
                            }
                        }, nextItems));
                    } else {
                        nextItems.run();
                    }
                } else {
                    nextItems.run();
                }

            }
        }

        private boolean shouldBackOff() {
            return indexingPressure.shouldSplitBulks();
        }

        public void lastItems(List<DocWriteRequest<?>> items, Releasable releasable, ActionListener<BulkResponse> listener) {
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                errorResponse(listener);
            } else {
                assert bulkRequest != null;
                if (internalAddItems(items, releasable)) {
                    client.bulk(bulkRequest, new ActionListener<>() {

                        private final boolean isFirstRequest = incrementalRequestSubmitted == false;

                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            responses.add(bulkResponse);
                            releaseCurrentReferences();
                            listener.onResponse(combineResponses());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handleBulkFailure(isFirstRequest, e);
                            errorResponse(listener);
                        }
                    });
                } else {
                    errorResponse(listener);
                }
            }
        }

        private void shortCircuitDueToTopLevelFailure(List<DocWriteRequest<?>> items, Releasable releasable) {
            assert releasables.isEmpty();
            assert bulkRequest == null;
            if (globalFailure == false) {
                addItemLevelFailures(items);
            }
            Releasables.close(releasable);
        }

        private void errorResponse(ActionListener<BulkResponse> listener) {
            if (globalFailure) {
                listener.onFailure(bulkActionLevelFailure);
            } else {
                listener.onResponse(combineResponses());
            }
        }

        private void handleBulkFailure(boolean isFirstRequest, Exception e) {
            assert bulkActionLevelFailure == null;
            globalFailure = isFirstRequest;
            bulkActionLevelFailure = e;
            addItemLevelFailures(bulkRequest.requests());
            releaseCurrentReferences();
        }

        private void addItemLevelFailures(List<DocWriteRequest<?>> items) {
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[items.size()];
            int idx = 0;
            for (DocWriteRequest<?> item : items) {
                BulkItemResponse.Failure failure = new BulkItemResponse.Failure(item.index(), item.id(), bulkActionLevelFailure);
                bulkItemResponses[idx++] = BulkItemResponse.failure(idx, item.opType(), failure);
            }

            responses.add(new BulkResponse(bulkItemResponses, 0, 0));
        }

        private boolean internalAddItems(List<DocWriteRequest<?>> items, Releasable releasable) {
            try {
                bulkRequest.add(items);
                releasables.add(releasable);
                releasables.add(
                    indexingPressure.markCoordinatingOperationStarted(
                        items.size(),
                        items.stream().mapToLong(Accountable::ramBytesUsed).sum(),
                        false
                    )
                );
                return true;
            } catch (EsRejectedExecutionException e) {
                handleBulkFailure(incrementalRequestSubmitted == false, e);
                return false;
            }
        }

        private void createNewBulkRequest(BulkRequest.IncrementalState incrementalState) {
            bulkRequest = new BulkRequest();
            bulkRequest.incrementalState(incrementalState);

            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(waitForActiveShards);
            }
            if (timeout != null) {
                bulkRequest.timeout(timeout);
            }
            if (refresh != null) {
                bulkRequest.setRefreshPolicy(refresh);
            }
        }

        private void releaseCurrentReferences() {
            bulkRequest = null;
            releasables.forEach(Releasable::close);
            releasables.clear();
        }

        private BulkResponse combineResponses() {
            long tookInMillis = 0;
            long ingestTookInMillis = 0;
            int itemResponseCount = 0;
            for (BulkResponse response : responses) {
                tookInMillis += response.getTookInMillis();
                ingestTookInMillis += response.getIngestTookInMillis();
                itemResponseCount += response.getItems().length;
            }
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[itemResponseCount];
            int i = 0;
            for (BulkResponse response : responses) {
                for (BulkItemResponse itemResponse : response.getItems()) {
                    bulkItemResponses[i++] = itemResponse;
                }
            }

            return new BulkResponse(bulkItemResponses, tookInMillis, ingestTookInMillis);
        }
    }
}

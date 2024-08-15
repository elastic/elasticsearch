/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;

public class IncrementalBulkService {

    private final Client client;

    public IncrementalBulkService(Client client) {
        this.client = client;
    }

    public Handler newBulkRequest() {
        return newBulkRequest(null, null, null);
    }

    public Handler newBulkRequest(@Nullable String waitForActiveShards, @Nullable TimeValue timeout, @Nullable String refresh) {
        return new Handler(client, waitForActiveShards, timeout, refresh);
    }

    public static class Handler {

        private final Client client;
        private final ActiveShardCount waitForActiveShards;
        private final TimeValue timeout;
        private final String refresh;

        private final ArrayList<Releasable> releasables = new ArrayList<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);
        private boolean globalFailure = false;
        private boolean incrementalRequestSubmitted = false;
        private Exception topLevelFailure = null;
        private BulkRequest bulkRequest = null;

        private Handler(Client client, @Nullable String waitForActiveShards, @Nullable TimeValue timeout, @Nullable String refresh) {
            this.client = client;
            this.waitForActiveShards = waitForActiveShards != null ? ActiveShardCount.parseString(waitForActiveShards) : null;
            this.timeout = timeout;
            this.refresh = refresh;
            createNewBulkRequest(BulkRequest.IncrementalState.EMPTY);
        }

        public void addItems(List<DocWriteRequest<?>> items, Releasable releasable, Runnable nextItems) {
            if (topLevelFailure != null) {
                assert releasables.isEmpty();
                assert bulkRequest == null;
                if (globalFailure == false) {
                    addItemLevelFailures(items);
                }
                Releasables.close(releasable);
                nextItems.run();
            } else {
                internalAddItems(items, releasable);

                if (shouldBackOff()) {
                    client.bulk(bulkRequest, ActionListener.runAfter(new ActionListener<>() {

                        private final boolean isFirstRequest = incrementalRequestSubmitted == false;

                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            responses.add(bulkResponse);
                            releaseCurrentReferences();
                            createNewBulkRequest(bulkResponse.getIncrementalState());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handleBulkFailure(isFirstRequest, e);
                        }
                    }, nextItems));
                    incrementalRequestSubmitted = true;
                } else {
                    nextItems.run();
                }
            }
        }

        private boolean shouldBackOff() {
            // TODO: Implement Real Memory Logic
            return bulkRequest.requests().size() > 16;
        }

        public void lastItems(List<DocWriteRequest<?>> items, Releasable releasable, ActionListener<BulkResponse> listener) {
            if (globalFailure) {
                assert releasables.isEmpty();
                assert bulkRequest == null;
                releasable.close();
                listener.onFailure(topLevelFailure);
            } else {
                internalAddItems(items, releasable);

                client.bulk(bulkRequest, ActionListener.runAfter(new ActionListener<>() {

                    private final boolean isFirstRequest = incrementalRequestSubmitted == false;

                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        responses.add(bulkResponse);
                        releaseCurrentReferences();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handleBulkFailure(isFirstRequest, e);
                    }
                }, () -> listener.onResponse(combineResponses())));
            }
        }

        private void handleBulkFailure(boolean isFirstRequest, Exception e) {
            assert topLevelFailure == null;
            globalFailure = isFirstRequest;
            topLevelFailure = e;
            addItemLevelFailures(bulkRequest.requests());
            releaseCurrentReferences();
            bulkRequest = null;
        }

        private void addItemLevelFailures(List<DocWriteRequest<?>> items) {
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[items.size()];
            int idx = 0;
            for (DocWriteRequest<?> item : items) {
                BulkItemResponse.Failure failure = new BulkItemResponse.Failure(item.index(), item.id(), topLevelFailure);
                bulkItemResponses[idx++] = BulkItemResponse.failure(idx, item.opType(), failure);
            }

            responses.add(new BulkResponse(bulkItemResponses, 0, 0));
        }

        private void internalAddItems(List<DocWriteRequest<?>> items, Releasable releasable) {
            bulkRequest.add(items);
            releasables.add(releasable);
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

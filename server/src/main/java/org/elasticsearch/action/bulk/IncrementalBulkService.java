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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.document.RestBulkAction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class IncrementalBulkService {

    private final Client client;
    private final ThreadContext threadContext;
    private final Enabled enabled;

    public IncrementalBulkService(Client client, ThreadContext threadContext) {
        this.client = client;
        this.threadContext = threadContext;
        this.enabled = new Enabled();
    }

    public IncrementalBulkService(Client client, ThreadContext threadContext, ClusterSettings clusterSettings) {
        this.client = client;
        this.threadContext = threadContext;
        this.enabled = new Enabled(clusterSettings);
    }

    public boolean incrementalBulkEnabled() {
        return enabled.incrementalBulkEnabled();
    }

    public Handler newBulkRequest() {
        return newBulkRequest(null, null, null);
    }

    public Handler newBulkRequest(@Nullable String waitForActiveShards, @Nullable TimeValue timeout, @Nullable String refresh) {
        return new Handler(client, threadContext, threadContext.newStoredContext(), waitForActiveShards, timeout, refresh);
    }

    public static class Enabled {

        private final AtomicBoolean incrementalBulksEnabled = new AtomicBoolean(true);

        public Enabled() {}

        public Enabled(ClusterSettings clusterSettings) {
            incrementalBulksEnabled.set(clusterSettings.get(RestBulkAction.INCREMENTAL_BULK));
            clusterSettings.addSettingsUpdateConsumer(RestBulkAction.INCREMENTAL_BULK, incrementalBulksEnabled::set);
        }

        public boolean incrementalBulkEnabled() {
            return incrementalBulksEnabled.get();
        }
    }

    public static class Handler implements Releasable {

        private final Client client;
        private final ThreadContext threadContext;
        private final ThreadContext.StoredContext requestContext;
        private final ActiveShardCount waitForActiveShards;
        private final TimeValue timeout;
        private final String refresh;

        private final ArrayList<Releasable> releasables = new ArrayList<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);
        private boolean globalFailure = false;
        private boolean incrementalRequestSubmitted = false;
        private Exception bulkActionLevelFailure = null;
        private BulkRequest bulkRequest = null;

        protected Handler(
            Client client,
            ThreadContext threadContext,
            ThreadContext.StoredContext requestContext,
            @Nullable String waitForActiveShards,
            @Nullable TimeValue timeout,
            @Nullable String refresh
        ) {
            this.client = client;
            this.threadContext = threadContext;
            this.requestContext = requestContext;
            this.waitForActiveShards = waitForActiveShards != null ? ActiveShardCount.parseString(waitForActiveShards) : null;
            this.timeout = timeout;
            this.refresh = refresh;
            createNewBulkRequest(BulkRequest.IncrementalState.EMPTY);
        }

        public void addItems(List<DocWriteRequest<?>> items, Releasable releasable, Runnable nextItems) {
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                nextItems.run();
            } else {
                assert bulkRequest != null;
                internalAddItems(items, releasable);

                if (shouldBackOff()) {
                    final boolean isFirstRequest = incrementalRequestSubmitted == false;
                    incrementalRequestSubmitted = true;

                    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                        requestContext.restore();
                        client.bulk(bulkRequest, ActionListener.runAfter(new ActionListener<>() {

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
                    }
                } else {
                    nextItems.run();
                }
            }
        }

        private boolean shouldBackOff() {
            // TODO: Implement Real Memory Logic
            return bulkRequest.requests().size() >= 16;
        }

        public void lastItems(List<DocWriteRequest<?>> items, Releasable releasable, ActionListener<BulkResponse> listener) {
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                errorResponse(listener);
            } else {
                assert bulkRequest != null;
                internalAddItems(items, releasable);

                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    requestContext.restore();
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

        @Override
        public void close() {
            // TODO: Implement
        }
    }
}

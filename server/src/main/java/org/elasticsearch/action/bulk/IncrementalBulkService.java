/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.boolSetting;

public class IncrementalBulkService {

    public static final Setting<Boolean> INCREMENTAL_BULK = boolSetting(
        "rest.incremental_bulk",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final Client client;
    private final AtomicBoolean enabledForTests = new AtomicBoolean(true);
    private final IndexingPressure indexingPressure;
    private final ThreadContext threadContext;

    public IncrementalBulkService(Client client, IndexingPressure indexingPressure, ThreadContext threadContext) {
        this.client = client;
        this.indexingPressure = indexingPressure;
        this.threadContext = threadContext;
    }

    public Handler newBulkRequest() {
        ensureEnabled();
        return newBulkRequest(null, null, null);
    }

    public Handler newBulkRequest(@Nullable String waitForActiveShards, @Nullable TimeValue timeout, @Nullable String refresh) {
        ensureEnabled();
        return new Handler(client, threadContext, indexingPressure, waitForActiveShards, timeout, refresh);
    }

    private void ensureEnabled() {
        if (enabledForTests.get() == false) {
            throw new AssertionError("Unexpected incremental bulk request");
        }
    }

    // This method only exists to tests that the feature flag works. Remove once we no longer need the flag.
    public void setForTests(boolean value) {
        enabledForTests.set(value);
    }

    public static class Enabled implements Supplier<Boolean> {

        private final AtomicBoolean incrementalBulksEnabled = new AtomicBoolean(true);

        public Enabled() {}

        public Enabled(ClusterSettings clusterSettings) {
            incrementalBulksEnabled.set(clusterSettings.get(INCREMENTAL_BULK));
            clusterSettings.addSettingsUpdateConsumer(INCREMENTAL_BULK, incrementalBulksEnabled::set);
        }

        @Override
        public Boolean get() {
            return incrementalBulksEnabled.get();
        }
    }

    public static class Handler implements Releasable {

        public static final BulkRequest.IncrementalState EMPTY_STATE = new BulkRequest.IncrementalState(Collections.emptyMap(), true);

        private final Client client;
        private final ThreadContext threadContext;
        private final IndexingPressure indexingPressure;
        private final ActiveShardCount waitForActiveShards;
        private final TimeValue timeout;
        private final String refresh;

        private final ArrayList<Releasable> releasables = new ArrayList<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);
        private boolean closed = false;
        private boolean globalFailure = false;
        private boolean incrementalRequestSubmitted = false;
        private boolean bulkInProgress = false;
        private ThreadContext.StoredContext requestContext;
        private Exception bulkActionLevelFailure = null;
        private long currentBulkSize = 0L;
        private BulkRequest bulkRequest = null;

        protected Handler(
            Client client,
            ThreadContext threadContext,
            IndexingPressure indexingPressure,
            @Nullable String waitForActiveShards,
            @Nullable TimeValue timeout,
            @Nullable String refresh
        ) {
            this.client = client;
            this.threadContext = threadContext;
            this.requestContext = threadContext.newStoredContext();
            this.indexingPressure = indexingPressure;
            this.waitForActiveShards = waitForActiveShards != null ? ActiveShardCount.parseString(waitForActiveShards) : null;
            this.timeout = timeout;
            this.refresh = refresh;
            createNewBulkRequest(EMPTY_STATE);
        }

        public void addItems(List<DocWriteRequest<?>> items, Releasable releasable, Runnable nextItems) {
            assert closed == false;
            assert bulkInProgress == false;
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                nextItems.run();
            } else {
                assert bulkRequest != null;
                if (internalAddItems(items, releasable)) {
                    if (shouldBackOff()) {
                        final boolean isFirstRequest = incrementalRequestSubmitted == false;
                        incrementalRequestSubmitted = true;
                        try (var ignored = threadContext.restoreExistingContext(requestContext)) {
                            final ArrayList<Releasable> toRelease = new ArrayList<>(releasables);
                            releasables.clear();
                            bulkInProgress = true;
                            client.bulk(bulkRequest, ActionListener.runAfter(new ActionListener<>() {

                                @Override
                                public void onResponse(BulkResponse bulkResponse) {
                                    handleBulkSuccess(bulkResponse);
                                    createNewBulkRequest(
                                        new BulkRequest.IncrementalState(bulkResponse.getIncrementalState().shardLevelFailures(), true)
                                    );
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    handleBulkFailure(isFirstRequest, e);
                                }
                            }, () -> {
                                bulkInProgress = false;
                                requestContext = threadContext.newStoredContext();
                                toRelease.forEach(Releasable::close);
                                nextItems.run();
                            }));
                        }
                    } else {
                        nextItems.run();
                    }
                } else {
                    nextItems.run();
                }
            }
        }

        private boolean shouldBackOff() {
            return indexingPressure.shouldSplitBulk(currentBulkSize);
        }

        public void lastItems(List<DocWriteRequest<?>> items, Releasable releasable, ActionListener<BulkResponse> listener) {
            assert bulkInProgress == false;
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                errorResponse(listener);
            } else {
                assert bulkRequest != null;
                if (internalAddItems(items, releasable)) {
                    try (var ignored = threadContext.restoreExistingContext(requestContext)) {
                        final ArrayList<Releasable> toRelease = new ArrayList<>(releasables);
                        releasables.clear();
                        // We do not need to set this back to false as this will be the last request.
                        bulkInProgress = true;
                        client.bulk(bulkRequest, ActionListener.runBefore(new ActionListener<>() {

                            private final boolean isFirstRequest = incrementalRequestSubmitted == false;

                            @Override
                            public void onResponse(BulkResponse bulkResponse) {
                                handleBulkSuccess(bulkResponse);
                                listener.onResponse(combineResponses());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handleBulkFailure(isFirstRequest, e);
                                errorResponse(listener);
                            }
                        }, () -> toRelease.forEach(Releasable::close)));
                    }
                } else {
                    errorResponse(listener);
                }
            }
        }

        @Override
        public void close() {
            closed = true;
            releasables.forEach(Releasable::close);
            releasables.clear();
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

        private void handleBulkSuccess(BulkResponse bulkResponse) {
            responses.add(bulkResponse);
            currentBulkSize = 0L;
            bulkRequest = null;
        }

        private void handleBulkFailure(boolean isFirstRequest, Exception e) {
            assert bulkActionLevelFailure == null;
            globalFailure = isFirstRequest;
            bulkActionLevelFailure = e;
            addItemLevelFailures(bulkRequest.requests());
            currentBulkSize = 0;
            bulkRequest = null;
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
                long size = items.stream().mapToLong(Accountable::ramBytesUsed).sum();
                releasables.add(indexingPressure.markCoordinatingOperationStarted(items.size(), size, false));
                currentBulkSize += size;
                return true;
            } catch (EsRejectedExecutionException e) {
                handleBulkFailure(incrementalRequestSubmitted == false, e);
                releasables.forEach(Releasable::close);
                releasables.clear();
                return false;
            }
        }

        private void createNewBulkRequest(BulkRequest.IncrementalState incrementalState) {
            assert currentBulkSize == 0L;
            assert bulkRequest == null;
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

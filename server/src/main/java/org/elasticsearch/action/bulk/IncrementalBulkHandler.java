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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;

public class IncrementalBulkHandler {

    private final Client client;

    public IncrementalBulkHandler(Client client) {
        this.client = client;
    }

    public Handler newBulkRequest(String waitForActiveShards, TimeValue timeout, String refresh) {
        return new Handler(waitForActiveShards, timeout, refresh);
    }

    public class Handler {

        private final String waitForActiveShards;
        private final TimeValue timeout;
        private final String refresh;

        private final ArrayList<Releasable> releasables = new ArrayList<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);
        private Exception failure = null;
        private BulkRequest bulkRequest = null;

        private Handler(String waitForActiveShards, TimeValue timeout, String refresh) {
            this.waitForActiveShards = waitForActiveShards;
            this.timeout = timeout;
            this.refresh = refresh;
            createNewBulkRequest();
        }

        public void addItems(List<DocWriteRequest<?>> items, Releasable releasable, Runnable nextItems) {
            if (failure != null) {
                // TODO: Memory accounting
                bulkRequest.add(items);
                releasables.add(releasable);

                boolean backoff = false;
                if (backoff) {
                    client.bulk(bulkRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            handleIndividualResponse(bulkResponse);
                            nextItems.run();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            releasables.forEach(Releasable::close);
                            releasables.clear();
                            failure = e;
                        }
                    });
                } else {
                    nextItems.run();
                }
            } else {
                assert releasables.isEmpty();
                assert bulkRequest == null;
                releasable.close();
                nextItems.run();
            }
        }

        public void lastItems(List<DocWriteRequest<?>> items, Releasable releasable, ActionListener<BulkResponse> listener) {
            bulkRequest.add(items);
            releasables.add(releasable);
            client.bulk(bulkRequest, listener.delegateFailureAndWrap((l, bulkResponse) -> {
                handleIndividualResponse(bulkResponse);
                l.onResponse(combineResponses());
            }));
        }

        private void createNewBulkRequest() {
            assert bulkRequest == null;
            bulkRequest = new BulkRequest();

            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
            }
            bulkRequest.timeout(timeout);
            bulkRequest.setRefreshPolicy(refresh);
        }

        private void handleIndividualResponse(BulkResponse bulkResponse) {
            responses.add(bulkResponse);
            releasables.forEach(Releasable::close);
            releasables.clear();
            createNewBulkRequest();
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

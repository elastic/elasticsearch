/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.Semaphore;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, or based on the size), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * <p/>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor {

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final Client client;
        private final ActionListener<BulkResponse> listener;

        private int concurrentRequests = 1;
        private int bulkActions = 1000;
        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);

        /**
         * Creates a builder of bulk processor with the client to use and the listener that will be used
         * to be notified on the completion of bulk requests.
         */
        public Builder(Client client, ActionListener<BulkResponse> listener) {
            this.client = client;
            this.listener = listener;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single
         * request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to <tt>1</tt>.
         */
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions currently added. Defaults to
         * <tt>1000</tt>. Can be set to <tt>-1</tt> to disable it.
         */
        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * <tt>5mb</tt>. Can be set to <tt>-1</tt> to disable it.
         */
        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public BulkProcessor build() {
            return new BulkProcessor(client, listener, concurrentRequests, bulkActions, bulkSize);
        }
    }

    public static Builder builder(Client client, ActionListener<BulkResponse> listener) {
        return new Builder(client, listener);
    }

    private final Client client;
    private final ActionListener<BulkResponse> listener;

    private int concurrentRequests;
    private final int bulkActions;
    private final int bulkSize;

    private final Semaphore semaphore;

    private BulkRequest bulkRequest;

    BulkProcessor(Client client, ActionListener<BulkResponse> listener, int concurrentRequests, int bulkActions, ByteSizeValue bulkSize) {
        this.client = client;
        this.listener = listener;
        this.concurrentRequests = concurrentRequests;
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.bytesAsInt();

        this.semaphore = new Semaphore(concurrentRequests);
        this.bulkRequest = new BulkRequest();
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor add(IndexRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkProcessor add(DeleteRequest request) {
        return add((ActionRequest) request);
    }

    public BulkProcessor add(ActionRequest request) {
        internalAdd(request);
        return this;
    }

    private synchronized void internalAdd(ActionRequest request) {
        bulkRequest.add(request);
        executeIfNeeded();
    }

    public synchronized BulkProcessor add(BytesReference data, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        bulkRequest.add(data, contentUnsafe, defaultIndex, defaultType);
        executeIfNeeded();
        return this;
    }

    private void executeIfNeeded() {
        if (!isOverTheLimit()) {
            return;
        }
        if (concurrentRequests == 0) {
            // execute in a blocking fashion...
            try {
                listener.onResponse(client.bulk(bulkRequest).actionGet());
            } catch (Exception e) {
                listener.onFailure(e);
            }
        } else {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                listener.onFailure(e);
                return;
            }
            client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse response) {
                    try {
                        listener.onResponse(response);
                    } finally {
                        semaphore.release();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        listener.onFailure(e);
                    } finally {
                        semaphore.release();
                    }
                }
            });
        }
        bulkRequest = new BulkRequest();
    }

    private boolean isOverTheLimit() {
        if (bulkActions != -1 && bulkRequest.numberOfActions() > bulkActions) {
            return true;
        }
        if (bulkSize != -1 && bulkRequest.estimatedSizeInBytes() > bulkSize) {
            return true;
        }
        return false;
    }
}

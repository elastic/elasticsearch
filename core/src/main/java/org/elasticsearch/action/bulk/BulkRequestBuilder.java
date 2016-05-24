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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A bulk request holds an ordered {@link IndexRequest}s and {@link DeleteRequest}s and allows to executes
 * it in a single batch.
 */
public class BulkRequestBuilder extends ActionRequestBuilder<BulkRequest, BulkResponse, BulkRequestBuilder>
        implements WriteRequestBuilder<BulkRequestBuilder> {

    public BulkRequestBuilder(ElasticsearchClient client, BulkAction action) {
        super(client, action, new BulkRequest());
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequestBuilder add(IndexRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequestBuilder add(IndexRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(DeleteRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(DeleteRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }


    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(UpdateRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(UpdateRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequestBuilder add(byte[] data, int from, int length) throws Exception {
        request.add(data, from, length, null, null);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        request.add(data, from, length, defaultIndex, defaultType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public BulkRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final BulkRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final BulkRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }
}

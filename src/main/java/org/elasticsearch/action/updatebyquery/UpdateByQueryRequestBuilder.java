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

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * A request builder that produces {@link IndexUpdateByQueryRequest} instances.
 */
public class UpdateByQueryRequestBuilder extends BaseRequestBuilder<UpdateByQueryRequest, UpdateByQueryResponse> {

    private UpdateByQuerySourceBuilder sourceBuilder;

    public UpdateByQueryRequestBuilder(Client client) {
        super(client, new UpdateByQueryRequest());
    }

    public UpdateByQueryRequestBuilder setTypes(String... types) {
        request().types(types);
        return this;
    }

    public UpdateByQueryRequestBuilder setIndices(String... indices) {
        request().indices(indices);
        return this;
    }

    public UpdateByQueryRequestBuilder setTimeout(TimeValue timeout) {
        request().timeout(timeout);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public UpdateByQueryRequestBuilder setListenerThreaded(boolean listenerThreaded) {
        request().listenerThreaded(listenerThreaded);
        return this;
    }

    public UpdateByQueryRequestBuilder setIncludeBulkResponses(BulkResponseOption option) {
        request().bulkResponseOptions(option);
        return this;
    }

    public UpdateByQueryRequestBuilder setReplicationType(ReplicationType replicationType) {
        request().replicationType(replicationType);
        return this;
    }

    public UpdateByQueryRequestBuilder setConsistencyLevel(WriteConsistencyLevel writeConsistencyLevel) {
        request().consistencyLevel(writeConsistencyLevel);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public UpdateByQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     */
    public UpdateByQueryRequestBuilder setQuery(BytesReference query) {
        sourceBuilder().query(query);
        return this;
    }

    public UpdateByQueryRequestBuilder setScriptLang(String lang) {
        sourceBuilder().scriptLang(lang);
        return this;
    }

    public UpdateByQueryRequestBuilder setScript(String script) {
        sourceBuilder().script(script);
        return this;
    }

    public UpdateByQueryRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        if (scriptParams != null) {
            sourceBuilder().scriptParams(scriptParams);
        }
        return this;
    }

    public UpdateByQueryRequestBuilder addScriptParam(String name, String value) {
        sourceBuilder().addScriptParam(name, value);
        return this;
    }

    protected void doExecute(ActionListener<UpdateByQueryResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }

        client.updateByQuery(request, listener);
    }

    private UpdateByQuerySourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new UpdateByQuerySourceBuilder();
        }
        return sourceBuilder;
    }

}

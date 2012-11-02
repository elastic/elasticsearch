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

package org.elasticsearch.action.admin.indices.custommeta.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 *
 */
public class PutCustomMetaRequestBuilder extends MasterNodeOperationRequestBuilder<PutCustomMetaRequest, PutCustomMetaResponse, PutCustomMetaRequestBuilder> {

    public PutCustomMetaRequestBuilder(IndicesAdminClient indicesClient, String name) {
        super((InternalIndicesAdminClient) indicesClient, new PutCustomMetaRequest().name(name));
    }

    public PutCustomMetaRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new PutCustomMetaRequest());
    }

    /**
     * Sets the name of the custom meta.
     */
    public PutCustomMetaRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }

    /**
     * Sets the search request to use to custom meta the index when applicable.
     */
    public PutCustomMetaRequestBuilder setSearchRequest(SearchRequest searchRequest) {
        request.searchRequest(searchRequest);
        return this;
    }

    /**
     * Sets the search request to use to custom meta the index when applicable.
     */
    public PutCustomMetaRequestBuilder setSearchRequest(SearchRequestBuilder searchRequest) {
        request.searchRequest(searchRequest);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutCustomMetaResponse> listener) {
        ((IndicesAdminClient) client).putCustomMeta(request, listener);
    }
}
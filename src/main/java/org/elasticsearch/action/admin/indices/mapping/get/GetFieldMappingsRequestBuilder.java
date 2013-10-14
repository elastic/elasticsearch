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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.info.ClusterInfoRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalGenericClient;

/**
 * A helper class to build {@link GetFieldMappingsRequest} objects
 */
public class GetFieldMappingsRequestBuilder extends ClusterInfoRequestBuilder<GetFieldMappingsRequest, GetFieldMappingsResponse, GetFieldMappingsRequestBuilder> {

    public GetFieldMappingsRequestBuilder(InternalGenericClient client, String... indices) {
        super(client, new GetFieldMappingsRequest().indices(indices));
    }


    /**
     * Sets the fields to retrieve.
     */
    public GetFieldMappingsRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    /**
     * Indicates whether default mapping settings should be returned
     */
    public GetFieldMappingsRequestBuilder includeDefaults(boolean includeDefaults) {
        request.includeDefaults(includeDefaults);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<GetFieldMappingsResponse> listener) {
        ((IndicesAdminClient) client).getFieldMappings(request, listener);
    }
}

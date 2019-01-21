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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

/** A helper class to build {@link GetFieldMappingsRequest} objects */
public class GetFieldMappingsRequestBuilder
        extends ActionRequestBuilder<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    public GetFieldMappingsRequestBuilder(ElasticsearchClient client, GetFieldMappingsAction action, String... indices) {
        super(client, action, new GetFieldMappingsRequest().indices(indices));
    }

    public GetFieldMappingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetFieldMappingsRequestBuilder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return this;
    }

    public GetFieldMappingsRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    public GetFieldMappingsRequestBuilder addTypes(String... types) {
        request.types(ArrayUtils.concat(request.types(), types));
        return this;
    }

    public GetFieldMappingsRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }


    /** Sets the fields to retrieve. */
    public GetFieldMappingsRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    /** Indicates whether default mapping settings should be returned */
    public GetFieldMappingsRequestBuilder includeDefaults(boolean includeDefaults) {
        request.includeDefaults(includeDefaults);
        return this;
    }
}

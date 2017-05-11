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

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

public class GetSettingsRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetSettingsRequest, GetSettingsResponse, GetSettingsRequestBuilder> {

    public GetSettingsRequestBuilder(ElasticsearchClient client, GetSettingsAction action, String... indices) {
        super(client, action, new GetSettingsRequest().indices(indices));
    }

    public GetSettingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetSettingsRequestBuilder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public GetSettingsRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    public GetSettingsRequestBuilder setNames(String... names) {
        request.names(names);
        return this;
    }
}

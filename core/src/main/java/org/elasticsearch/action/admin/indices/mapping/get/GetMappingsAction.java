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

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 */
public class GetMappingsAction extends Action<GetMappingsRequest, GetMappingsResponse, GetMappingsRequestBuilder> {

    public static final GetMappingsAction INSTANCE = new GetMappingsAction();
    public static final String NAME = "indices:admin/mappings/get";

    private GetMappingsAction() {
        super(NAME);
    }

    @Override
    public GetMappingsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetMappingsRequestBuilder(client, this);
    }

    @Override
    public GetMappingsResponse newResponse() {
        return new GetMappingsResponse();
    }
}

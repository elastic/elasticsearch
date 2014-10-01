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

import org.elasticsearch.action.admin.indices.IndicesAction;
import org.elasticsearch.client.IndicesAdminClient;

/**
 */
public class GetSettingsAction extends IndicesAction<GetSettingsRequest, GetSettingsResponse, GetSettingsRequestBuilder> {

    public static final GetSettingsAction INSTANCE = new GetSettingsAction();
    public static final String NAME = "indices:monitor/settings/get";

    public GetSettingsAction() {
        super(NAME);
    }

    @Override
    public GetSettingsRequestBuilder newRequestBuilder(IndicesAdminClient client) {
        return new GetSettingsRequestBuilder(client);
    }

    @Override
    public GetSettingsResponse newResponse() {
        return new GetSettingsResponse();
    }
}

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

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.action.admin.indices.IndicesAction;
import org.elasticsearch.client.IndicesAdminClient;

/**
 */
public class UpdateSettingsAction extends IndicesAction<UpdateSettingsRequest, UpdateSettingsResponse, UpdateSettingsRequestBuilder> {

    public static final UpdateSettingsAction INSTANCE = new UpdateSettingsAction();
    public static final String NAME = "indices:admin/settings/update";

    private UpdateSettingsAction() {
        super(NAME);
    }

    @Override
    public UpdateSettingsResponse newResponse() {
        return new UpdateSettingsResponse();
    }

    @Override
    public UpdateSettingsRequestBuilder newRequestBuilder(IndicesAdminClient client) {
        return new UpdateSettingsRequestBuilder(client);
    }
}

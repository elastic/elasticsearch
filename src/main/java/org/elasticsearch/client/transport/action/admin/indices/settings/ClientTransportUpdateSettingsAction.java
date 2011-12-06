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

package org.elasticsearch.client.transport.action.admin.indices.settings;

import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse;
import org.elasticsearch.client.transport.action.support.BaseClientTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class ClientTransportUpdateSettingsAction extends BaseClientTransportAction<UpdateSettingsRequest, UpdateSettingsResponse> {

    @Inject
    public ClientTransportUpdateSettingsAction(Settings settings, TransportService transportService) {
        super(settings, transportService, UpdateSettingsResponse.class);
    }

    @Override
    protected String action() {
        return TransportActions.Admin.Indices.UPDATE_SETTINGS;
    }
}
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

package org.elasticsearch.rest.action.admin.indices.exists;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.splitIndices;

/**
 *
 */
public class RestIndicesExistsAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    @Inject
    public RestIndicesExistsAction(Settings settings, Client client, RestController controller,
                                   SettingsFilter settingsFilter) {
        super(settings, client);
        controller.registerHandler(HEAD, "/{index}", this);

        this.settingsFilter = settingsFilter;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(splitIndices(request.param("index")));
        indicesExistsRequest.listenerThreaded(false);
        client.admin().indices().exists(indicesExistsRequest, new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(IndicesExistsResponse response) {
                try {
                    if (response.exists()) {
                        channel.sendResponse(new StringRestResponse(OK));
                    } else {
                        channel.sendResponse(new StringRestResponse(NOT_FOUND));
                    }
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new StringRestResponse(ExceptionsHelper.status(e)));
                } catch (Exception e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices.exists.types;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Rest api for checking if a type exists.
 */
public class RestTypesExistsAction extends BaseRestHandler {

    @Inject
    public RestTypesExistsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HEAD, "/{index}/{type}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        TypesExistsRequest typesExistsRequest = new TypesExistsRequest(
                Strings.splitStringByCommaToArray(request.param("index")), Strings.splitStringByCommaToArray(request.param("type"))
        );
        typesExistsRequest.listenerThreaded(false);
        if (request.hasParam("ignore_indices")) {
            typesExistsRequest.ignoreIndices(IgnoreIndices.fromString(request.param("ignore_indices")));
        }
        client.admin().indices().typesExists(typesExistsRequest, new ActionListener<TypesExistsResponse>() {
            @Override
            public void onResponse(TypesExistsResponse response) {
                try {
                    if (response.isExists()) {
                        channel.sendResponse(new StringRestResponse(OK));
                    } else {
                        channel.sendResponse(new StringRestResponse(NOT_FOUND));
                    }
                } catch (Throwable e) {
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

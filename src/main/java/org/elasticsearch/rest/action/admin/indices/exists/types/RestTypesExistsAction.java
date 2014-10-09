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
package org.elasticsearch.rest.action.admin.indices.exists.types;

import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;

import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Rest api for checking if a type exists.
 */
public class RestTypesExistsAction extends BaseRestHandler {

    @Inject
    public RestTypesExistsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(HEAD, "/{index}/{type}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        TypesExistsRequest typesExistsRequest = new TypesExistsRequest(
                Strings.splitStringByCommaToArray(request.param("index")), Strings.splitStringByCommaToArray(request.param("type"))
        );
        typesExistsRequest.listenerThreaded(false);
        typesExistsRequest.local(request.paramAsBoolean("local", typesExistsRequest.local()));
        typesExistsRequest.indicesOptions(IndicesOptions.fromRequest(request, typesExistsRequest.indicesOptions()));
        client.admin().indices().typesExists(typesExistsRequest, new RestResponseListener<TypesExistsResponse>(channel) {
            @Override
            public RestResponse buildResponse(TypesExistsResponse response) throws Exception {
                if (response.isExists()) {
                    return new BytesRestResponse(OK);
                } else {
                    return new BytesRestResponse(NOT_FOUND);
                }
            }
        });
    }
}

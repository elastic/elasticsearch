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

package org.elasticsearch.rest.action.get;

import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiGetAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    @Inject
    public RestMultiGetAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, "/_mget", this);
        controller.registerHandler(POST, "/_mget", this);
        controller.registerHandler(GET, "/{index}/_mget", this);
        controller.registerHandler(POST, "/{index}/_mget", this);
        controller.registerHandler(GET, "/{index}/{type}/_mget", this);
        controller.registerHandler(POST, "/{index}/{type}/_mget", this);

        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.refresh(request.paramAsBoolean("refresh", multiGetRequest.refresh()));
        multiGetRequest.preference(request.param("preference"));
        multiGetRequest.realtime(request.paramAsBoolean("realtime", multiGetRequest.realtime()));
        multiGetRequest.ignoreErrorsOnGeneratedFields(request.paramAsBoolean("ignore_errors_on_generated_fields", false));

        String[] sFields = null;
        String sField = request.param("fields");
        if (sField != null) {
            sFields = Strings.splitStringByCommaToArray(sField);
        }

        FetchSourceContext defaultFetchSource = FetchSourceContext.parseFromRestRequest(request);
        multiGetRequest.add(request.param("index"), request.param("type"), sFields, defaultFetchSource, request.param("routing"), RestActions.getRestContent(request), allowExplicitIndex);

        client.multiGet(multiGetRequest, new RestToXContentListener<MultiGetResponse>(channel));
    }
}

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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 */
public class RestMultiSearchAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    @Inject
    public RestMultiSearchAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(GET, "/_msearch", this);
        controller.registerHandler(POST, "/_msearch", this);
        controller.registerHandler(GET, "/{index}/_msearch", this);
        controller.registerHandler(POST, "/{index}/_msearch", this);
        controller.registerHandler(GET, "/{index}/{type}/_msearch", this);
        controller.registerHandler(POST, "/{index}/{type}/_msearch", this);

        controller.registerHandler(GET, "/_msearch/template", this);
        controller.registerHandler(POST, "/_msearch/template", this);
        controller.registerHandler(GET, "/{index}/_msearch/template", this);
        controller.registerHandler(POST, "/{index}/_msearch/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_msearch/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_msearch/template", this);

        this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        String path = request.path();
        boolean isTemplateRequest = isTemplateRequest(path);
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, multiSearchRequest.indicesOptions());
        multiSearchRequest.add(RestActions.getRestContent(request), isTemplateRequest, indices, types, request.param("search_type"), request.param("routing"), indicesOptions, allowExplicitIndex);

        client.multiSearch(multiSearchRequest, new RestToXContentListener<MultiSearchResponse>(channel));
    }

    private boolean isTemplateRequest(String path) {
        return (path != null && path.endsWith("/template"));
    }
}

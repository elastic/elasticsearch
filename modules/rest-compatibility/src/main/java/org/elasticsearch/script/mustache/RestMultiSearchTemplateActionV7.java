/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.script.mustache;

import org.elasticsearch.compat.TypeConsumer;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchTemplateActionV7 extends RestMultiSearchTemplateAction {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestMultiSearchTemplateActionV7.class);

    static final String TYPES_DEPRECATION_MESSAGE = "[types removal]"
        + " Specifying types in multi search template requests is deprecated.";

    public RestMultiSearchTemplateActionV7(Settings settings) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_msearch/template"),
            new Route(POST, "/_msearch/template"),
            new Route(GET, "/{index}/_msearch/template"),
            new Route(POST, "/{index}/_msearch/template"),

            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_msearch/template"),
            new Route(POST, "/{index}/{type}/_msearch/template")
        );
    }

    @Override
    public String getName() {
        return super.getName() + "_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        request.param("type");

        TypeConsumer typeConsumer = new TypeConsumer(request, "type", "types");
        MultiSearchTemplateRequest multiRequest = parseRequest(request, allowExplicitIndex, typeConsumer);
        if (typeConsumer.hasTypes()) {
            deprecationLogger.deprecate("msearch_with_types", TYPES_DEPRECATION_MESSAGE);
        }
        return channel -> client.execute(MultiSearchTemplateAction.INSTANCE, multiRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchTemplateRequest}
     */
    public static MultiSearchTemplateRequest parseRequest(RestRequest restRequest, boolean allowExplicitIndex) throws IOException {
        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        if (restRequest.hasParam("max_concurrent_searches")) {
            multiRequest.maxConcurrentSearchRequests(restRequest.paramAsInt("max_concurrent_searches", 0));
        }

        RestMultiSearchAction.parseMultiLineRequest(
            restRequest,
            multiRequest.indicesOptions(),
            allowExplicitIndex,
            (searchRequest, bytes) -> {
                SearchTemplateRequest searchTemplateRequest = SearchTemplateRequest.fromXContent(bytes);
                if (searchTemplateRequest.getScript() != null) {
                    searchTemplateRequest.setRequest(searchRequest);
                    multiRequest.add(searchTemplateRequest);
                } else {
                    throw new IllegalArgumentException("Malformed search template");
                }
                RestSearchAction.checkRestTotalHits(restRequest, searchRequest);
            },
            k -> false
        );
        return multiRequest;
    }
}

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

package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchTemplateAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    @Inject
    public RestMultiSearchTemplateAction(Settings settings, RestController controller) {
        super(settings);
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);

        controller.registerHandler(GET, "/_msearch/template", this);
        controller.registerHandler(POST, "/_msearch/template", this);
        controller.registerHandler(GET, "/{index}/_msearch/template", this);
        controller.registerHandler(POST, "/{index}/_msearch/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_msearch/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_msearch/template", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (RestActions.hasBodyContent(request) == false) {
            throw new ElasticsearchException("request body is required");
        }

        MultiSearchTemplateRequest multiRequest = parseRequest(request, allowExplicitIndex);
        client.execute(MultiSearchTemplateAction.INSTANCE, multiRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchTemplateRequest}
     */
    public static MultiSearchTemplateRequest parseRequest(RestRequest restRequest, boolean allowExplicitIndex) throws IOException {

        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        RestMultiSearchAction.parseMultiLineRequest(restRequest, multiRequest.indicesOptions(), allowExplicitIndex,
                (searchRequest, bytes) -> {
                    try {
                        SearchTemplateRequest searchTemplateRequest = RestSearchTemplateAction.parse(bytes);
                        if (searchTemplateRequest.getScript() != null) {
                            searchTemplateRequest.setRequest(searchRequest);
                            multiRequest.add(searchTemplateRequest);
                        } else {
                            throw new IllegalArgumentException("Malformed search template");
                        }
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("Exception when parsing search template request", e);
                    }
                });
        return multiRequest;
    }
}

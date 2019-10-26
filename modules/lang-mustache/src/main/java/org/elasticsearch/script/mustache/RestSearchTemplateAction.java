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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchTemplateAction extends BaseRestHandler {
    public static final String TYPED_KEYS_PARAM = "typed_keys";
    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(Arrays.asList(TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM));
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    public RestSearchTemplateAction(RestController controller) {
        controller.registerHandler(GET, "/_search/template", this);
        controller.registerHandler(POST, "/_search/template", this);
        controller.registerHandler(GET, "/{index}/_search/template", this);
        controller.registerHandler(POST, "/{index}/_search/template", this);
    }

    @Override
    public String getName() {
        return "search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Creates the search request with all required params
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(searchRequest, request, null, size -> searchRequest.source().size(size));

        // Creates the search template request
        SearchTemplateRequest searchTemplateRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            searchTemplateRequest = SearchTemplateRequest.fromXContent(parser);
        }
        searchTemplateRequest.setRequest(searchRequest);
        RestSearchAction.checkRestTotalHits(request, searchRequest);

        return channel -> client.execute(SearchTemplateAction.INSTANCE, searchTemplateRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}

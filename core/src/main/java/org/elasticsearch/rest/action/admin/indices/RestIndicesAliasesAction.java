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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestIndicesAliasesAction extends BaseRestHandler {
    static final ObjectParser<IndicesAliasesRequest, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("aliases");
    static {
        PARSER.declareObjectArray((request, actions) -> {
            for (AliasActions action: actions) {
                request.addAliasAction(action);
            }
        }, AliasActions.PARSER, new ParseField("actions"));
    }

    @Inject
    public RestIndicesAliasesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_aliases", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws Exception {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", indicesAliasesRequest.masterNodeTimeout()));
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
        try (XContentParser parser = XContentFactory.xContent(request.content()).createParser(request.content())) {
            PARSER.parse(parser, indicesAliasesRequest, () -> ParseFieldMatcher.STRICT);
        }
        if (indicesAliasesRequest.getAliasActions().isEmpty()) {
            throw new IllegalArgumentException("No action specified");
        }
        client.admin().indices().aliases(indicesAliasesRequest, new AcknowledgedRestListener<IndicesAliasesResponse>(channel));
    }
}

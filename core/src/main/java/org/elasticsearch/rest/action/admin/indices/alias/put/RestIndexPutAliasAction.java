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
package org.elasticsearch.rest.action.admin.indices.alias.put;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 */
public class RestIndexPutAliasAction extends BaseRestHandler {

    @Inject
    public RestIndexPutAliasAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(PUT, "/{index}/_alias/{name}", this);
        controller.registerHandler(PUT, "/_alias/{name}", this);
        controller.registerHandler(PUT, "/{index}/_aliases/{name}", this);
        controller.registerHandler(PUT, "/_aliases/{name}", this);
        controller.registerHandler(PUT, "/{index}/_alias", this);
        controller.registerHandler(PUT, "/_alias", this);

        controller.registerHandler(POST, "/{index}/_alias/{name}", this);
        controller.registerHandler(POST, "/_alias/{name}", this);
        controller.registerHandler(POST, "/{index}/_aliases/{name}", this);
        controller.registerHandler(POST, "/_aliases/{name}", this);
        controller.registerHandler(PUT, "/{index}/_aliases", this);
        //we cannot add POST for "/_aliases" because this is the _aliases api already defined in RestIndicesAliasesAction
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String alias = request.param("name");
        Map<String, Object> filter = null;
        String routing = null;
        String indexRouting = null;
        String searchRouting = null;

        if (request.hasContent()) {
            try (XContentParser parser = XContentFactory.xContent(request.content()).createParser(request.content())) {
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    throw new IllegalArgumentException("No index alias is specified");
                }
                String currentFieldName = null;
                while ((token = parser.nextToken()) != null) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("index".equals(currentFieldName)) {
                            indices = Strings.splitStringByCommaToArray(parser.text());
                        } else if ("alias".equals(currentFieldName)) {
                            alias = parser.text();
                        } else if ("routing".equals(currentFieldName)) {
                            routing = parser.textOrNull();
                        } else if ("indexRouting".equals(currentFieldName) || "index-routing".equals(currentFieldName) || "index_routing".equals(currentFieldName)) {
                            indexRouting = parser.textOrNull();
                        } else if ("searchRouting".equals(currentFieldName) || "search-routing".equals(currentFieldName) || "search_routing".equals(currentFieldName)) {
                            searchRouting = parser.textOrNull();
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("filter".equals(currentFieldName)) {
                            filter = parser.mapOrdered();
                        }
                    }
                }
            }
        }

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
        String[] aliases = new String[]{alias};
        IndicesAliasesRequest.AliasActions aliasAction = new AliasActions(AliasAction.Type.ADD, indices, aliases);
        indicesAliasesRequest.addAliasAction(aliasAction);
        indicesAliasesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", indicesAliasesRequest.masterNodeTimeout()));


        if (routing != null) {
            aliasAction.routing(routing);
        }
        if (searchRouting != null) {
            aliasAction.searchRouting(searchRouting);
        }
        if (indexRouting != null) {
            aliasAction.indexRouting(indexRouting);
        }
        if (filter != null) {
            aliasAction.filter(filter);
        }
        client.admin().indices().aliases(indicesAliasesRequest, new AcknowledgedRestListener<IndicesAliasesResponse>(channel));
    }
}

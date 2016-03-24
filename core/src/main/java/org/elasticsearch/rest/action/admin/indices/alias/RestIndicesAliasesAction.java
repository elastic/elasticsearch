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

package org.elasticsearch.rest.action.admin.indices.alias;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestIndicesAliasesAction extends BaseRestHandler {

    @Inject
    public RestIndicesAliasesAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(POST, "/_aliases", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", indicesAliasesRequest.masterNodeTimeout()));
        try (XContentParser parser = XContentFactory.xContent(request.content()).createParser(request.content())) {
            // {
            //     actions : [
            //         { add : { index : "test1", alias : "alias1", filter : {"user" : "kimchy"} } }
            //         { remove : { index : "test1", alias : "alias1" } }
            //     ]
            // }
            indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                throw new IllegalArgumentException("No action is specified");
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String action = parser.currentName();
                            AliasAction.Type type;
                            if ("add".equals(action)) {
                                type = AliasAction.Type.ADD;
                            } else if ("remove".equals(action)) {
                                type = AliasAction.Type.REMOVE;
                            } else {
                                throw new IllegalArgumentException("Alias action [" + action + "] not supported");
                            }
                            String[] indices = null;
                            String[] aliases = null;
                            Map<String, Object> filter = null;
                            String routing = null;
                            boolean routingSet = false;
                            String indexRouting = null;
                            boolean indexRoutingSet = false;
                            String searchRouting = null;
                            boolean searchRoutingSet = false;
                            String currentFieldName = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if ("index".equals(currentFieldName)) {
                                        indices = new String[] { parser.text() };
                                    } else if ("alias".equals(currentFieldName)) {
                                        aliases = new String[] { parser.text() };
                                    } else if ("routing".equals(currentFieldName)) {
                                        routing = parser.textOrNull();
                                        routingSet = true;
                                    } else if ("indexRouting".equals(currentFieldName) || "index-routing".equals(currentFieldName) || "index_routing".equals(currentFieldName)) {
                                        indexRouting = parser.textOrNull();
                                        indexRoutingSet = true;
                                    } else if ("searchRouting".equals(currentFieldName) || "search-routing".equals(currentFieldName) || "search_routing".equals(currentFieldName)) {
                                        searchRouting = parser.textOrNull();
                                        searchRoutingSet = true;
                                    }
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    if ("indices".equals(currentFieldName)) {
                                        List<String> indexNames = new ArrayList<>();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            String index = parser.text();
                                            indexNames.add(index);
                                        }
                                        indices = indexNames.toArray(new String[indexNames.size()]);
                                    }
                                    if ("aliases".equals(currentFieldName)) {
                                        List<String> aliasNames = new ArrayList<>();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            String alias = parser.text();
                                            aliasNames.add(alias);
                                        }
                                        aliases = aliasNames.toArray(new String[aliasNames.size()]);
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if ("filter".equals(currentFieldName)) {
                                        filter = parser.mapOrdered();
                                    }
                                }
                            }

                            if (type == AliasAction.Type.ADD) {
                                AliasActions aliasActions = new AliasActions(type, indices, aliases).filter(filter);
                                if (routingSet) {
                                    aliasActions.routing(routing);
                                }
                                if (indexRoutingSet) {
                                    aliasActions.indexRouting(indexRouting);
                                }
                                if (searchRoutingSet) {
                                    aliasActions.searchRouting(searchRouting);
                                }
                                indicesAliasesRequest.addAliasAction(aliasActions);
                            } else if (type == AliasAction.Type.REMOVE) {
                                indicesAliasesRequest.removeAlias(indices, aliases);
                            }
                        }
                    }
                }
            }
        }
        client.admin().indices().aliases(indicesAliasesRequest, new AcknowledgedRestListener<IndicesAliasesResponse>(channel));
    }
}

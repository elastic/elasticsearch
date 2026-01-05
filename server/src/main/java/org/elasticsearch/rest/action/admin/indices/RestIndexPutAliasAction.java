/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestIndexPutAliasAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_alias/{name}"),
            new Route(PUT, "/{index}/_alias/{name}"),
            new Route(POST, "/_alias/{name}"),
            new Route(PUT, "/_alias/{name}"),
            new Route(POST, "/{index}/_aliases/{name}"),
            new Route(PUT, "/{index}/_aliases/{name}"),
            new Route(POST, "/_aliases/{name}"),
            new Route(PUT, "/_aliases/{name}"),
            new Route(PUT, "/{index}/_alias"),
            new Route(PUT, "/{index}/_aliases"),
            new Route(PUT, "/_alias")
        );
    }

    @Override
    public String getName() {
        return "index_put_alias_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String alias = request.param("name");
        Map<String, Object> filter = null;
        String routing = null;
        String indexRouting = null;
        String searchRouting = null;
        Boolean writeIndex = null;

        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
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
                        } else if ("indexRouting".equals(currentFieldName)
                            || "index-routing".equals(currentFieldName)
                            || "index_routing".equals(currentFieldName)) {
                                indexRouting = parser.textOrNull();
                            } else if ("searchRouting".equals(currentFieldName)
                                || "search-routing".equals(currentFieldName)
                                || "search_routing".equals(currentFieldName)) {
                                    searchRouting = parser.textOrNull();
                                } else if ("is_write_index".equals(currentFieldName)) {
                                    writeIndex = parser.booleanValue();
                                } else {
                                    throw new IllegalArgumentException("Unsupported field [" + currentFieldName + "]");
                                }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("filter".equals(currentFieldName)) {
                            filter = parser.mapOrdered();
                        }
                    } else if (token != XContentParser.Token.END_OBJECT) {
                        throw new ParsingException(parser.getTokenLocation(), "Unexpected token [" + token + "]");
                    }
                }
            }
        }

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest(getMasterNodeTimeout(request), getAckTimeout(request));

        IndicesAliasesRequest.AliasActions aliasAction = AliasActions.add().indices(indices).alias(alias);
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
        if (writeIndex != null) {
            aliasAction.writeIndex(writeIndex);
        }
        indicesAliasesRequest.addAliasAction(aliasAction);
        return channel -> client.admin().indices().aliases(indicesAliasesRequest, new RestToXContentListener<>(channel));
    }
}

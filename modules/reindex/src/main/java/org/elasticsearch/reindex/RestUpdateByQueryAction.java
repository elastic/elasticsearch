/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpdateByQueryAction extends AbstractBulkByQueryRestHandler<UpdateByQueryRequest, UpdateByQueryAction> {

    public RestUpdateByQueryAction() {
        super(UpdateByQueryAction.INSTANCE);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_update_by_query"),
            Route.builder(POST, "/{index}/{type}/_update_by_query")
                .deprecated(RestSearchAction.TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "update_by_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, false, true);
    }

    @Override
    protected UpdateByQueryRequest buildRequest(RestRequest request, NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            request.param("type");
        }
        /*
         * Passing the search request through UpdateByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parse can override them.
         */
        UpdateByQueryRequest internal = new UpdateByQueryRequest();

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));
        consumers.put("script", o -> internal.setScript(Script.parse(o)));
        consumers.put("max_docs", s -> setMaxDocsValidateIdentical(internal, ((Number) s).intValue()));

        parseInternalRequest(internal, request, namedWriteableRegistry, consumers);

        internal.setPipeline(request.param("pipeline"));
        return internal;
    }
}

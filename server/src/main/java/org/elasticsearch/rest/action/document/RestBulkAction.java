/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 */
@ServerlessScope(Scope.PUBLIC)
public class RestBulkAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in bulk requests is deprecated.";

    private final boolean allowExplicitIndex;

    public RestBulkAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_bulk"),
            new Route(PUT, "/_bulk"),
            new Route(POST, "/{index}/_bulk"),
            new Route(PUT, "/{index}/_bulk"),
            Route.builder(POST, "/{index}/{type}/_bulk").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/{index}/{type}/_bulk").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            request.param("type");
        }
        BulkRequest bulkRequest = new BulkRequest();
        String defaultIndex = request.param("index");
        String defaultRouting = request.param("routing");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String defaultPipeline = request.param("pipeline");
        boolean defaultListExecutedPipelines = request.paramAsBoolean("list_executed_pipelines", false);
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        Boolean defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false);
        boolean defaultRequireDataStream = request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false);
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(request.param("refresh"));
        bulkRequest.add(
            request.requiredContent(),
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            defaultRequireDataStream,
            defaultListExecutedPipelines,
            allowExplicitIndex,
            request.getXContentType(),
            request.getRestApiVersion()
        );

        return channel -> client.bulk(bulkRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }

    @Override
    public boolean supportsBulkContent() {
        return true;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}

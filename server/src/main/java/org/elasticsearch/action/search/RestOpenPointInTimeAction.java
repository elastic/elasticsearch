/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestOpenPointInTimeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "open_point_in_time";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_pit"), new Route(POST, "/_pit"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(indices);
        openRequest.indicesOptions(IndicesOptions.fromRequest(request, OpenPointInTimeRequest.DEFAULT_INDICES_OPTIONS));
        openRequest.routing(request.param("routing"));
        openRequest.preference(request.param("preference"));
        openRequest.keepAlive(TimeValue.parseTimeValue(request.param("keep_alive"), null, "keep_alive"));
        openRequest.allowPartialSearchResults(request.paramAsBoolean("allow_partial_search_results", false));
        if (request.hasParam("max_concurrent_shard_requests")) {
            final int maxConcurrentShardRequests = request.paramAsInt(
                "max_concurrent_shard_requests",
                openRequest.maxConcurrentShardRequests()
            );
            openRequest.maxConcurrentShardRequests(maxConcurrentShardRequests);
        }

        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                PARSER.parse(parser, openRequest, null);
            }
        });

        return channel -> client.execute(TransportOpenPointInTimeAction.TYPE, openRequest, new RestToXContentListener<>(channel));
    }

    private static final ObjectParser<OpenPointInTimeRequest, Void> PARSER = new ObjectParser<>("open_point_in_time_request");
    private static final ParseField INDEX_FILTER_FIELD = new ParseField("index_filter");

    static {
        PARSER.declareObject(OpenPointInTimeRequest::indexFilter, (p, c) -> parseTopLevelQuery(p), INDEX_FILTER_FIELD);
    }
}

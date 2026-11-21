/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_query/stream/{id}}: reads the stored result of a stream, from {@code offset}, limited by {@code max_rows} and
 * {@code max_bytes} when they are set. It is the counterpart of {@code RestEsqlGetAsyncResultAction}, and {@code wait_timeout} plays the
 * role of {@code wait_for_completion_timeout}. For now nothing is stored, so every id is not found.
 * <p>
 * {@code max_rows=0} and {@code max_bytes=0} are both accepted and return the status only. This differs from {@code POST /_query/stream},
 * which rejects {@code max_bytes=0} because its response always writes the first row.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlGetStreamResultAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/stream/{id}"));
    }

    @Override
    public String getName() {
        return "esql_get_stream_result";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetStreamResultRequest get = parseRequest(request);
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            EsqlStreamGetResultAction.INSTANCE,
            get,
            new RestActionListener<ActionResponse.Empty>(channel) {
                @Override
                protected void processResponse(ActionResponse.Empty response) {
                    // Unreachable until streams are stored: every id is not found, so the action fails. The real implementation replaces
                    // this with the body that streams the rows.
                    channel.sendResponse(new RestResponse(RestStatus.OK, RestEsqlStreamQueryAction.NDJSON_MEDIA_TYPE, BytesArray.EMPTY));
                }
            }
        );
    }

    /**
     * Reads the parameters. A limit that is not provided stays unlimited, and an unset {@code wait_timeout} means no wait, as an unset
     * {@code wait_for_completion_timeout} does for the async API. A value that cannot be parsed is a 400, and so is a negative offset or
     * limit, a {@code wait_timeout} above {@link GetStreamResultRequest#MAX_WAIT_TIMEOUT}, and a negative {@code wait_timeout}
     * other than {@code -1}, which the time parser rejects, as it does for the async API.
     */
    static GetStreamResultRequest parseRequest(RestRequest request) {
        String streamId = request.param("id");
        long offset = request.paramAsLong("offset", 0L);
        long maxRows = GetStreamResultRequest.NO_LIMIT;
        if (request.hasParam("max_rows")) {
            maxRows = request.paramAsLong("max_rows", 0L);
            if (maxRows < 0) {
                // a negative value must not be read as the internal "not provided" marker
                throw new IllegalArgumentException("[max_rows] must be greater than or equal to 0 but was [" + maxRows + "]");
            }
        }
        long maxBytes = GetStreamResultRequest.NO_LIMIT;
        if (request.hasParam("max_bytes")) {
            maxBytes = request.paramAsSize("max_bytes", null).getBytes();
            if (maxBytes < 0) {
                throw new IllegalArgumentException("[max_bytes] must be greater than or equal to 0 but was [" + maxBytes + "]");
            }
        }
        TimeValue waitTimeout = request.hasParam("wait_timeout") ? request.paramAsTime("wait_timeout", TimeValue.MINUS_ONE) : null;
        return new GetStreamResultRequest(streamId, offset, maxRows, maxBytes, waitTimeout);
    }
}

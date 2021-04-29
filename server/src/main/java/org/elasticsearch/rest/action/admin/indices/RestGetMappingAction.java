/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.DispatchingRestToXContentListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetMappingAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetMappingAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get"
        + " mapping requests is deprecated. The parameter will be removed in the next major version.";

    private final ThreadPool threadPool;

    public RestGetMappingAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_mapping"),
            new Route(GET, "/_mappings"),
            new Route(GET, "/{index}/_mapping"),
            new Route(GET, "/{index}/_mappings"));
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            request.param(INCLUDE_TYPE_NAME_PARAMETER);
            deprecationLogger.compatibleApiWarning("get_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        final TimeValue timeout = request.paramAsTime("master_timeout", getMappingsRequest.masterNodeTimeout());
        getMappingsRequest.masterNodeTimeout(timeout);
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        final HttpChannel httpChannel = request.getHttpChannel();
        return channel -> new RestCancellableNodeClient(client, httpChannel).admin().indices().getMappings(getMappingsRequest,
                new DispatchingRestToXContentListener<>(threadPool.executor(ThreadPool.Names.MANAGEMENT), channel, request)
                    .map(getMappingsResponse ->
                        new RestGetMappingsResponse(getMappingsResponse, threadPool::relativeTimeInMillis, timeout)));
    }

    private static final class RestGetMappingsResponse implements ToXContentObject {
        private final GetMappingsResponse response;
        private final LongSupplier relativeTimeSupplierMillis;
        private final TimeValue timeout;
        private final long startTimeMs;

        private RestGetMappingsResponse(GetMappingsResponse response, LongSupplier relativeTimeSupplierMillis, TimeValue timeout) {
            this.response = response;
            this.relativeTimeSupplierMillis = relativeTimeSupplierMillis;
            this.timeout = timeout;
            this.startTimeMs = relativeTimeSupplierMillis.getAsLong();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (relativeTimeSupplierMillis.getAsLong() - startTimeMs > timeout.millis()) {
                throw new ElasticsearchTimeoutException("Timed out getting mappings");
            }

            builder.startObject();
            response.toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }
}

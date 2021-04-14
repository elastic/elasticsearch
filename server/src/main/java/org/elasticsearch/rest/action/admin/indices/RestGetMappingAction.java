/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

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
        return channel -> client.admin().indices().getMappings(getMappingsRequest, new RestActionListener<>(channel) {

            @Override
            protected void processResponse(GetMappingsResponse getMappingsResponse) {
                final long startTimeMs = threadPool.relativeTimeInMillis();
                // Process serialization on MANAGEMENT pool since the serialization of the raw mappings to XContent can be too slow to
                // execute on an IO thread
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(
                        ActionRunnable.wrap(this, l -> new RestBuilderListener<GetMappingsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(final GetMappingsResponse response,
                                                              final XContentBuilder builder) throws Exception {
                                if (threadPool.relativeTimeInMillis() - startTimeMs > timeout.millis()) {
                                    throw new ElasticsearchTimeoutException("Timed out getting mappings");
                                }
                                builder.startObject();
                                response.toXContent(builder, request);
                                builder.endObject();
                                return new BytesRestResponse(RestStatus.OK, builder);
                            }
                        }.onResponse(getMappingsResponse)));
            }
        });
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.rest.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequestBuilder;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestMonitoringBulkAction extends BaseRestHandler {

    public static final String MONITORING_ID = "system_id";
    public static final String MONITORING_VERSION = "system_api_version";
    public static final String INTERVAL = "interval";

    private static final List<String> ALL_VERSIONS = asList(
        MonitoringTemplateUtils.TEMPLATE_VERSION,
        MonitoringTemplateUtils.OLD_TEMPLATE_VERSION
    );

    private static final Map<MonitoredSystem, List<String>> SUPPORTED_API_VERSIONS = Map.of(
        MonitoredSystem.KIBANA,
        ALL_VERSIONS,
        MonitoredSystem.LOGSTASH,
        ALL_VERSIONS,
        MonitoredSystem.BEATS,
        ALL_VERSIONS
    );

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_monitoring/bulk").replaces(POST, "/_xpack/monitoring/_bulk", RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_monitoring/bulk").replaces(PUT, "/_xpack/monitoring/_bulk", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "monitoring_bulk";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        final String id = request.param(MONITORING_ID);
        if (Strings.isEmpty(id)) {
            throw new IllegalArgumentException("no [" + MONITORING_ID + "] for monitoring bulk request");
        }

        final String version = request.param(MONITORING_VERSION);
        if (Strings.isEmpty(version)) {
            throw new IllegalArgumentException("no [" + MONITORING_VERSION + "] for monitoring bulk request");
        }

        final String intervalAsString = request.param(INTERVAL);
        if (Strings.isEmpty(intervalAsString)) {
            throw new IllegalArgumentException("no [" + INTERVAL + "] for monitoring bulk request");
        }

        if (false == request.hasContentOrSourceParam()) {
            throw new ElasticsearchParseException("no body content for monitoring bulk request");
        }

        final MonitoredSystem system = MonitoredSystem.fromSystem(id);
        if (isSupportedSystemVersion(system, version) == false) {
            throw new IllegalArgumentException(
                MONITORING_VERSION + " [" + version + "] is not supported by " + MONITORING_ID + " [" + id + "]"
            );
        }

        final long timestamp = System.currentTimeMillis();
        final long intervalMillis = parseTimeValue(intervalAsString, INTERVAL).getMillis();

        final MonitoringBulkRequestBuilder requestBuilder = new MonitoringBulkRequestBuilder(client);
        requestBuilder.add(system, request.content(), request.getXContentType(), timestamp, intervalMillis);
        return channel -> requestBuilder.execute(getRestBuilderListener(channel));
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    /**
     * Indicate if the given {@link MonitoredSystem} and system api version pair is supported by
     * the Monitoring Bulk API.
     *
     * @param system the {@link MonitoredSystem}
     * @param version the system API version
     * @return true if supported, false otherwise
     */
    private boolean isSupportedSystemVersion(final MonitoredSystem system, final String version) {
        final List<String> monitoredSystem = SUPPORTED_API_VERSIONS.getOrDefault(system, emptyList());
        return monitoredSystem.contains(version);
    }

    static RestBuilderListener<MonitoringBulkResponse> getRestBuilderListener(RestChannel channel) {
        return new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(MonitoringBulkResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                {
                    builder.field("took", response.getTookInMillis());
                    builder.field("ignored", response.isIgnored());

                    final MonitoringBulkResponse.Error error = response.getError();
                    builder.field("errors", error != null);

                    if (error != null) {
                        builder.field("error", response.getError());
                    }
                }
                builder.endObject();
                return new RestResponse(response.status(), builder);
            }
        };
    }
}

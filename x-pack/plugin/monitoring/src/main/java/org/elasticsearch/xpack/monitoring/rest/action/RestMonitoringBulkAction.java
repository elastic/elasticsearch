/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.rest.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequestBuilder;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestMonitoringBulkAction extends XPackRestHandler {

    public static final String MONITORING_ID = "system_id";
    public static final String MONITORING_VERSION = "system_api_version";
    public static final String INTERVAL = "interval";
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestMonitoringBulkAction.class));
    private final Map<MonitoredSystem, List<String>> supportedApiVersions;

    public RestMonitoringBulkAction(Settings settings, RestController controller) {
        super(settings);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(POST, "/_monitoring/bulk", this,
            POST, "/_xpack/monitoring/_bulk", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, "/_monitoring/bulk", this,
            PUT, "/_xpack/monitoring/_bulk", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, "/_monitoring/{type}/bulk", this,
            POST, "/_xpack/monitoring/{type}/_bulk", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, "/_monitoring/{type}/bulk", this,
            PUT, "/_xpack/monitoring/{type}/_bulk", deprecationLogger);

        final List<String> allVersions = Arrays.asList(
                MonitoringTemplateUtils.TEMPLATE_VERSION,
                MonitoringTemplateUtils.OLD_TEMPLATE_VERSION
        );

        final Map<MonitoredSystem, List<String>> versionsMap = new HashMap<>();
        versionsMap.put(MonitoredSystem.KIBANA, allVersions);
        versionsMap.put(MonitoredSystem.LOGSTASH, allVersions);
        // Beats did not report data in the 5.x timeline, so it should never send the original version
        versionsMap.put(MonitoredSystem.BEATS, Collections.singletonList(MonitoringTemplateUtils.TEMPLATE_VERSION));
        supportedApiVersions = Collections.unmodifiableMap(versionsMap);
    }

    @Override
    public String getName() {
        return "monitoring_bulk";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        final String defaultType = request.param("type");

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
            throw new IllegalArgumentException(MONITORING_VERSION + " [" + version + "] is not supported by "
                    + MONITORING_ID + " [" + id + "]");
        }

        final long timestamp = System.currentTimeMillis();
        final long intervalMillis = parseTimeValue(intervalAsString, INTERVAL).getMillis();

        final MonitoringBulkRequestBuilder requestBuilder = client.monitoring().prepareMonitoringBulk();
        requestBuilder.add(system, defaultType, request.content(), request.getXContentType(), timestamp, intervalMillis);
        return channel -> requestBuilder.execute(new RestBuilderListener<MonitoringBulkResponse>(channel) {
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
                return new BytesRestResponse(response.status(), builder);
            }
        });
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
        final List<String> monitoredSystem = supportedApiVersions.getOrDefault(system, emptyList());
        return monitoredSystem.contains(version);
    }
}

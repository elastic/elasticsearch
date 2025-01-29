/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMonitoringMigrateAlertsAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MonitoringBulkAction.class);
    private static final String DEPRECATION_ID = "xpack_monitoring_migrate_alerts_api_removal";
    private static final String DEPRECATION_MESSAGE =
        "The xpack monitoring migrate alerts action is deprecated and will be removed in the next major release.";

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_monitoring/migrate/alerts"));
    }

    @Override
    public String getName() {
        return "monitoring_migrate_alerts";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        deprecationLogger.critical(DeprecationCategory.API, DEPRECATION_ID, DEPRECATION_MESSAGE);
        MonitoringMigrateAlertsRequest migrateRequest = new MonitoringMigrateAlertsRequest();
        return channel -> client.execute(MonitoringMigrateAlertsAction.INSTANCE, migrateRequest, getRestBuilderListener(channel));
    }

    static RestBuilderListener<MonitoringMigrateAlertsResponse> getRestBuilderListener(RestChannel channel) {
        return new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(MonitoringMigrateAlertsResponse response, XContentBuilder builder) throws Exception {
                return new RestResponse(RestStatus.OK, response.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        };
    }
}

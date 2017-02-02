/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.rest.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkRequestBuilder;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.monitoring.rest.MonitoringRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestMonitoringBulkAction extends MonitoringRestHandler {

    public static final String MONITORING_ID = "system_id";
    public static final String MONITORING_VERSION = "system_api_version";
    public static final String INTERVAL = "interval";

    public RestMonitoringBulkAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, URI_BASE + "/_bulk", this);
        controller.registerHandler(PUT, URI_BASE + "/_bulk", this);
        controller.registerHandler(POST, URI_BASE + "/{type}/_bulk", this);
        controller.registerHandler(PUT, URI_BASE + "/{type}/_bulk", this);
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        String defaultType = request.param("type");

        String id = request.param(MONITORING_ID);
        if (Strings.isEmpty(id)) {
            throw new IllegalArgumentException("no [" + MONITORING_ID + "] for monitoring bulk request");
        }
        String version = request.param(MONITORING_VERSION);
        if (Strings.isEmpty(version)) {
            throw new IllegalArgumentException("no [" + MONITORING_VERSION + "] for monitoring bulk request");
        }
        // we don't currently use the interval, but in future releases we can incorporate it without breaking BWC since it was here from
        //  the beginning
        if (Strings.isEmpty(request.param(INTERVAL))) {
            throw new IllegalArgumentException("no [" + INTERVAL + "] for monitoring bulk request");
        }

        if (false == request.hasContentOrSourceParam()) {
            throw new ElasticsearchParseException("no body content for monitoring bulk request");
        }

        MonitoringBulkRequestBuilder requestBuilder = client.monitoring().prepareMonitoringBulk();
        requestBuilder.add(request.content(), id, version, defaultType, request.getXContentType());
        return channel -> requestBuilder.execute(new RestBuilderListener<MonitoringBulkResponse>(channel) {
            @Override
            public RestResponse buildResponse(MonitoringBulkResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields.TOOK, response.getTookInMillis());

                MonitoringBulkResponse.Error error = response.getError();
                builder.field(Fields.ERRORS, error != null);

                if (error != null) {
                    builder.field(Fields.ERROR, response.getError());
                }
                builder.endObject();
                return new BytesRestResponse(response.status(), builder);
            }
        });
    }

    static final class Fields {
        static final String TOOK = "took";
        static final String ERRORS = "errors";
        static final String ERROR = "error";
    }

}

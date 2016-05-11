/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.action.XPackUsageResponse;
import org.elasticsearch.xpack.rest.XPackRestHandler;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestXPackUsageAction extends XPackRestHandler {

    @Inject
    public RestXPackUsageAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, URI_BASE + "/usage", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel restChannel, XPackClient client) throws Exception {
        new XPackUsageRequestBuilder(client.es()).execute(new RestBuilderListener<XPackUsageResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(XPackUsageResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                for (XPackFeatureSet.Usage usage : response.getUsages()) {
                    builder.field(usage.name(), usage);
                }
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestXPackUsageAction extends XPackRestHandler {
    public RestXPackUsageAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, URI_BASE + "/usage", this);
    }

    @Override
    public String getName() {
        return "xpack_usage_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        final TimeValue masterTimeout = request.paramAsTime("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT);
        return channel -> new XPackUsageRequestBuilder(client.es())
                .setMasterNodeTimeout(masterTimeout)
                .execute(new RestBuilderListener<XPackUsageResponse>(channel) {
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestXPackUsageAction extends XPackRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_xpack/usage"));
    }

    @Override
    public String getName() {
        return "xpack_usage_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) {
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

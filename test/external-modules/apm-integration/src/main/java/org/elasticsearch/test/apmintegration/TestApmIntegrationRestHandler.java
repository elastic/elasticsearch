/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class TestApmIntegrationRestHandler extends BaseRestHandler {

    private final SetOnce<TestMeterUsages> testMeterUsages = new SetOnce<>();

    TestApmIntegrationRestHandler() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_use_apm_metrics"));
    }

    @Override
    public String getName() {
        return "apm_integration_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            testMeterUsages.get().testUponRequest();

            try (XContentBuilder builder = channel.newBuilder()) {
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    public void setTestMeterUsages(TestMeterUsages testMeterUsages) {
        this.testMeterUsages.set(testMeterUsages);
    }
}

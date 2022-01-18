/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.enrollment;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentResponse;

import java.io.IOException;
import java.util.List;

public final class RestNodeEnrollmentAction extends EnrollmentBaseRestHandler {

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    public RestNodeEnrollmentAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "node_enroll_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_security/enroll/node"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        return restChannel -> client.execute(
            NodeEnrollmentAction.INSTANCE,
            new NodeEnrollmentRequest(),
            new RestBuilderListener<NodeEnrollmentResponse>(restChannel) {
                @Override
                public RestResponse buildResponse(NodeEnrollmentResponse nodeEnrollmentResponse, XContentBuilder builder) throws Exception {
                    nodeEnrollmentResponse.toXContent(builder, channel.request());
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            }
        );
    }
}

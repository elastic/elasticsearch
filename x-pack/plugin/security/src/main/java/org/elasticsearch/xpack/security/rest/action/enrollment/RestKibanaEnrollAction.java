/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.enrollment;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

public class RestKibanaEnrollAction extends SecurityBaseRestHandler {

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    public RestKibanaEnrollAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override public String getName() {
        return "kibana_enroll_action";
    }

    @Override public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_security/enroll/kibana"));
    }

    @Override protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        return restChannel -> client.execute(KibanaEnrollmentAction.INSTANCE,
            new KibanaEnrollmentRequest(),
            new RestBuilderListener<KibanaEnrollmentResponse>(restChannel) {
                @Override public RestResponse buildResponse(
                    KibanaEnrollmentResponse kibanaEnrollmentResponse, XContentBuilder builder) throws Exception {
                    kibanaEnrollmentResponse.toXContent(builder, channel.request());
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
    }
}

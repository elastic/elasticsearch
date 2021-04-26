/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.enrollment;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.enrollment.EnrollmentSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.enrollment.CreateEnrollmentTokenAction;
import org.elasticsearch.xpack.core.enrollment.CreateEnrollmentTokenRequest;
import org.elasticsearch.xpack.core.enrollment.CreateEnrollmentTokenResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to create an enrollment token
 */
public class RestCreateEnrollmentTokenAction extends SecurityBaseRestHandler {
    private final Settings settings;

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestCreateEnrollmentTokenAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
        this.settings = settings;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_cluster/enrollment_token"),
            new Route(PUT, "/_cluster/enrollment_token"));
    }

    @Override
    public String getName() {
        return "cluster_enrolment_token_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (EnrollmentSettings.ENROLLMENT_ENABLED.get(settings) != true) {
            throw new IllegalStateException("Enrollment mode is not enabled.");
        }
        final CreateEnrollmentTokenRequest enrollmentTokenRequest = new CreateEnrollmentTokenRequest();
        final ActionType<CreateEnrollmentTokenResponse> action = CreateEnrollmentTokenAction.INSTANCE;
        return channel -> client.execute(action, enrollmentTokenRequest,
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(CreateEnrollmentTokenResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.field("enrollment_token", response.getEnrollmentToken().toString());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            }
        );
    }
}

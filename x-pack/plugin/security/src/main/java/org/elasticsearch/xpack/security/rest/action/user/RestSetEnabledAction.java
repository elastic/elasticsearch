/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for enabling and disabling users. The username is required and we use the path to determine if the user is being
 * enabled or disabled.
 */
public class RestSetEnabledAction extends SecurityBaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestSetEnabledAction.class));

    public RestSetEnabledAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, "/_security/user/{username}/_enable", this,
            POST, "/_xpack/security/user/{username}/_enable", deprecationLogger);
        controller.registerWithDeprecatedHandler(
            PUT, "/_security/user/{username}/_enable", this,
            PUT, "/_xpack/security/user/{username}/_enable", deprecationLogger);
        controller.registerWithDeprecatedHandler(
            POST, "/_security/user/{username}/_disable", this,
            POST, "/_xpack/security/user/{username}/_disable", deprecationLogger);
        controller.registerWithDeprecatedHandler(
            PUT, "/_security/user/{username}/_disable", this,
            PUT, "/_xpack/security/user/{username}/_disable", deprecationLogger);
    }

    @Override
    public String getName() {
        return "security_set_enabled_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean enabled = request.path().endsWith("_enable");
        assert enabled || request.path().endsWith("_disable");
        final String username = request.param("username");
        return channel -> new SetEnabledRequestBuilder(client)
            .username(username)
            .enabled(enabled)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(SetEnabledResponse setEnabledResponse, XContentBuilder builder) throws Exception {
                    return new BytesRestResponse(RestStatus.OK, builder.startObject().endObject());
                }
            });
    }
}

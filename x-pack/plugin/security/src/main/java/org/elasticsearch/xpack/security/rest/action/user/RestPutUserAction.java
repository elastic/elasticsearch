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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to add a User to the security index
 */
public class RestPutUserAction extends SecurityBaseRestHandler implements RestRequestFilter {

    private final Hasher passwordHasher;
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestPutUserAction.class));

    public RestPutUserAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
        passwordHasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.unmodifiableList(Arrays.asList(
            new ReplacedRoute(POST, "/_security/user/{username}",
                POST, "/_xpack/security/user/{username}", deprecationLogger),
            new ReplacedRoute(PUT, "/_security/user/{username}",
                PUT, "/_xpack/security/user/{username}", deprecationLogger)
        ));
    }

    @Override
    public String getName() {
        return "security_put_user_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        PutUserRequestBuilder requestBuilder = new PutUserRequestBuilder(client)
            .source(request.param("username"), request.requiredContent(), request.getXContentType(), passwordHasher)
            .setRefreshPolicy(request.param("refresh"));

        return channel -> requestBuilder.execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(PutUserResponse putUserResponse, XContentBuilder builder) throws Exception {
                putUserResponse.toXContent(builder, request);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    private static final Set<String> FILTERED_FIELDS = Collections.unmodifiableSet(Sets.newHashSet("password", "passwordHash"));

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}

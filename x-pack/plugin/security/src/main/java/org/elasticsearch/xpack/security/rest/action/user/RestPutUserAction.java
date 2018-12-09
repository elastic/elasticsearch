/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.security.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to add a User to the security index
 */
public class RestPutUserAction extends SecurityBaseRestHandler implements RestRequestFilter {
    private static final Logger logger = LogManager.getLogger(RestPutUserAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    private final Hasher passwordHasher;

    public RestPutUserAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        passwordHasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
        controller.registerHandler(POST, "/_xpack/security/user/{username}", this);
        controller.registerHandler(POST, "/_security/user/{username}", this);
        controller.registerHandler(PUT, "/_xpack/security/user/{username}", this);
        controller.registerHandler(PUT, "/_security/user/{username}", this);

        // @deprecated: Remove in 6.0
        controller.registerAsDeprecatedHandler(POST, "/_shield/user/{username}", this,
                                               "[POST /_shield/user/{username}] is deprecated! Use " +
                                               "[POST /_xpack/security/user/{username}] instead.",
                                               deprecationLogger);
        controller.registerAsDeprecatedHandler(PUT, "/_shield/user/{username}", this,
                                               "[PUT /_shield/user/{username}] is deprecated! Use " +
                                               "[PUT /_xpack/security/user/{username}] instead.",
                                               deprecationLogger);
    }

    @Override
    public String getName() {
        return "xpack_security_put_user_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        PutUserRequestBuilder requestBuilder = new SecurityClient(client)
            .preparePutUser(request.param("username"), request.requiredContent(), request.getXContentType(), passwordHasher)
                .setRefreshPolicy(request.param("refresh"));

        return channel -> requestBuilder.execute(new RestBuilderListener<PutUserResponse>(channel) {
            @Override
            public RestResponse buildResponse(PutUserResponse putUserResponse, XContentBuilder builder) throws Exception {
                builder.startObject()
                    .startObject("user"); // TODO in 7.0 remove wrapping of response in the user object and just return the object
                putUserResponse.toXContent(builder, request);
                builder.endObject();

                putUserResponse.toXContent(builder, request);
                return new BytesRestResponse(RestStatus.OK, builder.endObject());
            }
        });
    }

    private static final Set<String> FILTERED_FIELDS = Collections.unmodifiableSet(Sets.newHashSet("password", "passwordHash"));

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}

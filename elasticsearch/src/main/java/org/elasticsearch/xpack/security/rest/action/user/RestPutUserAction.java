/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.RestRequestFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to add a User to the security index
 */
public class RestPutUserAction extends BaseRestHandler implements RestRequestFilter {

    public RestPutUserAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_xpack/security/user/{username}", this);
        controller.registerHandler(PUT, "/_xpack/security/user/{username}", this);

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
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PutUserRequestBuilder requestBuilder = new SecurityClient(client)
                .preparePutUser(request.param("username"), request.content(), request.getXContentType())
                .setRefreshPolicy(request.param("refresh"));

        return channel -> requestBuilder.execute(new RestBuilderListener<PutUserResponse>(channel) {
            @Override
            public RestResponse buildResponse(PutUserResponse putUserResponse, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK,
                        builder.startObject()
                                .field("user", putUserResponse)
                                .endObject());
            }
        });
    }

    private static final Set<String> FILTERED_FIELDS = Collections.unmodifiableSet(Sets.newHashSet("password", "passwordHash"));

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}

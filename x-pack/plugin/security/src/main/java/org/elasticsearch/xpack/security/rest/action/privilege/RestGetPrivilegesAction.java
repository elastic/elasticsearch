/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.privilege;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to retrieve an application privilege from the security index
 */
public class RestGetPrivilegesAction extends SecurityBaseRestHandler {

    public RestGetPrivilegesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_security/privilege/")
                .replaces(GET, "/_xpack/security/privilege/", RestApiVersion.V_7).build(),
            Route.builder(GET, "/_security/privilege/{application}")
                .replaces(GET, "/_xpack/security/privilege/{application}", RestApiVersion.V_7).build(),
            Route.builder(GET, "/_security/privilege/{application}/{privilege}")
                .replaces(GET, "/_xpack/security/privilege/{application}/{privilege}", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_get_privileges_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String application = request.param("application");
        final String[] privileges = request.paramAsStringArray("privilege", Strings.EMPTY_ARRAY);

        final GetPrivilegesRequestBuilder requestBuilder = new GetPrivilegesRequestBuilder(client);
        if (Strings.hasText(application)) {
            requestBuilder.application(application).privileges(privileges);
        }

        return channel -> requestBuilder.execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetPrivilegesResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();

                final Map<String, Set<ApplicationPrivilegeDescriptor>> appPrivs = groupByApplicationName(response.privileges());
                for (String app : appPrivs.keySet()) {
                    builder.startObject(app);
                    for (ApplicationPrivilegeDescriptor privilege : appPrivs.get(app)) {
                        builder.field(privilege.getName(), privilege);
                    }
                    builder.endObject();
                }

                builder.endObject();

                // if the user asked for specific privileges, but none of them were found
                // we'll return an empty result and 404 status code
                if (privileges.length != 0 && response.isEmpty()) {
                    return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                }

                // either the user asked for all privileges, or at least one of the privileges
                // was found
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    static Map<String, Set<ApplicationPrivilegeDescriptor>> groupByApplicationName(ApplicationPrivilegeDescriptor[] privileges) {
        return Arrays.stream(privileges).collect(Collectors.toMap(
            ApplicationPrivilegeDescriptor::getApplication,
            Collections::singleton,
            Sets::union
        ));
    }
}

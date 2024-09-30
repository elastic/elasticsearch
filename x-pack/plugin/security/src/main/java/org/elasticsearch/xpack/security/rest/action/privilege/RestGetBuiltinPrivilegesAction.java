/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponseTranslator;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to retrieve built-in (cluster/index) privileges
 */
@ServerlessScope(Scope.PUBLIC)
public class RestGetBuiltinPrivilegesAction extends SecurityBaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestGetBuiltinPrivilegesAction.class);
    private final GetBuiltinPrivilegesResponseTranslator responseTranslator;

    public RestGetBuiltinPrivilegesAction(
        Settings settings,
        XPackLicenseState licenseState,
        GetBuiltinPrivilegesResponseTranslator responseTranslator
    ) {
        super(settings, licenseState);
        this.responseTranslator = responseTranslator;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/privilege/_builtin"));
    }

    @Override
    public String getName() {
        return "security_get_builtin_privileges_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(
            GetBuiltinPrivilegesAction.INSTANCE,
            new GetBuiltinPrivilegesRequest(),
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetBuiltinPrivilegesResponse response, XContentBuilder builder) throws Exception {
                    final var translatedResponse = responseTranslator.translate(response);
                    builder.startObject();
                    builder.array("cluster", translatedResponse.getClusterPrivileges());
                    builder.array("index", translatedResponse.getIndexPrivileges());
                    String[] remoteClusterPrivileges = translatedResponse.getRemoteClusterPrivileges();
                    if (remoteClusterPrivileges.length > 0) { // remote clusters are not supported in stateless mode, so hide entirely
                        builder.array("remote_cluster", remoteClusterPrivileges);
                    }
                    builder.endObject();
                    return new RestResponse(RestStatus.OK, builder);
                }
            }
        );
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        final boolean shouldRestrictForServerless = shouldRestrictForServerless(request);
        assert false == shouldRestrictForServerless || DiscoveryNode.isStateless(settings);
        if (false == shouldRestrictForServerless) {
            return super.innerCheckFeatureAvailable(request);
        }
        // This is a temporary hack: we are re-using the native roles setting as an overall feature flag for custom roles.
        final Boolean nativeRolesEnabled = settings.getAsBoolean(NativeRolesStore.NATIVE_ROLES_ENABLED, true);
        if (nativeRolesEnabled == false) {
            logger.debug(
                "Attempt to call [{} {}] but [{}] is [{}]",
                request.method(),
                request.rawPath(),
                NativeRolesStore.NATIVE_ROLES_ENABLED,
                settings.get(NativeRolesStore.NATIVE_ROLES_ENABLED)
            );
            return new ElasticsearchStatusException("This API is not enabled on this Elasticsearch instance", RestStatus.GONE);
        } else {
            return null;
        }
    }

    private boolean shouldRestrictForServerless(RestRequest request) {
        return request.isServerlessRequest() && false == request.isOperatorRequest();
    }
}

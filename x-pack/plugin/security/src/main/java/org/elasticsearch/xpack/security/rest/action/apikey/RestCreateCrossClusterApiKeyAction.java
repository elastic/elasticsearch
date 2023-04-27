/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Rest action to create an API key specific to cross cluster access via the dedicate remote cluster server port
 */
public final class RestCreateCrossClusterApiKeyAction extends ApiKeyBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "cross_cluster_api_key_request_payload",
        false,
        (args, v) -> new Payload(
            (String) args[0],
            args[1] == null ? List.of() : (List<RoleDescriptor.IndicesPrivileges>) args[1],
            args[2] == null ? List.of() : (List<RoleDescriptor.IndicesPrivileges>) args[2],
            TimeValue.parseTimeValue((String) args[3], null, "expiration"),
            (Map<String, Object>) args[4]
        )
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptor.parseIndexWithPrivileges("cross_cluster", new String[] { "read" }, p),
            new ParseField("search")
        );
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptor.parseIndexWithPrivileges("cross_cluster", new String[] { "read" }, p),
            new ParseField("replication")
        );
        PARSER.declareString(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
    }

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestCreateCrossClusterApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/cross_cluster/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_create_cross_cluster_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final Payload payload = PARSER.parse(request.contentParser(), null);
        System.out.println("PAYLOAD IS " + payload);

        final CreateApiKeyRequest createApiKeyRequest = payload.toCreateApiKeyRequest();
        String refresh = request.param("refresh");
        if (refresh != null) {
            createApiKeyRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(request.param("refresh")));
        }
        return channel -> client.execute(
            CreateCrossClusterApiKeyAction.INSTANCE,
            createApiKeyRequest,
            new RestToXContentListener<>(channel)
        );
    }

    record Payload(
        String name,
        List<RoleDescriptor.IndicesPrivileges> search,
        List<RoleDescriptor.IndicesPrivileges> replication,
        TimeValue expiration,
        Map<String, Object> metadata
    ) {
        public CreateApiKeyRequest toCreateApiKeyRequest() {
            final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest();
            createApiKeyRequest.setName(name);
            createApiKeyRequest.setType(ApiKey.Type.CROSS_CLUSTER);
            createApiKeyRequest.setExpiration(expiration);
            createApiKeyRequest.setMetadata(metadata);

            final String[] clusterPrivileges;
            if (search.isEmpty() && replication.isEmpty()) {
                throw new IllegalArgumentException("must specify non-empty indices for either [search] or [replication]");
            } else if (search.isEmpty()) {
                clusterPrivileges = new String[] { "cross_cluster_access" };
            } else if (replication.isEmpty()) {
                clusterPrivileges = new String[] { "cross_cluster_access" };
            } else {
                clusterPrivileges = new String[] { "cross_cluster_access", "cross_cluster_access" };
            }
            final RoleDescriptor roleDescriptor = new RoleDescriptor(
                name,
                clusterPrivileges,
                CollectionUtils.concatLists(search, replication).toArray(RoleDescriptor.IndicesPrivileges[]::new),
                null
            );
            createApiKeyRequest.setRoleDescriptors(List.of(roleDescriptor));

            return createApiKeyRequest;
        }
    }
}

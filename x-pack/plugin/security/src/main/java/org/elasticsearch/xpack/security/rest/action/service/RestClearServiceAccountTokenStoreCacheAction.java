/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClearServiceAccountTokenStoreCacheAction extends SecurityBaseRestHandler {

    public RestClearServiceAccountTokenStoreCacheAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/service/{namespace}/{service}/credential/token/{name}/_clear_cache"));
    }

    @Override
    public String getName() {
        return "xpack_security_clear_service_account_token_store_cache";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String namespace = request.param("namespace");
        final String service = request.param("service");
        String[] tokenNames = request.paramAsStringArrayOrEmptyIfAll("name");

        ClearSecurityCacheRequest req = new ClearSecurityCacheRequest().cacheName("service");
        if (tokenNames.length == 0) {
            // This is the wildcard case for tokenNames
            req.keys(namespace + "/" + service + "/");
        } else {
            final Set<String> qualifiedTokenNames = new HashSet<>(tokenNames.length);
            for (String name: tokenNames) {
                if (false == Validation.isValidServiceAccountTokenName(name)) {
                    throw new IllegalArgumentException(Validation.formatInvalidServiceTokenNameErrorMessage(name));
                }
                qualifiedTokenNames.add(namespace + "/" + service + "/" + name);
            }
            req.keys(qualifiedTokenNames.toArray(String[]::new));
        }
        return channel -> client.execute(ClearSecurityCacheAction.INSTANCE, req, new RestActions.NodesResponseRestListener<>(channel));
    }
}

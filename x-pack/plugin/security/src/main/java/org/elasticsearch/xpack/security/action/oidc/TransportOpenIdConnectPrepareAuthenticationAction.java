/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.oidc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm;

import java.util.List;
import java.util.stream.Collectors;

public class TransportOpenIdConnectPrepareAuthenticationAction extends HandledTransportAction<OpenIdConnectPrepareAuthenticationRequest,
    OpenIdConnectPrepareAuthenticationResponse> {

    private final Realms realms;

    @Inject
    public TransportOpenIdConnectPrepareAuthenticationAction(TransportService transportService,
                                                             ActionFilters actionFilters, Realms realms) {
        super(OpenIdConnectPrepareAuthenticationAction.NAME, transportService, actionFilters, OpenIdConnectPrepareAuthenticationRequest::new);
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, OpenIdConnectPrepareAuthenticationRequest request,
                             ActionListener<OpenIdConnectPrepareAuthenticationResponse> listener) {
        List<OpenIdConnectRealm> realms = this.realms.stream()
            .filter(r -> r instanceof OpenIdConnectRealm)
            .map(r -> (OpenIdConnectRealm) r)
            .filter(r -> r.name().equals(request.getRealmName()))
            .collect(Collectors.toList());
        if (realms.isEmpty()) {
            listener.onFailure(new ElasticsearchSecurityException("Cannot find OIDC realm with name [{}]", request.getRealmName()));
        } else if (realms.size() > 1) {
            // Can't define multiple realms with the same name in configuration, but check, still.
            listener.onFailure(new ElasticsearchSecurityException("Found multiple ([{}]) OIDC realms with name [{}]", realms.size(),
                request.getRealmName()));
        } else if (Strings.isNullOrEmpty(request.getState())) {
            listener.onFailure(new ElasticsearchSecurityException("State parameter cannot be empty"));
        } else {
            prepareAuthenticationResponse(realms.get(0), request.getState(), listener);
        }
    }

    private void prepareAuthenticationResponse(OpenIdConnectRealm realm, String state,
                                               ActionListener<OpenIdConnectPrepareAuthenticationResponse> listener) {
        //TODO: Generate the Authorization URL from the OP metadata and the configuration
        final String authorizationEndpointURl = "";
        listener.onResponse(new OpenIdConnectPrepareAuthenticationResponse(authorizationEndpointURl, state));
    }
}

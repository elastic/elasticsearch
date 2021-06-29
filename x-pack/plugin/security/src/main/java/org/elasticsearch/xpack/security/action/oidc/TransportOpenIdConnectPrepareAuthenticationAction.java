/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.oidc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.Realm;
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
        super(OpenIdConnectPrepareAuthenticationAction.NAME, transportService, actionFilters,
            (Writeable.Reader<OpenIdConnectPrepareAuthenticationRequest>) OpenIdConnectPrepareAuthenticationRequest::new);
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, OpenIdConnectPrepareAuthenticationRequest request,
                             ActionListener<OpenIdConnectPrepareAuthenticationResponse> listener) {
        Realm realm = null;
        if (Strings.hasText(request.getIssuer())) {
            List<OpenIdConnectRealm> matchingRealms = this.realms.stream()
                .filter(r -> r instanceof OpenIdConnectRealm && ((OpenIdConnectRealm) r).isIssuerValid(request.getIssuer()))
                .map(r -> (OpenIdConnectRealm) r)
                .collect(Collectors.toList());
            if (matchingRealms.isEmpty()) {
                listener.onFailure(
                    new ElasticsearchSecurityException("Cannot find OpenID Connect realm with issuer [{}]", request.getIssuer()));
            } else if (matchingRealms.size() > 1) {
                listener.onFailure(
                    new ElasticsearchSecurityException("Found multiple OpenID Connect realm with issuer [{}]", request.getIssuer()));
            } else {
                realm = matchingRealms.get(0);
            }
        } else if (Strings.hasText(request.getRealmName())) {
            realm = this.realms.realm(request.getRealmName());
        }

        if (realm instanceof OpenIdConnectRealm) {
            prepareAuthenticationResponse((OpenIdConnectRealm) realm, request.getState(), request.getNonce(), request.getLoginHint(),
                listener);
        } else {
            listener.onFailure(
                new ElasticsearchSecurityException("Cannot find OpenID Connect realm with name [{}]", request.getRealmName()));
        }
    }

    private void prepareAuthenticationResponse(OpenIdConnectRealm realm, String state, String nonce, String loginHint,
                                               ActionListener<OpenIdConnectPrepareAuthenticationResponse> listener) {
        try {
            final OpenIdConnectPrepareAuthenticationResponse authenticationResponse =
                realm.buildAuthenticationRequestUri(state, nonce, loginHint);
            listener.onResponse(authenticationResponse);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.oidc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
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
        final Realm realm = this.realms.realm(request.getRealmName());
        if (null == realm || realm instanceof OpenIdConnectRealm == false) {
            listener.onFailure(
                new ElasticsearchSecurityException("Cannot find OpenID Connect realm with name [{}]", request.getRealmName()));
        } else {
            prepareAuthenticationResponse((OpenIdConnectRealm) realm, listener);
        }
    }

    private void prepareAuthenticationResponse(OpenIdConnectRealm realm,
                                               ActionListener<OpenIdConnectPrepareAuthenticationResponse> listener) {
        try {
            final OpenIdConnectPrepareAuthenticationResponse authenticationResponse = realm.buildAuthenticationRequestUri();
            listener.onResponse(authenticationResponse);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        }
    }
}

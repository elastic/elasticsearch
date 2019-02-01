/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.oidc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm;

import java.io.IOException;

/**
 * Transport action responsible for generating an OpenID connect logout request to be sent to an OpenID Connect Provider
 */
public class TransportOpenIdConnectLogoutAction extends HandledTransportAction<OpenIdConnectLogoutRequest, OpenIdConnectLogoutResponse> {

    private final Realms realms;
    private final TokenService tokenService;
    private static final Logger logger = LogManager.getLogger(TransportOpenIdConnectLogoutAction.class);

    @Inject
    public TransportOpenIdConnectLogoutAction(TransportService transportService, ActionFilters actionFilters, Realms realms,
                                              TokenService tokenService) {
        super(OpenIdConnectLogoutAction.NAME, transportService, actionFilters,
            (Writeable.Reader<OpenIdConnectLogoutRequest>) OpenIdConnectLogoutRequest::new);
        this.realms = realms;
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, OpenIdConnectLogoutRequest request, ActionListener<OpenIdConnectLogoutResponse> listener) {
        invalidateRefreshToken(request.getRefreshToken(), ActionListener.wrap(ignore -> {
            try {
                final String token = request.getToken();
                tokenService.getAuthenticationAndMetaData(token, ActionListener.wrap(
                    tuple -> {
                        Authentication authentication = tuple.v1();
                        tokenService.invalidateAccessToken(token, ActionListener.wrap(
                            result -> {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("OpenID Connect Logout for user [{}] and token [{}...{}]",
                                        authentication.getUser().principal(),
                                        token.substring(0, 8),
                                        token.substring(token.length() - 8));
                                }
                                OpenIdConnectLogoutResponse response = buildResponse(authentication);
                                listener.onResponse(response);
                            }, listener::onFailure)
                        );
                    }, listener::onFailure));
            } catch (IOException e) {
                logger.debug("Internal error during OpenID Connect Logout");
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    private OpenIdConnectLogoutResponse buildResponse(Authentication authentication) {
        final Authentication.RealmRef ref = authentication.getAuthenticatedBy();
        if (ref == null || Strings.isNullOrEmpty(ref.getName())) {
            throw new ElasticsearchSecurityException("Authentication {} has no authenticating realm", authentication);
        }
        final Realm realm = this.realms.realm(authentication.getAuthenticatedBy().getName());
        if (realm == null) {
            throw new ElasticsearchSecurityException("Authenticating realm {} does not exist", ref.getName());
        }
        if (realm instanceof OpenIdConnectRealm) {
            return ((OpenIdConnectRealm) realm).buildLogoutResponse();
        } else {
            throw new ElasticsearchSecurityException("Authenticating realm {} is not a SAML realm", realm);
        }
    }

    private void invalidateRefreshToken(String refreshToken, ActionListener<TokensInvalidationResult> listener) {
        if (refreshToken == null) {
            listener.onResponse(null);
        } else {
            tokenService.invalidateRefreshToken(refreshToken, listener);
        }
    }
}

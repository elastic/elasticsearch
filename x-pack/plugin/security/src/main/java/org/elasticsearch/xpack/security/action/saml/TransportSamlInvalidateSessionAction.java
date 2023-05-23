/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionResponse;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.UserToken;
import org.elasticsearch.xpack.security.authc.saml.SamlLogoutRequestHandler;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRedirect;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;
import org.opensaml.saml.saml2.core.LogoutResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.findSamlRealms;

/**
 * Transport action responsible for taking a SAML {@code LogoutRequest} and invalidating any associated Security Tokens
 */
public final class TransportSamlInvalidateSessionAction extends HandledTransportAction<
    SamlInvalidateSessionRequest,
    SamlInvalidateSessionResponse> {

    private final TokenService tokenService;
    private final Realms realms;

    @Inject
    public TransportSamlInvalidateSessionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        TokenService tokenService,
        Realms realms
    ) {
        super(SamlInvalidateSessionAction.NAME, transportService, actionFilters, SamlInvalidateSessionRequest::new);
        this.tokenService = tokenService;
        this.realms = realms;
    }

    @Override
    protected void doExecute(Task task, SamlInvalidateSessionRequest request, ActionListener<SamlInvalidateSessionResponse> listener) {
        List<SamlRealm> realms = findSamlRealms(this.realms, request.getRealmName(), request.getAssertionConsumerServiceURL());
        if (realms.isEmpty()) {
            listener.onFailure(SamlUtils.samlException("Cannot find any matching realm for [{}]", request));
        } else if (realms.size() > 1) {
            listener.onFailure(SamlUtils.samlException("Found multiple matching realms [{}] for [{}]", realms, request));
        } else {
            invalidateSession(realms.get(0), request, listener);
        }
    }

    private void invalidateSession(
        SamlRealm realm,
        SamlInvalidateSessionRequest request,
        ActionListener<SamlInvalidateSessionResponse> listener
    ) {
        try {
            final SamlLogoutRequestHandler.Result result = realm.getLogoutHandler().parseFromQueryString(request.getQueryString());
            findAndInvalidateTokens(
                realm,
                result,
                ActionListener.wrap(
                    count -> listener.onResponse(
                        new SamlInvalidateSessionResponse(realm.name(), count, buildLogoutResponseUrl(realm, result))
                    ),
                    listener::onFailure
                )
            );
        } catch (ElasticsearchSecurityException e) {
            logger.info("Failed to invalidate SAML session", e);
            listener.onFailure(e);
        }
    }

    private String buildLogoutResponseUrl(SamlRealm realm, SamlLogoutRequestHandler.Result result) {
        final LogoutResponse response = realm.buildLogoutResponse(result.getRequestId());
        return new SamlRedirect(response, realm.getSigningConfiguration()).getRedirectUrl(result.getRelayState());
    }

    private void findAndInvalidateTokens(SamlRealm realm, SamlLogoutRequestHandler.Result result, ActionListener<Integer> listener) {
        final Map<String, Object> tokenMetadata = realm.createTokenMetadata(result.getNameId(), result.getSession());
        if (Strings.isNullOrEmpty((String) tokenMetadata.get(SamlRealm.TOKEN_METADATA_NAMEID_VALUE))) {
            // If we don't have a valid name-id to match against, don't do anything
            logger.debug("Logout request [{}] has no NameID value, so cannot invalidate any sessions", result);
            listener.onResponse(0);
            return;
        }

        tokenService.findActiveTokensForRealm(realm.name(), containsMetadata(tokenMetadata), ActionListener.wrap(tokens -> {
            logger.debug("Found [{}] token pairs to invalidate for SAML metadata [{}]", tokens.size(), tokenMetadata);
            if (tokens.isEmpty()) {
                listener.onResponse(0);
            } else {
                GroupedActionListener<TokensInvalidationResult> groupedListener = new GroupedActionListener<>(
                    tokens.size(),
                    ActionListener.wrap(collection -> listener.onResponse(collection.size()), listener::onFailure)
                );
                tokens.forEach(tuple -> invalidateTokenPair(tuple, groupedListener));
            }
        }, listener::onFailure));
    }

    private void invalidateTokenPair(Tuple<UserToken, String> tokenPair, ActionListener<TokensInvalidationResult> listener) {
        // Invalidate the refresh token first, so the client doesn't trigger a refresh once the access token is invalidated
        if (tokenPair.v2() != null) {
            tokenService.invalidateRefreshToken(
                tokenPair.v2(),
                ActionListener.wrap(ignore -> invalidateAccessToken(tokenPair.v1(), listener), listener::onFailure)
            );
        } else {
            invalidateAccessToken(tokenPair.v1(), listener);
        }
    }

    private void invalidateAccessToken(UserToken userToken, ActionListener<TokensInvalidationResult> listener) {
        tokenService.invalidateAccessToken(userToken, ActionListener.wrap(listener::onResponse, e -> {
            logger.info("Failed to invalidate SAML access_token [{}] - {}", userToken.getId(), e.toString());
            listener.onFailure(e);
        }));
    }

    private Predicate<Map<String, Object>> containsMetadata(Map<String, Object> requiredMetadata) {
        return source -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> actualMetadata = (Map<String, Object>) source.get("metadata");
            return requiredMetadata.entrySet().stream().allMatch(e -> Objects.equals(actualMetadata.get(e.getKey()), e.getValue()));
        };
    }

}

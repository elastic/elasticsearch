/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionResponse;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
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

    private static final Logger LOGGER = LogManager.getLogger(TransportSamlInvalidateSessionAction.class);
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
            LOGGER.info("Failed to invalidate SAML session", e);
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
            LOGGER.debug("Logout request [{}] has no NameID value, so cannot invalidate any sessions", result);
            listener.onResponse(0);
            return;
        }

        tokenService.findActiveTokensForRealm(realm.name(), containsMetadata(tokenMetadata), ActionListener.wrap(tokens -> {
            LOGGER.debug("Found [{}] token pairs to invalidate for SAML metadata [{}]", tokens.size(), tokenMetadata);
            if (tokens.isEmpty()) {
                listener.onResponse(0);
            } else {
                tokenService.invalidateAllTokens(tokens, ActionListener.wrap(tokensInvalidationResult -> {
                    if (LOGGER.isInfoEnabled() && tokensInvalidationResult.getErrors().isEmpty() == false) {
                        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                            tokensInvalidationResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
                            LOGGER.info("Failed to invalidate some SAML access or refresh tokens {}", Strings.toString(builder));
                        }
                    }
                    // return only the total of active tokens for users of the realm, i.e. not the number of actually invalidated tokens
                    listener.onResponse(tokens.size());
                }, listener::onFailure));
            }
        }, listener::onFailure));
    }

    private Predicate<Map<String, Object>> containsMetadata(Map<String, Object> requiredMetadata) {
        return source -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> actualMetadata = (Map<String, Object>) source.get("metadata");
            return requiredMetadata.entrySet().stream().allMatch(e -> Objects.equals(actualMetadata.get(e.getKey()), e.getValue()));
        };
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.saml.SamlNameId;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRedirect;
import org.elasticsearch.xpack.security.authc.saml.SamlUtils;
import org.opensaml.saml.saml2.core.LogoutRequest;

import java.util.Map;

/**
 * Transport action responsible for generating a SAML {@code &lt;LogoutRequest&gt;} as a redirect binding URL.
 */
public final class TransportSamlLogoutAction extends HandledTransportAction<SamlLogoutRequest, SamlLogoutResponse> {

    private final Realms realms;
    private final TokenService tokenService;

    @Inject
    public TransportSamlLogoutAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Realms realms,
        TokenService tokenService
    ) {
        super(SamlLogoutAction.NAME, transportService, actionFilters, SamlLogoutRequest::new);
        this.realms = realms;
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, SamlLogoutRequest request, ActionListener<SamlLogoutResponse> listener) {
        invalidateRefreshToken(request.getRefreshToken(), listener.delegateFailureAndWrap((delegate, ignore) -> {
            try {
                final String token = request.getToken();
                tokenService.getAuthenticationAndMetadata(token, delegate.delegateFailureAndWrap((delegate2, tuple) -> {
                    Authentication authentication = tuple.v1();
                    assert false == authentication.isRunAs() : "saml realm authentication cannot have run-as";
                    final Map<String, Object> tokenMetadata = tuple.v2();
                    SamlLogoutResponse response = buildResponse(authentication, tokenMetadata);
                    tokenService.invalidateAccessToken(token, delegate2.delegateFailureAndWrap((delegate3, ecreated) -> {
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                "SAML Logout User [{}], Token [{}...{}]",
                                authentication.getEffectiveSubject().getUser().principal(),
                                token.substring(0, 8),
                                token.substring(token.length() - 8)
                            );
                        }
                        delegate3.onResponse(response);
                    }));
                }));
            } catch (ElasticsearchException e) {
                logger.debug("Internal exception during SAML logout", e);
                delegate.onFailure(e);
            }
        }));
    }

    private void invalidateRefreshToken(String refreshToken, ActionListener<TokensInvalidationResult> listener) {
        if (refreshToken == null) {
            listener.onResponse(null);
        } else {
            tokenService.invalidateRefreshToken(refreshToken, listener);
        }
    }

    private SamlLogoutResponse buildResponse(Authentication authentication, Map<String, Object> tokenMetadata) {
        if (authentication == null) {
            throw SamlUtils.samlException("No active authentication");
        }
        final User user = authentication.getEffectiveSubject().getUser();
        if (user == null) {
            throw SamlUtils.samlException("No active user");
        }

        final SamlRealm realm = findRealm(authentication);
        final String tokenRealm = getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_REALM);
        if (realm.name().equals(tokenRealm) == false) {
            throw SamlUtils.samlException("Authenticating realm [{}] does not match token realm [{}]", realm, tokenRealm);
        }

        final SamlNameId nameId = new SamlNameId(
            getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_NAMEID_FORMAT),
            getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_NAMEID_VALUE),
            getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_NAMEID_QUALIFIER),
            getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_NAMEID_SP_QUALIFIER),
            getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_NAMEID_SP_PROVIDED_ID)
        );
        final String session = getMetadataString(tokenMetadata, SamlRealm.TOKEN_METADATA_SESSION);
        final LogoutRequest logout = realm.buildLogoutRequest(nameId.asXml(), session);
        if (logout == null) {
            return new SamlLogoutResponse(null, null);
        }
        final String uri = new SamlRedirect(logout, realm.getSigningConfiguration()).getRedirectUrl();
        return new SamlLogoutResponse(logout.getID(), uri);
    }

    private String getMetadataString(Map<String, Object> metadata, String key) {
        final Object value = metadata.get(key);
        if (value == null) {
            if (metadata.containsKey(key)) {
                return null;
            }
            throw SamlUtils.samlException("Access token does not have SAML metadata [{}]", key);
        }
        if (value instanceof String) {
            return (String) value;
        } else {
            throw SamlUtils.samlException("In access token, SAML metadata [{}] is [{}] rather than String", key, value.getClass());
        }
    }

    private SamlRealm findRealm(Authentication authentication) {
        final Authentication.RealmRef ref = authentication.getEffectiveSubject().getRealm();
        if (ref == null || Strings.isNullOrEmpty(ref.getName())) {
            throw SamlUtils.samlException("Authentication {} has no effective realm", authentication);
        }
        final Realm realm = realms.realm(ref.getName());
        if (realm == null) {
            throw SamlUtils.samlException("Authenticating realm {} does not exist", ref.getName());
        }
        if (realm instanceof SamlRealm) {
            return (SamlRealm) realm;
        } else {
            throw SamlUtils.samlException("Authenticating realm {} is not a SAML realm", realm);
        }
    }

}

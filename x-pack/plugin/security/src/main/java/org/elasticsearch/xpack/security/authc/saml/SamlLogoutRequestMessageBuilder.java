/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.time.Clock;

import org.elasticsearch.common.Strings;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.SessionIndex;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.SSODescriptor;

/**
 * Constructs {@code &lt;LogoutRequest&lt;} objects for use in a SAML Single-Sign-Out flow.
 */
class SamlLogoutRequestMessageBuilder extends SamlMessageBuilder {
    private final NameID nameId;
    private final String session;

    SamlLogoutRequestMessageBuilder(Clock clock, SpConfiguration serviceProvider, EntityDescriptor identityProvider,
                                    NameID nameId, String session) {
        super(identityProvider, serviceProvider, clock);
        this.nameId = nameId;
        this.session = session;
    }

    LogoutRequest build() {
        final String logoutUrl = getLogoutUrl();
        if (Strings.isNullOrEmpty(logoutUrl)) {
            logger.debug("Cannot perform logout because the IDP {} does not provide a logout service", identityProvider.getEntityID());
            return null;
        }

        final SessionIndex sessionIndex = SamlUtils.buildObject(SessionIndex.class, SessionIndex.DEFAULT_ELEMENT_NAME);
        sessionIndex.setValue(session);

        final Issuer issuer = buildIssuer();

        final LogoutRequest request = SamlUtils.buildObject(LogoutRequest.class, LogoutRequest.DEFAULT_ELEMENT_NAME);
        request.setID(buildId());
        request.setIssueInstant(clock.instant());
        request.setDestination(logoutUrl);
        request.setNameID(nameId);
        request.getSessionIndexes().add(sessionIndex);
        request.setIssuer(issuer);
        return request;
    }

    protected String getLogoutUrl() {
        return getIdentityProviderEndpoint(SAMLConstants.SAML2_REDIRECT_BINDING_URI, SSODescriptor::getSingleLogoutServices);
    }

}

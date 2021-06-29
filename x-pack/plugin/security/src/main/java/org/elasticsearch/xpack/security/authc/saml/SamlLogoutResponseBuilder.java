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
import org.opensaml.saml.saml2.core.LogoutResponse;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.SSODescriptor;

import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;

/**
 * Constructs {@code &lt;LogoutRespond&lt;} objects for use in a SAML Single-Sign-Out flow.
 */
class SamlLogoutResponseBuilder extends SamlMessageBuilder {
    private final String inResponseTo;
    private final String statusValue;

    SamlLogoutResponseBuilder(Clock clock, SpConfiguration serviceProvider, EntityDescriptor identityProvider,
                              String inResponseTo, String statusValue) {
        super(identityProvider, serviceProvider, clock);
        this.inResponseTo = inResponseTo;
        this.statusValue = statusValue;
    }

    LogoutResponse build() {
        final String destination = getLogoutUrl();
        if (Strings.isNullOrEmpty(destination)) {
            throw samlException("Cannot send LogoutResponse because the IDP {} does not provide a logout service",
                    identityProvider.getEntityID());
        }

        final LogoutResponse res = SamlUtils.buildObject(LogoutResponse.class, LogoutResponse.DEFAULT_ELEMENT_NAME);
        res.setID(buildId());
        res.setIssueInstant(now());
        res.setDestination(destination);
        res.setIssuer(buildIssuer());
        res.setInResponseTo(inResponseTo);

        final Status status = SamlUtils.buildObject(Status.class, Status.DEFAULT_ELEMENT_NAME);
        final StatusCode statusCode= SamlUtils.buildObject(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        statusCode.setValue(this.statusValue);
        status.setStatusCode(statusCode);
        res.setStatus(status);
        return res;
    }

    protected String getLogoutUrl() {
        return getIdentityProviderEndpoint(SAMLConstants.SAML2_REDIRECT_BINDING_URI, SSODescriptor::getSingleLogoutServices);
    }

}

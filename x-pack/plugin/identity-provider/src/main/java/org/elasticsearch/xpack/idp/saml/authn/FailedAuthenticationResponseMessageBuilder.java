/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.StatusMessage;

import java.time.Clock;

public class FailedAuthenticationResponseMessageBuilder {

    private final Clock clock;
    private final SamlFactory samlFactory;
    private final SamlIdentityProvider idp;
    private String primaryStatusCode;
    private String statusMessage;
    private String acsUrl;
    private String inResponseTo;
    private String secondaryStatusCode;

    public FailedAuthenticationResponseMessageBuilder(SamlFactory samlFactory, Clock clock, SamlIdentityProvider idp) {
        SamlInit.initialize();
        this.samlFactory = samlFactory;
        this.clock = clock;
        this.idp = idp;
        this.primaryStatusCode = StatusCode.REQUESTER;
    }

    public FailedAuthenticationResponseMessageBuilder setAcsUrl(String acsUrl) {
        this.acsUrl = acsUrl;
        return this;
    }

    public FailedAuthenticationResponseMessageBuilder setPrimaryStatusCode(String primaryStatusCode) {
        this.primaryStatusCode = primaryStatusCode;
        return this;
    }

    public FailedAuthenticationResponseMessageBuilder setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
        return this;
    }

    public FailedAuthenticationResponseMessageBuilder setSecondaryStatusCode(String secondaryStatusCode) {
        this.secondaryStatusCode = secondaryStatusCode;
        return this;
    }

    public FailedAuthenticationResponseMessageBuilder setInResponseTo(String inResponseTo) {
        this.inResponseTo = inResponseTo;
        return this;
    }

    public Response build() {
        final DateTime now = now();
        final Response response = samlFactory.object(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setID(samlFactory.secureIdentifier());
        response.setInResponseTo(inResponseTo);
        response.setIssuer(buildIssuer());
        response.setIssueInstant(now);
        response.setStatus(buildStatus());
        response.setDestination(acsUrl);
        return response;
    }

    private Issuer buildIssuer() {
        final Issuer issuer = samlFactory.object(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(this.idp.getEntityId());
        return issuer;
    }

    private Status buildStatus() {
        final StatusCode firstLevelCode = samlFactory.object(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        firstLevelCode.setValue(primaryStatusCode);

        if (Strings.hasText(secondaryStatusCode)) {
            final StatusCode secondLevelCode = samlFactory.object(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
            secondLevelCode.setValue(secondaryStatusCode);
            firstLevelCode.setStatusCode(secondLevelCode);
        }

        final Status status = samlFactory.object(Status.class, Status.DEFAULT_ELEMENT_NAME);
        if (Strings.hasText(statusMessage)) {
            final StatusMessage firstLevelMessage = samlFactory.object(StatusMessage.class, StatusMessage.DEFAULT_ELEMENT_NAME);
            firstLevelMessage.setMessage(statusMessage);
            status.setStatusMessage(firstLevelMessage);
        }
        status.setStatusCode(firstLevelCode);
        return status;
    }

    private DateTime now() {
        return new DateTime(clock.millis(), DateTimeZone.UTC);
    }
}

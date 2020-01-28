/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.core.xml.schema.XSString;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.AttributeValue;
import org.opensaml.saml.saml2.core.Audience;
import org.opensaml.saml.saml2.core.AudienceRestriction;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.Conditions;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.NameIDType;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.Subject;

import java.time.Clock;
import java.util.Collection;

/**
 * Builds SAML 2.0 {@link Response} objects for successful authentication results.
 */
public class SuccessfulAuthenticationResponseMessageBuilder {

    private final SamlFactory samlFactory;
    private final Clock clock;
    private final SamlIdentityProvider idp;

    public SuccessfulAuthenticationResponseMessageBuilder(SamlFactory samlFactory, Clock clock, SamlIdentityProvider idp) {
        this.samlFactory = samlFactory;
        this.clock = clock;
        this.idp = idp;
    }

    public Response build(UserServiceAuthentication user, @Nullable AuthnRequest request) {
        final DateTime now = now();
        final SamlServiceProvider serviceProvider = user.getServiceProvider();

        final Response response = samlFactory.object(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setID(samlFactory.secureIdentifier());
        if (request != null) {
            response.setInResponseTo(request.getID());
        }
        response.setIssuer(buildIssuer());
        response.setIssueInstant(now);
        response.setStatus(buildStatus());
        response.setDestination(serviceProvider.getAssertionConsumerService().toString());

        final Assertion assertion = samlFactory.object(Assertion.class, Assertion.DEFAULT_ELEMENT_NAME);
        assertion.setID(samlFactory.secureIdentifier());
        assertion.setIssuer(buildIssuer());
        assertion.setIssueInstant(now);
        assertion.setConditions(buildConditions(now, serviceProvider));
        assertion.setSubject(buildSubject(user));
        assertion.getAttributeStatements().add(buildAttributes(user));
        response.getAssertions().add(assertion);

        return sign(response);
    }

    private Response sign(Response response) {
        // TODO
        return response;
    }

    private Conditions buildConditions(DateTime now, SamlServiceProvider serviceProvider) {
        final Audience spAudience = samlFactory.object(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        spAudience.setAudienceURI(serviceProvider.getEntityId());

        final AudienceRestriction restriction = samlFactory.object(AudienceRestriction.class, AudienceRestriction.DEFAULT_ELEMENT_NAME);
        restriction.getAudiences().add(spAudience);

        final Conditions conditions = samlFactory.object(Conditions.class, Conditions.DEFAULT_ELEMENT_NAME);
        conditions.setNotBefore(now);
        conditions.setNotOnOrAfter(now.plus(serviceProvider.getAuthnExpiry()));
        conditions.getAudienceRestrictions().add(restriction);
        return conditions;
    }

    private DateTime now() {
        return new DateTime(clock.millis(), DateTimeZone.UTC);
    }

    private Subject buildSubject(UserServiceAuthentication user) {
        final NameID nameID = samlFactory.object(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameID.setFormat(NameIDType.PERSISTENT);
        nameID.setValue(user.getPrincipal());

        final Subject subject = samlFactory.object(Subject.class, Subject.DEFAULT_ELEMENT_NAME);
        subject.setNameID(nameID);

        return subject;
    }

    private AttributeStatement buildAttributes(UserServiceAuthentication user) {
        final SamlServiceProvider serviceProvider = user.getServiceProvider();
        final AttributeStatement statement = samlFactory.object(AttributeStatement.class, AttributeStatement.DEFAULT_ELEMENT_NAME);
        statement.getAttributes().add(buildAttribute(serviceProvider.getAttributeNames().groups, "groups", user.getGroups()));
        return statement;
    }

    private Attribute buildAttribute(String formalName, String friendlyName, Collection<String> values) {
        final Attribute statement = samlFactory.object(Attribute.class, Attribute.DEFAULT_ELEMENT_NAME);
        statement.setName(formalName);
        statement.setFriendlyName(friendlyName);
        for (String val : values) {
            final XSString string = samlFactory.object(XSString.class, AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
            string.setValue(val);
            statement.getAttributeValues().add(string);
        }
        return statement;
    }

    private Issuer buildIssuer() {
        final Issuer issuer = samlFactory.object(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(this.idp.getEntityId());
        return issuer;
    }

    private Status buildStatus() {
        final StatusCode code = samlFactory.object(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        code.setValue(StatusCode.SUCCESS);

        final Status status = samlFactory.object(Status.class, Status.DEFAULT_ELEMENT_NAME);
        status.setStatusCode(code);

        return status;
    }
}

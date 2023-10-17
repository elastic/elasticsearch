/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.idp.authc.AuthenticationMethod;
import org.elasticsearch.xpack.idp.authc.NetworkControl;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlAuthenticationState;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.support.SamlObjectSigner;
import org.opensaml.core.xml.schema.XSString;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.AttributeValue;
import org.opensaml.saml.saml2.core.Audience;
import org.opensaml.saml.saml2.core.AudienceRestriction;
import org.opensaml.saml.saml2.core.AuthnContext;
import org.opensaml.saml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml.saml2.core.AuthnStatement;
import org.opensaml.saml.saml2.core.Conditions;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.Subject;
import org.opensaml.saml.saml2.core.SubjectConfirmation;
import org.opensaml.saml.saml2.core.SubjectConfirmationData;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

/**
 * Builds SAML 2.0 {@link Response} objects for successful authentication results.
 */
public class SuccessfulAuthenticationResponseMessageBuilder {

    private final Logger logger = LogManager.getLogger(SuccessfulAuthenticationResponseMessageBuilder.class);

    private final Clock clock;
    private final SamlIdentityProvider idp;
    private final SamlFactory samlFactory;

    public SuccessfulAuthenticationResponseMessageBuilder(SamlFactory samlFactory, Clock clock, SamlIdentityProvider idp) {
        SamlInit.initialize();
        this.samlFactory = samlFactory;
        this.clock = clock;
        this.idp = idp;
    }

    public Response build(UserServiceAuthentication user, @Nullable SamlAuthenticationState authnState) {
        logger.debug("Building success response for [{}] from [{}]", user, authnState);
        final Instant now = clock.instant();
        final SamlServiceProvider serviceProvider = user.getServiceProvider();

        final Response response = samlFactory.object(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setID(samlFactory.secureIdentifier());
        if (authnState != null && authnState.getAuthnRequestId() != null) {
            response.setInResponseTo(authnState.getAuthnRequestId());
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
        assertion.setSubject(buildSubject(now, user, authnState));
        assertion.getAuthnStatements().add(buildAuthnStatement(now, user));
        final AttributeStatement attributes = buildAttributes(user);
        if (attributes != null) {
            assertion.getAttributeStatements().add(attributes);
        }
        response.getAssertions().add(assertion);
        return sign(response);
    }

    private Response sign(Response response) {
        final SamlObjectSigner signer = new SamlObjectSigner(samlFactory, idp);
        return SamlFactory.buildXmlObject(signer.sign(response), Response.class);
    }

    private Conditions buildConditions(Instant now, SamlServiceProvider serviceProvider) {
        final Audience spAudience = samlFactory.object(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        spAudience.setURI(serviceProvider.getEntityId());

        final AudienceRestriction restriction = samlFactory.object(AudienceRestriction.class, AudienceRestriction.DEFAULT_ELEMENT_NAME);
        restriction.getAudiences().add(spAudience);

        final Conditions conditions = samlFactory.object(Conditions.class, Conditions.DEFAULT_ELEMENT_NAME);
        conditions.setNotBefore(now);
        conditions.setNotOnOrAfter(now.plus(serviceProvider.getAuthnExpiry()));
        conditions.getAudienceRestrictions().add(restriction);
        return conditions;
    }

    private Subject buildSubject(Instant now, UserServiceAuthentication user, SamlAuthenticationState authnState) {
        final SamlServiceProvider serviceProvider = user.getServiceProvider();

        final NameID nameID = buildNameId(user, authnState);

        final Subject subject = samlFactory.object(Subject.class, Subject.DEFAULT_ELEMENT_NAME);
        subject.setNameID(nameID);

        final SubjectConfirmationData data = samlFactory.object(
            SubjectConfirmationData.class,
            SubjectConfirmationData.DEFAULT_ELEMENT_NAME
        );
        if (authnState != null && authnState.getAuthnRequestId() != null) {
            data.setInResponseTo(authnState.getAuthnRequestId());
        }
        data.setNotBefore(now);
        data.setNotOnOrAfter(now.plus(serviceProvider.getAuthnExpiry()));
        data.setRecipient(serviceProvider.getAssertionConsumerService().toString());

        final SubjectConfirmation confirmation = samlFactory.object(SubjectConfirmation.class, SubjectConfirmation.DEFAULT_ELEMENT_NAME);
        confirmation.setMethod(SubjectConfirmation.METHOD_BEARER);
        confirmation.setSubjectConfirmationData(data);

        subject.getSubjectConfirmations().add(confirmation);
        return subject;
    }

    private AuthnStatement buildAuthnStatement(Instant now, UserServiceAuthentication user) {
        final SamlServiceProvider serviceProvider = user.getServiceProvider();
        final AuthnStatement statement = samlFactory.object(AuthnStatement.class, AuthnStatement.DEFAULT_ELEMENT_NAME);
        statement.setAuthnInstant(now);
        statement.setSessionNotOnOrAfter(now.plus(serviceProvider.getAuthnExpiry()));

        final AuthnContext context = samlFactory.object(AuthnContext.class, AuthnContext.DEFAULT_ELEMENT_NAME);
        final AuthnContextClassRef classRef = samlFactory.object(AuthnContextClassRef.class, AuthnContextClassRef.DEFAULT_ELEMENT_NAME);
        classRef.setURI(resolveAuthnClass(user.getAuthenticationMethods(), user.getNetworkControls()));
        context.setAuthnContextClassRef(classRef);
        statement.setAuthnContext(context);

        return statement;
    }

    private static String resolveAuthnClass(Set<AuthenticationMethod> authenticationMethods, Set<NetworkControl> networkControls) {
        if (authenticationMethods.contains(AuthenticationMethod.PASSWORD)) {
            if (networkControls.contains(NetworkControl.IP_FILTER)) {
                return AuthnContext.IP_PASSWORD_AUTHN_CTX;
            } else if (networkControls.contains(NetworkControl.TLS)) {
                return AuthnContext.PPT_AUTHN_CTX;
            } else {
                return AuthnContext.PASSWORD_AUTHN_CTX;
            }
        } else if (authenticationMethods.contains(AuthenticationMethod.KERBEROS)) {
            return AuthnContext.KERBEROS_AUTHN_CTX;
        } else if (authenticationMethods.contains(AuthenticationMethod.TLS_CLIENT_AUTH) && networkControls.contains(NetworkControl.TLS)) {
            return AuthnContext.TLS_CLIENT_AUTHN_CTX;
        } else if (authenticationMethods.contains(AuthenticationMethod.PRIOR_SESSION)) {
            return AuthnContext.PREVIOUS_SESSION_AUTHN_CTX;
        } else if (networkControls.contains(NetworkControl.IP_FILTER)) {
            return AuthnContext.IP_AUTHN_CTX;
        } else {
            return AuthnContext.UNSPECIFIED_AUTHN_CTX;
        }
    }

    private AttributeStatement buildAttributes(UserServiceAuthentication user) {
        final SamlServiceProvider serviceProvider = user.getServiceProvider();
        final AttributeStatement statement = samlFactory.object(AttributeStatement.class, AttributeStatement.DEFAULT_ELEMENT_NAME);
        final List<Attribute> attributes = new ArrayList<>();
        final Attribute roles = buildAttribute(serviceProvider.getAttributeNames().roles, "roles", user.getRoles());
        if (roles != null) {
            attributes.add(roles);
        }
        final Attribute principal = buildAttribute(serviceProvider.getAttributeNames().principal, "principal", user.getPrincipal());
        if (principal != null) {
            attributes.add(principal);
        }
        final Attribute email = buildAttribute(serviceProvider.getAttributeNames().email, "email", user.getEmail());
        if (email != null) {
            attributes.add(email);
        }
        final Attribute name = buildAttribute(serviceProvider.getAttributeNames().name, "name", user.getName());
        if (name != null) {
            attributes.add(name);
        }
        if (attributes.isEmpty()) {
            return null;
        }
        statement.getAttributes().addAll(attributes);
        return statement;
    }

    private Attribute buildAttribute(String formalName, String friendlyName, String value) {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }
        return buildAttribute(formalName, friendlyName, List.of(value));
    }

    private Attribute buildAttribute(String formalName, String friendlyName, Collection<String> values) {
        if (values.isEmpty() || Strings.isNullOrEmpty(formalName)) {
            return null;
        }
        final Attribute attribute = samlFactory.object(Attribute.class, Attribute.DEFAULT_ELEMENT_NAME);
        attribute.setName(formalName);
        attribute.setFriendlyName(friendlyName);
        attribute.setNameFormat(Attribute.URI_REFERENCE);
        for (String val : values) {
            final XSString string = samlFactory.object(XSString.class, AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
            string.setValue(val);
            attribute.getAttributeValues().add(string);
        }
        return attribute;
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

    private NameID buildNameId(UserServiceAuthentication user, @Nullable SamlAuthenticationState authnState) {
        final SamlServiceProvider serviceProvider = user.getServiceProvider();
        final NameID nameID = samlFactory.object(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        final String nameIdFormat;
        if (authnState != null && authnState.getRequestedNameidFormat() != null) {
            nameIdFormat = authnState.getRequestedNameidFormat();
        } else {
            nameIdFormat = serviceProvider.getAllowedNameIdFormat() != null
                ? serviceProvider.getAllowedNameIdFormat()
                : idp.getServiceProviderDefaults().nameIdFormat;
        }
        nameID.setFormat(nameIdFormat);
        nameID.setValue(getNameIdValueForFormat(nameIdFormat, user));
        return nameID;
    }

    private String getNameIdValueForFormat(String format, UserServiceAuthentication user) {
        return switch (format) {
            case TRANSIENT ->
                // See SAML 2.0 Core 8.3.8 & 1.3.4
                samlFactory.secureIdentifier();
            default -> throw new IllegalStateException("Unsupported NameID Format: " + format);
        };
    }
}

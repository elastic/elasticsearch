/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.opensaml.saml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml.saml2.core.AuthnContextComparisonTypeEnumeration;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.NameIDPolicy;
import org.opensaml.saml.saml2.core.RequestedAuthnContext;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;

import java.time.Clock;

/**
 * Generates a SAML {@link AuthnRequest} from a simplified set of parameters.
 */
class SamlAuthnRequestBuilder extends SamlMessageBuilder {

    private final String spBinding;
    private final String idpBinding;
    private Boolean forceAuthn;
    private NameIDPolicySettings nameIdSettings;

    SamlAuthnRequestBuilder(SpConfiguration spConfig, String spBinding, EntityDescriptor idpDescriptor, String idBinding, Clock clock) {
        super(idpDescriptor, spConfig, clock);
        this.spBinding = spBinding;
        this.idpBinding = idBinding;
        this.nameIdSettings = new NameIDPolicySettings(null, false, null);
    }

    SamlAuthnRequestBuilder forceAuthn(Boolean forceAuthn) {
        this.forceAuthn = forceAuthn;
        return this;
    }

    SamlAuthnRequestBuilder nameIDPolicy(NameIDPolicySettings settings) {
        this.nameIdSettings = settings;
        return this;
    }

    AuthnRequest build() {
        final String destination = getIdpLocation();

        final AuthnRequest request = SamlUtils.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        request.setID(buildId());
        request.setIssueInstant(clock.instant());
        request.setDestination(destination);
        request.setProtocolBinding(spBinding);
        request.setAssertionConsumerServiceURL(serviceProvider.getAscUrl());
        request.setIssuer(buildIssuer());
        if (nameIdSettings != null) {
            request.setNameIDPolicy(buildNameIDPolicy());
        }
        if (super.serviceProvider.getReqAuthnCtxClassRef().isEmpty() == false) {
            request.setRequestedAuthnContext(buildRequestedAuthnContext());
        }
        request.setForceAuthn(forceAuthn);
        return request;
    }

    private RequestedAuthnContext buildRequestedAuthnContext() {
        RequestedAuthnContext requestedAuthnContext = SamlUtils.buildObject(
            RequestedAuthnContext.class,
            RequestedAuthnContext.DEFAULT_ELEMENT_NAME
        );
        for (String authnCtxClass : super.serviceProvider.getReqAuthnCtxClassRef()) {
            AuthnContextClassRef authnContextClassRef = SamlUtils.buildObject(
                AuthnContextClassRef.class,
                AuthnContextClassRef.DEFAULT_ELEMENT_NAME
            );
            authnContextClassRef.setURI(authnCtxClass);
            requestedAuthnContext.getAuthnContextClassRefs().add(authnContextClassRef);
        }
        // We handle only EXACT comparison
        requestedAuthnContext.setComparison(AuthnContextComparisonTypeEnumeration.EXACT);
        return requestedAuthnContext;
    }

    private NameIDPolicy buildNameIDPolicy() {
        NameIDPolicy nameIDPolicy = SamlUtils.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(Strings.isNullOrEmpty(nameIdSettings.format) ? null : nameIdSettings.format);
        nameIDPolicy.setAllowCreate(nameIdSettings.allowCreate);
        nameIDPolicy.setSPNameQualifier(Strings.isNullOrEmpty(nameIdSettings.spNameQualifier) ? null : nameIdSettings.spNameQualifier);
        return nameIDPolicy;
    }

    private String getIdpLocation() {
        final String location = getIdentityProviderEndpoint(idpBinding, IDPSSODescriptor::getSingleSignOnServices);
        if (location == null) {
            throw new ElasticsearchException(
                "Cannot find [{}]/[{}] in descriptor [{}]",
                IDPSSODescriptor.DEFAULT_ELEMENT_NAME,
                idpBinding,
                identityProvider.getID()
            );
        }
        return location;
    }

    static class NameIDPolicySettings {
        private final String format;
        private final boolean allowCreate;
        private final String spNameQualifier;

        NameIDPolicySettings(String format, boolean allowCreate, String spNameQualifier) {
            this.format = format;
            this.allowCreate = allowCreate;
            this.spNameQualifier = spNameQualifier;
        }
    }
}

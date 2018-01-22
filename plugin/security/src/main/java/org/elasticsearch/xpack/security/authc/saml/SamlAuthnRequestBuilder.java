/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.time.Clock;

import org.elasticsearch.ElasticsearchException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameIDPolicy;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;

/**
 * Generates a SAML {@link AuthnRequest} from a simplified set of parameters.
 */
class SamlAuthnRequestBuilder extends SamlMessageBuilder {

    private final String spBinding;
    private final String idpBinding;
    private String nameIdFormat;
    private Boolean forceAuthn;

    SamlAuthnRequestBuilder(SpConfiguration spConfig, String spBinding, EntityDescriptor idpDescriptor, String idBinding, Clock clock) {
        super(idpDescriptor, spConfig, clock);
        this.spBinding = spBinding;
        this.idpBinding = idBinding;
    }

    SamlAuthnRequestBuilder nameIdFormat(String nameIdFormat) {
        this.nameIdFormat = nameIdFormat;
        return this;
    }

    SamlAuthnRequestBuilder forceAuthn(Boolean forceAuthn) {
        this.forceAuthn = forceAuthn;
        return this;
    }

    AuthnRequest build() {
        final String destination = getIdpLocation();

        final AuthnRequest request = SamlUtils.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        request.setID(buildId());
        request.setIssueInstant(now());
        request.setDestination(destination);
        request.setProtocolBinding(spBinding);
        request.setAssertionConsumerServiceURL(serviceProvider.getAscUrl());
        request.setIssuer(buildIssuer());
        request.setNameIDPolicy(buildNameIDPolicy());
        request.setForceAuthn(forceAuthn);
        return request;
    }

    private NameIDPolicy buildNameIDPolicy() {
        NameIDPolicy nameIDPolicy = SamlUtils.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(nameIdFormat);
        nameIDPolicy.setSPNameQualifier(serviceProvider.getEntityId());
        nameIDPolicy.setAllowCreate(false);
        return nameIDPolicy;
    }

    private String getIdpLocation() {
        final String location = getIdentityProviderEndpoint(idpBinding, IDPSSODescriptor::getSingleSignOnServices);
        if (location == null) {
            throw new ElasticsearchException("Cannot find [{}]/[{}] in descriptor [{}]",
                    IDPSSODescriptor.DEFAULT_ELEMENT_NAME, idpBinding, identityProvider.getID());
        }
        return location;
    }

}

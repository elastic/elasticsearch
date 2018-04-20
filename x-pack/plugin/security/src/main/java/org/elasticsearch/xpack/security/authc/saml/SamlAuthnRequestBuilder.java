/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.NameIDPolicy;
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
        this.nameIdSettings = new NameIDPolicySettings(NameID.TRANSIENT, false, null);
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
        request.setIssueInstant(now());
        request.setDestination(destination);
        request.setProtocolBinding(spBinding);
        request.setAssertionConsumerServiceURL(serviceProvider.getAscUrl());
        request.setIssuer(buildIssuer());
        if (nameIdSettings != null) {
            request.setNameIDPolicy(buildNameIDPolicy());
        }
        request.setForceAuthn(forceAuthn);
        return request;
    }

    private NameIDPolicy buildNameIDPolicy() {
        NameIDPolicy nameIDPolicy = SamlUtils.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(nameIdSettings.format);
        nameIDPolicy.setAllowCreate(nameIdSettings.allowCreate);
        nameIDPolicy.setSPNameQualifier(Strings.isNullOrEmpty(nameIdSettings.spNameQualifier) ? null : nameIdSettings.spNameQualifier);
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;


import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.opensaml.security.x509.X509Credential;


/**
 * SAML 2.0 configuration information about this IdP
 */
public interface SamlIdentityProvider {

    String getEntityId();

    String getSingleSignOnEndpoint(String binding);

    String getSingleLogoutEndpoint(String binding);

    X509Credential getSigningCredential();

    X509Credential getMetadataSigningCredential();

    SamlServiceProvider getRegisteredServiceProvider(String spEntityId);

    SamlIdPMetadataBuilder.OrganizationInfo getOrganization();

    SamlIdPMetadataBuilder.ContactInfo getTechnicalContact();
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;


import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import java.net.URL;

/**
 * SAML 2.0 configuration information about this IdP
 */
public interface SamlIdentityProvider {

    String getEntityId();

    URL getSingleSignOnEndpoint(String binding);

    URL getSingleLogoutEndpoint(String binding);

    SamlServiceProvider getRegisteredServiceProvider(String spEntityId);

    SamlIdPMetadataBuilder.OrganizationInfo getOrganization();

    SamlIdPMetadataBuilder.ContactInfo getTechnicalContact();
}

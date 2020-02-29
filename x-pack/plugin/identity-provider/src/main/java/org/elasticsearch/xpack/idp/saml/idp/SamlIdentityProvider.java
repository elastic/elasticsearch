/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;


import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.joda.time.ReadableDuration;
import org.opensaml.security.x509.X509Credential;

/**
 * SAML 2.0 configuration information about this IdP
 */
public interface SamlIdentityProvider {

    String getEntityId();

    String getSingleSignOnEndpoint(String binding);

    String getSingleLogoutEndpoint(String binding);

    X509Credential getSigningCredential();

    SamlServiceProvider getRegisteredServiceProvider(String spEntityId);

    ServiceProviderDefaults getServiceProviderDefaults();

    final class ServiceProviderDefaults {
        public final String applicationName;
        public final String loginAction;
        public final String nameIdFormat;
        public final ReadableDuration authenticationExpiry;

        public ServiceProviderDefaults(String applicationName,
                                       String loginAction,
                                       String nameIdFormat,
                                       ReadableDuration authenticationExpiry) {
            this.applicationName = applicationName;
            this.loginAction = loginAction;
            this.nameIdFormat = nameIdFormat;
            this.authenticationExpiry = authenticationExpiry;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.opensaml.security.x509.X509Credential;

import java.net.URL;
import java.time.Duration;
import java.util.Set;

/**
 * SAML 2.0 configuration information about a specific service provider
 */
public interface SamlServiceProvider {

    String getName();

    boolean isEnabled();

    String getEntityId();

    String getAllowedNameIdFormat();

    URL getAssertionConsumerService();

    Duration getAuthnExpiry();

    class AttributeNames {
        public final String principal;
        public final String name;
        public final String email;
        public final String roles;

        public AttributeNames(String principal, String name, String email, String roles) {
            this.principal = principal;
            this.name = name;
            this.email = email;
            this.roles = roles;
        }
    }

    AttributeNames getAttributeNames();

    ServiceProviderPrivileges getPrivileges();

    Set<X509Credential> getSpSigningCredentials();

    boolean shouldSignAuthnRequests();

    boolean shouldSignLogoutRequests();
}

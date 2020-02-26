/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.joda.time.ReadableDuration;
import org.opensaml.security.x509.X509Credential;

import java.net.URL;
import java.util.Set;

/**
 * SAML 2.0 configuration information about a specific service provider
 */
public interface SamlServiceProvider {
    String getEntityId();

    Set<String> getAllowedNameIdFormats();

    URL getAssertionConsumerService();

    ReadableDuration getAuthnExpiry();

    class AttributeNames {
        public final String groups = "https://saml.elasticsearch.org/attributes/groups";
    }

    AttributeNames getAttributeNames();

    @Nullable
    X509Credential getSpSigningCredential();

    boolean shouldSignAuthnRequests();

    boolean shouldSignLogoutRequests();

    ServiceProviderPrivileges getPrivileges();
}

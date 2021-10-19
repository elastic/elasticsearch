/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.opensaml.security.x509.X509Credential;

import java.net.URL;
import java.time.Duration;
import java.util.Set;


public class CloudServiceProvider implements SamlServiceProvider {

    private final String entityId;
    private final String name;
    private final boolean enabled;
    private final URL assertionConsumerService;
    private final String allowedNameIdFormat;
    private final Duration authnExpiry;
    private final ServiceProviderPrivileges privileges;
    private final AttributeNames attributeNames;
    private final Set<X509Credential> spSigningCredentials;
    private final boolean signAuthnRequests;
    private final boolean signLogoutRequests;

    public CloudServiceProvider(String entityId, String name, boolean enabled, URL assertionConsumerService, String allowedNameIdFormat,
                                Duration authnExpiry, ServiceProviderPrivileges privileges, AttributeNames attributeNames,
                                Set<X509Credential> spSigningCredentials, boolean signAuthnRequests, boolean signLogoutRequests) {
        if (Strings.isNullOrEmpty(entityId)) {
            throw new IllegalArgumentException("Service Provider Entity ID cannot be null or empty");
        }
        this.entityId = entityId;
        this.name = name;
        this.enabled = enabled;
        this.assertionConsumerService = assertionConsumerService;
        this.allowedNameIdFormat = allowedNameIdFormat;
        this.authnExpiry = authnExpiry;
        this.privileges = privileges;
        this.attributeNames = attributeNames;
        this.spSigningCredentials = spSigningCredentials == null ? Set.of() : Set.copyOf(spSigningCredentials);
        this.signLogoutRequests = signLogoutRequests;
        this.signAuthnRequests = signAuthnRequests;
    }

    @Override
    public String getEntityId() {
        return entityId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getAllowedNameIdFormat() {
        return allowedNameIdFormat;
    }

    @Override
    public URL getAssertionConsumerService() {
        return assertionConsumerService;
    }

    @Override
    public Duration getAuthnExpiry() {
        return authnExpiry;
    }

    @Override
    public AttributeNames getAttributeNames() {
        return attributeNames;
    }

    @Override
    public Set<X509Credential> getSpSigningCredentials() {
        return spSigningCredentials;
    }

    @Override
    public boolean shouldSignAuthnRequests() {
        return signAuthnRequests;
    }

    @Override
    public boolean shouldSignLogoutRequests() {
        return signLogoutRequests;
    }

    @Override
    public ServiceProviderPrivileges getPrivileges() {
        return privileges;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "entityId=[" + entityId + ']'
            + " name=[" + name + ']'
            + " enabled=" + enabled
            + " acs=[" + assertionConsumerService + "]"
            + "}";
    }
}

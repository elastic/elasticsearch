/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;
import org.opensaml.security.x509.X509Credential;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;


public class CloudServiceProvider implements SamlServiceProvider {

    private final String entityid;
    private final URL assertionConsumerService;
    private final ReadableDuration authnExpiry;
    private final ServiceProviderPrivileges privileges;
    private final Set<String> allowedNameIdFormats;
    private final X509Credential spSigningCredential;
    private final boolean signAuthnRequests;
    private final boolean signLogoutRequests;

    public CloudServiceProvider(String entityId, String assertionConsumerService, Set<String> allowedNameIdFormats,
                                ServiceProviderPrivileges privileges, boolean signAuthnRequests, boolean signLogoutRequests,
                                @Nullable X509Credential spSigningCredential) {
        if (Strings.isNullOrEmpty(entityId)) {
            throw new IllegalArgumentException("Service Provider Entity ID cannot be null or empty");
        }
        this.entityid = entityId;
        try {
            this.assertionConsumerService = new URL(assertionConsumerService);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid URL for Assertion Consumer Service", e);
        }
        this.allowedNameIdFormats = Set.copyOf(allowedNameIdFormats);
        this.authnExpiry = Duration.standardMinutes(5);
        this.privileges = new ServiceProviderPrivileges("cloud-idp", "service$" + entityId, "action:sso", Map.of());
        this.spSigningCredential = spSigningCredential;
        this.signLogoutRequests = signLogoutRequests;
        this.signAuthnRequests = signAuthnRequests;

    }

    @Override
    public String getEntityId() {
        return entityid;
    }

    @Override
    public Set<String> getAllowedNameIdFormats() {
        return allowedNameIdFormats;
    }

    @Override
    public URL getAssertionConsumerService() {
        return assertionConsumerService;
    }

    @Override
    public ReadableDuration getAuthnExpiry() {
        return authnExpiry;
    }

    @Override
    public AttributeNames getAttributeNames() {
        return new SamlServiceProvider.AttributeNames();
    }

    @Override
    public X509Credential getSpSigningCredential() {
        return spSigningCredential;
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
}

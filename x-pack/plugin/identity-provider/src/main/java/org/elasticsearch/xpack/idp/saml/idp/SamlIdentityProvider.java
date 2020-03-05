/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xpack.idp.saml.sp.CloudServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
import org.opensaml.security.x509.X509Credential;

import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

/**
 * SAML 2.0 configuration information about this IdP
 */
public class SamlIdentityProvider {

    private final String entityId;
    private final Map<String, URL> ssoEndpoints;
    private final Map<String, URL> sloEndpoints;
    private final ServiceProviderDefaults serviceProviderDefaults;
    private final X509Credential signingCredential;
    private final X509Credential metadataSigningCredential;
    private ContactInfo technicalContact;
    private OrganizationInfo organization;

    private Map<String, SamlServiceProvider> registeredServiceProviders;

    // Package access - use Builder instead
    SamlIdentityProvider(String entityId, Map<String, URL> ssoEndpoints, Map<String, URL> sloEndpoints,
                                X509Credential signingCredential, X509Credential metadataSigningCredential,
                                ContactInfo technicalContact, OrganizationInfo organization) {
        this.entityId = entityId;
        this.ssoEndpoints = ssoEndpoints;
        this.sloEndpoints = sloEndpoints;
        this.signingCredential = signingCredential;
        // TODO
        this.serviceProviderDefaults = new ServiceProviderDefaults("elastic-cloud", "action:login", TRANSIENT, Duration.standardMinutes(5));
        this.metadataSigningCredential = metadataSigningCredential;
        this.technicalContact = technicalContact;
        this.organization = organization;
        // TODO - this should use the index
        this.registeredServiceProviders = gatherRegisteredServiceProviders();
    }

    public static SamlIdentityProviderBuilder builder() {
        return new SamlIdentityProviderBuilder();
    }

    public String getEntityId() {
        return entityId;
    }

    public URL getSingleSignOnEndpoint(String binding) {
        return ssoEndpoints.get(binding);
    }

    public URL getSingleLogoutEndpoint(String binding) {
        return sloEndpoints.get(binding);
    }

    public X509Credential getSigningCredential() {
        return signingCredential;
    }

    public X509Credential getMetadataSigningCredential() {
        return metadataSigningCredential;
    }

    public OrganizationInfo getOrganization() {
        return organization;
    }

    public ContactInfo getTechnicalContact() {
        return technicalContact;
    }

    public ServiceProviderDefaults getServiceProviderDefaults() {
        return serviceProviderDefaults;
    }

    /**
     * Asynchronously lookup the specified {@link SamlServiceProvider} by entity-id.
     * @param listener Responds with the requested Service Provider object, or {@code null} if no such SP exists.
     *                 {@link ActionListener#onFailure} is only used for fatal errors (e.g. being unable to access
     *                 the backing store (elasticsearch index) that hold the SP data).
     */
    public void getRegisteredServiceProvider(String spEntityId, ActionListener<SamlServiceProvider> listener) {
        // TODO use resolver
        listener.onResponse(registeredServiceProviders.get(spEntityId));
    }

    private Map<String, SamlServiceProvider> gatherRegisteredServiceProviders() {
        // TODO Fetch all the registered service providers from the index (?) they are persisted.
        // For now hardcode something to use.
        Map<String, SamlServiceProvider> registeredSps = new HashMap<>();
        try {
            registeredSps.put("https://sp.some.org",
                new CloudServiceProvider("https://sp.some.org", new URL("https://sp.some.org/api/security/v1/saml"), Set.of(TRANSIENT),
                    Duration.standardMinutes(5), null,
                    new SamlServiceProvider.AttributeNames(
                        "https://saml.elasticsearch.org/attributes/principal",
                        "https://saml.elasticsearch.org/attributes/name",
                        "https://saml.elasticsearch.org/attributes/email",
                        "https://saml.elasticsearch.org/attributes/groups"),
                    null, false, false));
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
        return registeredSps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SamlIdentityProvider that = (SamlIdentityProvider) o;
        return Objects.equals(entityId, that.entityId) &&
            Objects.equals(ssoEndpoints, that.ssoEndpoints) &&
            Objects.equals(sloEndpoints, that.sloEndpoints) &&
            Objects.equals(signingCredential, that.signingCredential) &&
            Objects.equals(metadataSigningCredential, that.metadataSigningCredential) &&
            Objects.equals(technicalContact, that.technicalContact) &&
            Objects.equals(organization, that.organization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId);
    }

    public static class ContactInfo {
        static final Map<String, ContactPersonTypeEnumeration> TYPES = Collections.unmodifiableMap(
            MapBuilder.newMapBuilder(new LinkedHashMap<String, ContactPersonTypeEnumeration>())
                .put(ContactPersonTypeEnumeration.ADMINISTRATIVE.toString(), ContactPersonTypeEnumeration.ADMINISTRATIVE)
                .put(ContactPersonTypeEnumeration.BILLING.toString(), ContactPersonTypeEnumeration.BILLING)
                .put(ContactPersonTypeEnumeration.SUPPORT.toString(), ContactPersonTypeEnumeration.SUPPORT)
                .put(ContactPersonTypeEnumeration.TECHNICAL.toString(), ContactPersonTypeEnumeration.TECHNICAL)
                .put(ContactPersonTypeEnumeration.OTHER.toString(), ContactPersonTypeEnumeration.OTHER)
                .map());

        public final ContactPersonTypeEnumeration type;
        public final String givenName;
        public final String surName;
        public final String email;

        public ContactInfo(ContactPersonTypeEnumeration type, String givenName, String surName, String email) {
            this.type = Objects.requireNonNull(type, "Contact Person Type is required");
            this.givenName = givenName;
            this.surName = surName;
            this.email = Objects.requireNonNull(email, "Contact Person email is required");
        }

        public static ContactPersonTypeEnumeration getType(String name) {
            final ContactPersonTypeEnumeration type = TYPES.get(name.toLowerCase(Locale.ROOT));
            if (type == null) {
                throw new IllegalArgumentException("Invalid contact type " + name + " allowed values are "
                    + Strings.collectionToCommaDelimitedString(TYPES.keySet()));
            }
            return type;
        }
    }

    public static class OrganizationInfo {
        public final String organizationName;
        public final String displayName;
        public final String url;

        public OrganizationInfo(String organizationName, String displayName, String url) {
            this.organizationName = organizationName;
            this.displayName = displayName;
            this.url = url;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrganizationInfo that = (OrganizationInfo) o;
            return Objects.equals(organizationName, that.organizationName) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(organizationName, displayName, url);
        }
    }

    public static final class ServiceProviderDefaults {
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

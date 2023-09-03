/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.sp.WildcardServiceProviderResolver;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
import org.opensaml.security.x509.X509Credential;

import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * SAML 2.0 configuration information about this IdP
 */
public class SamlIdentityProvider {

    private final Logger logger = LogManager.getLogger(SamlIdentityProvider.class);

    private final String entityId;
    private final Map<String, URL> ssoEndpoints;
    private final Map<String, URL> sloEndpoints;
    private final Set<String> allowedNameIdFormats;
    private final ServiceProviderDefaults serviceProviderDefaults;
    private final X509Credential signingCredential;
    private final SamlServiceProviderResolver serviceProviderResolver;
    private final WildcardServiceProviderResolver wildcardServiceResolver;
    private final X509Credential metadataSigningCredential;
    private ContactInfo technicalContact;
    private OrganizationInfo organization;

    // Package access - use Builder instead
    SamlIdentityProvider(
        String entityId,
        Map<String, URL> ssoEndpoints,
        Map<String, URL> sloEndpoints,
        Set<String> allowedNameIdFormats,
        X509Credential signingCredential,
        X509Credential metadataSigningCredential,
        ContactInfo technicalContact,
        OrganizationInfo organization,
        ServiceProviderDefaults serviceProviderDefaults,
        SamlServiceProviderResolver serviceProviderResolver,
        WildcardServiceProviderResolver wildcardServiceResolver
    ) {
        this.entityId = entityId;
        this.ssoEndpoints = ssoEndpoints;
        this.sloEndpoints = sloEndpoints;
        this.allowedNameIdFormats = allowedNameIdFormats;
        this.signingCredential = signingCredential;
        this.serviceProviderDefaults = serviceProviderDefaults;
        this.metadataSigningCredential = metadataSigningCredential;
        this.technicalContact = technicalContact;
        this.organization = organization;
        this.serviceProviderResolver = serviceProviderResolver;
        this.wildcardServiceResolver = wildcardServiceResolver;
    }

    public static SamlIdentityProviderBuilder builder(
        SamlServiceProviderResolver serviceResolver,
        WildcardServiceProviderResolver wildcardResolver
    ) {
        return new SamlIdentityProviderBuilder(serviceResolver, wildcardResolver);
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

    public Set<String> getAllowedNameIdFormats() {
        return allowedNameIdFormats;
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
     * @param spEntityId The (URI) entity ID of the service provider
     * @param acs The ACS of the service provider - only used if there is no registered service provider and we need to dynamically define
     *            one from a template (wildcard). May be null, in which case wildcard services will not be resolved.
     * @param allowDisabled whether to return service providers that are not {@link SamlServiceProvider#isEnabled() enabled}.
     *                      For security reasons, callers should typically avoid working with disabled service providers.
     * @param listener Responds with the requested Service Provider object, or {@code null} if no such SP exists.
    *                 {@link ActionListener#onFailure} is only used for fatal errors (e.g. being unable to access
     */
    public void resolveServiceProvider(
        String spEntityId,
        @Nullable String acs,
        boolean allowDisabled,
        ActionListener<SamlServiceProvider> listener
    ) {
        serviceProviderResolver.resolve(spEntityId, ActionListener.wrap(sp -> {
            if (sp == null) {
                logger.debug("No explicitly registered service provider exists for entityId [{}]", spEntityId);
                resolveWildcardService(spEntityId, acs, listener);
            } else if (allowDisabled == false && sp.isEnabled() == false) {
                logger.info("Service provider [{}][{}] is not enabled", spEntityId, sp.getName());
                listener.onResponse(null);
            } else {
                logger.debug("Service provider for [{}] is [{}]", spEntityId, sp);
                listener.onResponse(sp);
            }
        }, listener::onFailure));
    }

    private void resolveWildcardService(String spEntityId, String acs, ActionListener<SamlServiceProvider> listener) {
        if (acs == null) {
            logger.debug("No ACS provided for [{}], skipping wildcard matching", spEntityId);
            listener.onResponse(null);
        } else {
            try {
                final SamlServiceProvider sp = wildcardServiceResolver.resolve(spEntityId, acs);
                logger.debug("Wildcard service provider for [{}][{}] is [{}]", spEntityId, acs, sp);
                listener.onResponse(sp);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SamlIdentityProvider that = (SamlIdentityProvider) o;
        return Objects.equals(entityId, that.entityId)
            && Objects.equals(ssoEndpoints, that.ssoEndpoints)
            && Objects.equals(sloEndpoints, that.sloEndpoints)
            && Objects.equals(allowedNameIdFormats, that.allowedNameIdFormats)
            && Objects.equals(signingCredential, that.signingCredential)
            && Objects.equals(metadataSigningCredential, that.metadataSigningCredential)
            && Objects.equals(technicalContact, that.technicalContact)
            && Objects.equals(organization, that.organization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId);
    }

    public static class ContactInfo {
        static final Map<String, ContactPersonTypeEnumeration> TYPES = Stream.of(
            ContactPersonTypeEnumeration.ADMINISTRATIVE,
            ContactPersonTypeEnumeration.BILLING,
            ContactPersonTypeEnumeration.SUPPORT,
            ContactPersonTypeEnumeration.TECHNICAL,
            ContactPersonTypeEnumeration.OTHER
        ).collect(Maps.toUnmodifiableOrderedMap(ContactPersonTypeEnumeration::toString, Function.identity()));

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
                throw new IllegalArgumentException(
                    "Invalid contact type " + name + " allowed values are " + Strings.collectionToCommaDelimitedString(TYPES.keySet())
                );
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
            return Objects.equals(organizationName, that.organizationName)
                && Objects.equals(displayName, that.displayName)
                && Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(organizationName, displayName, url);
        }
    }
}

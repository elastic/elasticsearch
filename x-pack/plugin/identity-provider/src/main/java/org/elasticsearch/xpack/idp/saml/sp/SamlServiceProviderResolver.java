/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentSupplier;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentVersion;
import org.joda.time.ReadableDuration;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.security.x509.X509Credential;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SamlServiceProviderResolver {

    private static final int CACHE_SIZE_DEFAULT = 1000;
    private static final TimeValue CACHE_TTL_DEFAULT = TimeValue.timeValueMinutes(60);

    public static final Setting<Integer> CACHE_SIZE
        = Setting.intSetting("xpack.idp.sp.cache.size", CACHE_SIZE_DEFAULT, Setting.Property.NodeScope);
    public static final Setting<TimeValue> CACHE_TTL
        = Setting.timeSetting("xpack.idp.sp.cache.ttl", CACHE_TTL_DEFAULT, Setting.Property.NodeScope);

    private final Cache<String, CachedServiceProvider> cache;
    private final SamlServiceProviderIndex index;
    private final SamlIdentityProvider identityProvider;

    public SamlServiceProviderResolver(Settings settings, SamlServiceProviderIndex index, SamlIdentityProvider identityProvider) {
        this.cache = CacheBuilder.<String, CachedServiceProvider>builder()
            .setMaximumWeight(CACHE_SIZE.get(settings))
            .setExpireAfterAccess(CACHE_TTL.get(settings))
            .build();
        this.index = index;
        this.identityProvider = identityProvider;
    }

    /**
     * Find a {@link SamlServiceProvider} by entity-id.
     *
     * @param listener Callback for the service provider object. Calls {@link ActionListener#onResponse} with a {@code null} value if the
     *                 service provider does not exist.
     */
    public void resolve(String entityId, ActionListener<SamlServiceProvider> listener) {
        index.findByEntityId(entityId, ActionListener.wrap(
            documentSuppliers -> {
                if (documentSuppliers.isEmpty()) {
                    listener.onResponse(null);
                    return;
                }
                if (documentSuppliers.size() > 1) {
                    listener.onFailure(new IllegalStateException(
                        "Found multiple service providers with entity-id [" + entityId
                            + "] - document ids ["
                            + documentSuppliers.stream().map(s -> s.version.id).collect(Collectors.joining(","))
                            + "] in index [" + index + "]"));
                    return;
                }
                final DocumentSupplier doc = Iterables.get(documentSuppliers, 0);
                final CachedServiceProvider cached = cache.get(entityId);
                if (cached != null && cached.documentVersion.equals(doc.version)) {
                    listener.onResponse(cached.serviceProvider);
                    return;
                } else {
                    populateCacheAndReturn(entityId, doc, listener);
                }
            },
            listener::onFailure
        ));

    }

    private void populateCacheAndReturn(String entityId, DocumentSupplier doc, ActionListener<SamlServiceProvider> listener) {
        final SamlServiceProvider serviceProvider = buildServiceProvider(doc.document.get());
        final CachedServiceProvider cacheEntry = new CachedServiceProvider(entityId, doc.version, serviceProvider);
        cache.put(entityId, cacheEntry);
        listener.onResponse(serviceProvider);
    }

    private SamlServiceProvider buildServiceProvider(SamlServiceProviderDocument document) {
        final ServiceProviderPrivileges privileges = buildPrivileges(document.privileges);
        final SamlServiceProvider.AttributeNames attributes = new SamlServiceProvider.AttributeNames(
            document.attributeNames.principal, document.attributeNames.name, document.attributeNames.email, document.attributeNames.groups
        );
        final Set<X509Credential> credentials = document.certificates.getServiceProviderX509SigningCertificates()
            .stream()
            .map(BasicX509Credential::new)
            .collect(Collectors.toUnmodifiableSet());

        final URL acs = parseUrl(document);
        Set<String> nameIdFormats = document.nameIdFormats;
        if (nameIdFormats == null || nameIdFormats.isEmpty()) {
            nameIdFormats = Set.of(identityProvider.getServiceProviderDefaults().nameIdFormat);
        }

        final ReadableDuration authnExpiry = Optional.ofNullable(document.getAuthenticationExpiry())
            .orElse(identityProvider.getServiceProviderDefaults().authenticationExpiry);

        final boolean signAuthnRequests = document.signMessages.contains(SamlServiceProviderDocument.SIGN_AUTHN);
        final boolean signLogoutRequests = document.signMessages.contains(SamlServiceProviderDocument.SIGN_LOGOUT);

        return new CloudServiceProvider(document.entityId, acs, nameIdFormats, authnExpiry, privileges,
            attributes, credentials, signAuthnRequests, signLogoutRequests);
    }

    private ServiceProviderPrivileges buildPrivileges(SamlServiceProviderDocument.Privileges configuredPrivileges) {
        final String applicationName = Optional.ofNullable(configuredPrivileges.application)
            .orElse(identityProvider.getServiceProviderDefaults().applicationName);
        final String resource = configuredPrivileges.resource;
        final String loginAction = Optional.ofNullable(configuredPrivileges.loginAction)
            .orElse(identityProvider.getServiceProviderDefaults().loginAction);
        final Map<String, String> groups = Optional.ofNullable(configuredPrivileges.groupActions).orElse(Map.of());
        return new ServiceProviderPrivileges(applicationName, resource, loginAction, groups);
    }

    private URL parseUrl(SamlServiceProviderDocument document) {
        final URL acs;
        try {
            acs = new URL(document.acs);
        } catch (MalformedURLException e) {
            final ServiceProviderException exception = new ServiceProviderException(
                "Service provider [{}] (doc {}) has an invalid ACS [{}]", e, document.entityId, document.docId, document.acs);
            exception.setEntityId(document.entityId);
            throw exception;
        }
        return acs;
    }

    private class CachedServiceProvider {
        private final String entityId;
        private final DocumentVersion documentVersion;
        private final SamlServiceProvider serviceProvider;

        private CachedServiceProvider(String entityId, DocumentVersion documentVersion, SamlServiceProvider serviceProvider) {
            this.entityId = entityId;
            this.documentVersion = documentVersion;
            this.serviceProvider = serviceProvider;
        }
    }
}

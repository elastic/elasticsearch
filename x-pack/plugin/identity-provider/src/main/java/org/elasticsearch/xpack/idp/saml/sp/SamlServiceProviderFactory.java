/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.joda.time.ReadableDuration;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.security.x509.X509Credential;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A class for creating a {@link SamlServiceProvider} from a {@link SamlServiceProviderDocument}.
 */
public final class SamlServiceProviderFactory {

    private final ServiceProviderDefaults defaults;

    public SamlServiceProviderFactory(ServiceProviderDefaults defaults) {
        this.defaults = defaults;
    }

    SamlServiceProvider buildServiceProvider(SamlServiceProviderDocument document) {
        final ServiceProviderPrivileges privileges = buildPrivileges(document.privileges);
        final SamlServiceProvider.AttributeNames attributes = new SamlServiceProvider.AttributeNames(
            document.attributeNames.principal, document.attributeNames.name, document.attributeNames.email, document.attributeNames.roles
        );
        final Set<X509Credential> credentials = document.certificates.getServiceProviderX509SigningCertificates()
            .stream()
            .map(BasicX509Credential::new)
            .collect(Collectors.toUnmodifiableSet());

        final URL acs = parseUrl(document);
        String nameIdFormat = document.nameIdFormat;
        if (nameIdFormat == null) {
            nameIdFormat = defaults.nameIdFormat;
        }

        final ReadableDuration authnExpiry = Optional.ofNullable(document.getAuthenticationExpiry())
            .orElse(defaults.authenticationExpiry);

        final boolean signAuthnRequests = document.signMessages.contains(SamlServiceProviderDocument.SIGN_AUTHN);
        final boolean signLogoutRequests = document.signMessages.contains(SamlServiceProviderDocument.SIGN_LOGOUT);

        return new CloudServiceProvider(document.entityId, document.name, document.enabled, acs, nameIdFormat, authnExpiry,
            privileges, attributes, credentials, signAuthnRequests, signLogoutRequests);
    }

    private ServiceProviderPrivileges buildPrivileges(SamlServiceProviderDocument.Privileges configuredPrivileges) {
        final String resource = configuredPrivileges.resource;
        final Function<String, Set<String>> roleMapping;
        if (configuredPrivileges.rolePatterns == null || configuredPrivileges.rolePatterns.isEmpty()) {
            roleMapping = in -> Set.of();
        } else {
            final Set<Pattern> patterns = configuredPrivileges.rolePatterns.stream()
                .map(Pattern::compile)
                .collect(Collectors.toUnmodifiableSet());
            roleMapping = action -> patterns.stream()
                .map(p -> p.matcher(action))
                .filter(Matcher::matches)
                .map(m -> m.group(1))
                .collect(Collectors.toUnmodifiableSet());
        }
        return new ServiceProviderPrivileges(defaults.applicationName, resource, roleMapping);
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
}

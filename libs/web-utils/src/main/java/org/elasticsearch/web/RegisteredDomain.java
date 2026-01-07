/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.web;

import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.elasticsearch.core.Nullable;

public class RegisteredDomain {

    private static final PublicSuffixMatcher SUFFIX_MATCHER = PublicSuffixMatcherLoader.getDefault();

    @Nullable
    public static DomainInfo getRegisteredDomain(@Nullable String fqdn) {
        if (fqdn == null || fqdn.isBlank()) {
            return null;
        }
        String registeredDomain = SUFFIX_MATCHER.getDomainRoot(fqdn);
        if (registeredDomain == null) {
            if (SUFFIX_MATCHER.matches(fqdn)) {
                return DomainInfo.of(fqdn);
            }
            return null;
        }
        if (registeredDomain.indexOf('.') == -1) {
            // we have domain with no matching public suffix, but "." in it
            return null;
        }
        return DomainInfo.of(registeredDomain, fqdn);
    }

    public record DomainInfo(
        String domain,
        String registeredDomain,
        String eTLD, // n.b. https://developer.mozilla.org/en-US/docs/Glossary/eTLD
        String subdomain
    ) {
        public static DomainInfo of(final String eTLD) {
            return new DomainInfo(eTLD, null, eTLD, null);
        }

        public static DomainInfo of(final String registeredDomain, final String domain) {
            int index = registeredDomain.indexOf('.') + 1;
            if (index > 0 && index < registeredDomain.length()) {
                int subdomainIndex = domain.lastIndexOf("." + registeredDomain);
                final String subdomain = subdomainIndex > 0 ? domain.substring(0, subdomainIndex) : null;
                return new DomainInfo(domain, registeredDomain, registeredDomain.substring(index), subdomain);
            } else {
                return new DomainInfo(null, null, null, null);
            }
        }
    }
}

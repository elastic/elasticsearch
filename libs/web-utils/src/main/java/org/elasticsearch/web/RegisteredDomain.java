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

import java.util.LinkedHashMap;

/**
 * Utility class for parsing fully qualified domain names (FQDNs) into their constituent parts:
 * domain, registered domain, top-level domain (eTLD), and subdomain.
 * <p>
 * This class uses the public suffix list to accurately determine domain boundaries.
 * For example, given "www.example.co.uk":
 * <ul>
 *     <li>domain: www.example.co.uk</li>
 *     <li>registered_domain: example.co.uk</li>
 *     <li>top_level_domain: co.uk</li>
 *     <li>subdomain: www</li>
 * </ul>
 *
 * @see <a href="https://publicsuffix.org/">Public Suffix List</a>
 * @see <a href="https://developer.mozilla.org/en-US/docs/Glossary/eTLD">eTLD (effective Top-Level Domain)</a>
 */
public class RegisteredDomain {

    public static final String DOMAIN = "domain";
    public static final String REGISTERED_DOMAIN = "registered_domain";
    public static final String eTLD = "top_level_domain";
    public static final String SUBDOMAIN = "subdomain";

    public static final LinkedHashMap<String, Class<?>> REGISTERED_DOMAIN_INFO_FIELDS;

    static {
        REGISTERED_DOMAIN_INFO_FIELDS = new LinkedHashMap<>();
        REGISTERED_DOMAIN_INFO_FIELDS.putLast(DOMAIN, String.class);
        REGISTERED_DOMAIN_INFO_FIELDS.putLast(REGISTERED_DOMAIN, String.class);
        REGISTERED_DOMAIN_INFO_FIELDS.putLast(eTLD, String.class);
        REGISTERED_DOMAIN_INFO_FIELDS.putLast(SUBDOMAIN, String.class);
    }

    private static final PublicSuffixMatcher SUFFIX_MATCHER = PublicSuffixMatcherLoader.getDefault();

    public static boolean parseRegisteredDomainInfo(@Nullable final String fqdn, final RegisteredDomainInfoCollector collector) {
        if (fqdn == null || fqdn.isBlank()) {
            return false;
        }
        String registeredDomain = SUFFIX_MATCHER.getDomainRoot(fqdn);
        if (registeredDomain == null) {
            if (SUFFIX_MATCHER.matches(fqdn)) {
                collector.topLevelDomain(fqdn);
                collector.domain(fqdn);
                return true;
            }
            return false;
        }
        int indexOfDot = registeredDomain.indexOf('.') + 1;
        if (indexOfDot > 0 && indexOfDot < registeredDomain.length()) {
            collector.domain(fqdn);
            collector.registeredDomain(registeredDomain);
            collector.topLevelDomain(registeredDomain.substring(indexOfDot));
            int subdomainIndex = fqdn.lastIndexOf("." + registeredDomain);
            if (subdomainIndex > 0) {
                collector.subdomain(fqdn.substring(0, subdomainIndex));
            }
            return true;
        }
        return false;
    }

    public static LinkedHashMap<String, Class<?>> getRegisteredDomainInfoFields() {
        return REGISTERED_DOMAIN_INFO_FIELDS;
    }

    /**
     * A collector for registered domain information.
     * Implementation can be specific to the use case, for example - it can write parsed info directly to the collecting data structure.
     */
    public interface RegisteredDomainInfoCollector {
        /**
         * @param domain the domain name
         */
        void domain(String domain);

        /**
         * @param registeredDomain the registered domain name
         */
        void registeredDomain(String registeredDomain);

        /**
         * @param topLevelDomain the top level domain, n.b. <a href="https://developer.mozilla.org/en-US/docs/Glossary/eTLD">eTLD</a>
         */
        void topLevelDomain(String topLevelDomain);

        /**
         * @param subdomain the subdomain name
         */
        void subdomain(String subdomain);
    }

    public record DomainInfo(String domain, String registeredDomain, String eTLD, String subdomain) {
        static class Factory implements RegisteredDomainInfoCollector {
            String domain;
            String registeredDomain;
            String topLevelDomain;
            String subdomain;

            @Override
            public void domain(String domain) {
                this.domain = domain;
            }

            @Override
            public void registeredDomain(String registeredDomain) {
                this.registeredDomain = registeredDomain;
            }

            @Override
            public void topLevelDomain(String topLevelDomain) {
                this.topLevelDomain = topLevelDomain;
            }

            @Override
            public void subdomain(String subdomain) {
                this.subdomain = subdomain;
            }

            public DomainInfo build() {
                return new DomainInfo(domain, registeredDomain, topLevelDomain, subdomain);
            }
        }
    }

    public static DomainInfo parseRegisteredDomainInfo(@Nullable final String fqdn) {
        DomainInfo.Factory factory = new DomainInfo.Factory();
        boolean infoFound = parseRegisteredDomainInfo(fqdn, factory);
        return infoFound ? factory.build() : null;
    }
}

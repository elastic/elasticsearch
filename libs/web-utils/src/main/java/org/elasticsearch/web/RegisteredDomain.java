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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class RegisteredDomain {

    public static final String DOMAIN = "domain";
    public static final String REGISTERED_DOMAIN = "registered_domain";
    public static final String ETLD = "top_level_domain";
    public static final String SUBDOMAIN = "subdomain";

    public static final LinkedHashMap<String, Class<?>> OUTPUT_FIELDS;
    private static final Set<String> OUTPUT_FIELD_KEYS;

    static {
        OUTPUT_FIELDS = new LinkedHashMap<>();
        OUTPUT_FIELDS.putLast(DOMAIN, String.class);
        OUTPUT_FIELDS.putLast(REGISTERED_DOMAIN, String.class);
        OUTPUT_FIELDS.putLast(ETLD, String.class);
        OUTPUT_FIELDS.putLast(SUBDOMAIN, String.class);
        OUTPUT_FIELD_KEYS = OUTPUT_FIELDS.keySet();
    }

    private static final PublicSuffixMatcher SUFFIX_MATCHER = PublicSuffixMatcherLoader.getDefault();

    @Nullable
    public static Map<String, String> getRegisteredDomainInfo(@Nullable String fqdn) {
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

    public static Map<String, Class<?>> getRegisteredDomainInfoFields() {
        return OUTPUT_FIELDS;
    }

    /**
     * A map implementation for the domain info that does not incur lookup overhead like a {@code HashMap} for example.
     * Keys are known in advance and lookup relies on an efficient switch statement rather than {@code hashCode()} and {@code equals()}.
     *
     * @param domain the domain name
     * @param registeredDomain the registered domain name
     * @param eTLD n.b. <a href="https://developer.mozilla.org/en-US/docs/Glossary/eTLD">eTLD</a>
     * @param subdomain the subdomain name
     */
    @SuppressWarnings("NullableProblems")
    record DomainInfo(String domain, String registeredDomain, String eTLD, String subdomain) implements Map<String, String> {
        static DomainInfo of(final String eTLD) {
            return new DomainInfo(eTLD, null, eTLD, null);
        }

        static DomainInfo of(final String registeredDomain, final String domain) {
            int index = registeredDomain.indexOf('.') + 1;
            if (index > 0 && index < registeredDomain.length()) {
                int subdomainIndex = domain.lastIndexOf("." + registeredDomain);
                final String subdomain = subdomainIndex > 0 ? domain.substring(0, subdomainIndex) : null;
                return new DomainInfo(domain, registeredDomain, registeredDomain.substring(index), subdomain);
            } else {
                return new DomainInfo(null, null, null, null);
            }
        }

        @Override
        public int size() {
            return OUTPUT_FIELDS.size();
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            return OUTPUT_FIELD_KEYS.contains(key.toString());
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException("This map does not support containsValue");
        }

        /**
         * A quick lookup that takes advantage of the fact that keys are known in advance.
         * Assuming that lookup relies on the public constants above (e.g {@link #DOMAIN}), the switch statement is expected to be
         * very efficient as it doesn't require computing hash codes or checking equality of arbitrary strings.
         *
         * @param key the key whose associated value is to be returned
         * @return the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the key
         */
        @Override
        public String get(Object key) {
            return switch ((String) key) {
                case DOMAIN -> domain;
                case REGISTERED_DOMAIN -> registeredDomain;
                case ETLD -> eTLD;
                case SUBDOMAIN -> subdomain;
                default -> null;
            };
        }

        @Override
        public String put(String key, String value) {
            throw new UnsupportedOperationException("This map is populated with values at construction time");
        }

        @Override
        public String remove(Object key) {
            throw new UnsupportedOperationException("This map is unmodifiable");
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> m) {
            throw new UnsupportedOperationException("This map is unmodifiable");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("This map is unmodifiable");
        }

        @Override
        public Set<String> keySet() {
            return OUTPUT_FIELD_KEYS;
        }

        @Override
        public Collection<String> values() {
            throw new UnsupportedOperationException("This map does not support iteration over values");
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            throw new UnsupportedOperationException("This map does not support iteration over entries");
        }
    }
}

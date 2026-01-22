/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class RegisteredDomainProcessor extends AbstractProcessor {

    public static final String TYPE = "registered_domain";
    private static final PublicSuffixMatcher SUFFIX_MATCHER = PublicSuffixMatcherLoader.getDefault();

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;

    RegisteredDomainProcessor(String tag, String description, String field, String targetField, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean getIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        final String fqdn = document.getFieldValue(field, String.class, ignoreMissing);
        final DomainInfo info = getRegisteredDomain(fqdn);
        if (info == null) {
            if (ignoreMissing) {
                return document;
            } else {
                throw new IllegalArgumentException("unable to set domain information for document");
            }
        }
        String fieldPrefix = targetField;
        if (fieldPrefix.isEmpty() == false) {
            fieldPrefix += ".";
        }
        String domainTarget = fieldPrefix + "domain";
        String registeredDomainTarget = fieldPrefix + "registered_domain";
        String subdomainTarget = fieldPrefix + "subdomain";
        String topLevelDomainTarget = fieldPrefix + "top_level_domain";

        if (info.domain() != null) {
            document.setFieldValue(domainTarget, info.domain());
        }
        if (info.registeredDomain() != null) {
            document.setFieldValue(registeredDomainTarget, info.registeredDomain());
        }
        if (info.eTLD() != null) {
            document.setFieldValue(topLevelDomainTarget, info.eTLD());
        }
        if (info.subdomain() != null) {
            document.setFieldValue(subdomainTarget, info.subdomain());
        }
        return document;
    }

    @Nullable
    // visible for testing
    static DomainInfo getRegisteredDomain(@Nullable String fqdn) {
        if (Strings.hasText(fqdn) == false) {
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

    @Override
    public String getType() {
        return TYPE;
    }

    // visible for testing
    record DomainInfo(
        String domain,
        String registeredDomain,
        String eTLD, // n.b. https://developer.mozilla.org/en-US/docs/Glossary/eTLD
        String subdomain
    ) {
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
    }

    public static final class Factory implements Processor.Factory {

        static final String DEFAULT_TARGET_FIELD = "";

        @Override
        public RegisteredDomainProcessor create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field", DEFAULT_TARGET_FIELD);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", true);

            return new RegisteredDomainProcessor(tag, description, field, targetField, ignoreMissing);
        }
    }
}

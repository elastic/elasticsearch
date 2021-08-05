/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class RegisteredDomainProcessor extends AbstractProcessor {
    private static final PublicSuffixMatcher SUFFIX_MATCHER = PublicSuffixMatcherLoader.getDefault();

    public static final String TYPE = "registered_domain";

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;

    RegisteredDomainProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing
    ) {
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
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        DomainInfo info = getRegisteredDomain(ingestDocument);
        if (info == null) {
            if (ignoreMissing) {
                return ingestDocument;
            } else {
                throw new IllegalArgumentException("unable to set domain information for document");
            }
        }
        String fieldPrefix = targetField;
        if (fieldPrefix.equals("") == false) {
            fieldPrefix += ".";
        }
        String domainTarget = fieldPrefix + "domain";
        String registeredDomainTarget = fieldPrefix + "registered_domain";
        String subdomainTarget = fieldPrefix + "subdomain";
        String topLevelDomainTarget = fieldPrefix + "top_level_domain";

        if (info.getDomain() != null) {
            ingestDocument.setFieldValue(domainTarget, info.getDomain());
        }
        if (info.getRegisteredDomain() != null) {
            ingestDocument.setFieldValue(registeredDomainTarget, info.getRegisteredDomain());
        }
        if (info.getETLD() != null) {
            ingestDocument.setFieldValue(topLevelDomainTarget, info.getETLD());
        }
        if (info.getSubdomain() != null) {
            ingestDocument.setFieldValue(subdomainTarget, info.getSubdomain());
        }
        return ingestDocument;
    }

    private DomainInfo getRegisteredDomain(IngestDocument d) {
        String fieldString = d.getFieldValue(field, String.class, ignoreMissing);
        if (fieldString == null) {
            return null;
        }
        String registeredDomain = SUFFIX_MATCHER.getDomainRoot(fieldString);
        if (registeredDomain == null) {
            if (SUFFIX_MATCHER.matches(fieldString)) {
                return new DomainInfo(fieldString);
            }
            return null;
        }
        if (registeredDomain.indexOf(".") == -1) {
            // we have domain with no matching public suffix, but "." in it
            return null;
        }
        return new DomainInfo(registeredDomain, fieldString);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private class DomainInfo {
        private final String domain;
        private final String registeredDomain;
        private final String eTLD;
        private final String subdomain;

        private DomainInfo(String eTLD) {
            this.domain = eTLD;
            this.eTLD = eTLD;
            this.registeredDomain = null;
            this.subdomain = null;
        }

        private DomainInfo(String registeredDomain, String domain) {
            int index = registeredDomain.indexOf(".") + 1;
            if (index > 0 && index < registeredDomain.length()) {
                this.domain = domain;
                this.eTLD = registeredDomain.substring(index);
                this.registeredDomain = registeredDomain;
                int subdomainIndex = domain.lastIndexOf("." + registeredDomain);
                if (subdomainIndex > 0) {
                    this.subdomain = domain.substring(0, subdomainIndex);
                } else {
                    this.subdomain = null;
                }
            } else {
                this.domain = null;
                this.eTLD = null;
                this.registeredDomain = null;
                this.subdomain = null;
            }
        }

        public String getDomain() {
            return domain;
        }

        public String getSubdomain() {
            return subdomain;
        }

        public String getRegisteredDomain() {
            return registeredDomain;
        }

        public String getETLD() {
            return eTLD;
        }
    }

    public static final class Factory implements Processor.Factory {

        static final String DEFAULT_TARGET_FIELD = "";

        @Override
        public RegisteredDomainProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", DEFAULT_TARGET_FIELD);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);

            return new RegisteredDomainProcessor(
                processorTag,
                description,
                field,
                targetField,
                ignoreMissing
            );
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest;

import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class RegisteredDomainProcessor extends AbstractProcessor {
    public static final PublicSuffixMatcher SUFFIX_MATCHER = PublicSuffixMatcherLoader.getDefault();

    public static final String TYPE = "registered_domain";

    private final String field;
    private final String targetField;
    private final String targetETLDField;
    private final String targetSubdomainField;
    private final boolean ignoreMissing;

    RegisteredDomainProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        String targetETLDField,
        String targetSubdomainField,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.targetETLDField = targetETLDField;
        this.targetSubdomainField = targetSubdomainField;
        this.ignoreMissing = ignoreMissing;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public String getTargetETLDField() {
        return targetETLDField;
    }

    public String getTargetSubdomainField() {
        return targetSubdomainField;
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
        if (info.getRegisteredDomain() != null) {
            ingestDocument.setFieldValue(targetField, info.getRegisteredDomain());
        }
        if (targetETLDField != null && info.getTLD() != null) {
            ingestDocument.setFieldValue(targetETLDField, info.getTLD());
        }
        if (targetSubdomainField != null && info.getSubdomain() != null) {
            ingestDocument.setFieldValue(targetSubdomainField, info.getSubdomain());
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
        private final String registeredDomain;
        private final String tld;
        private final String subdomain;

        private DomainInfo(String tld) {
            this.tld = tld;
            this.registeredDomain = null;
            this.subdomain = null;
        }

        private DomainInfo(String registeredDomain, String domain) {
            int index = registeredDomain.indexOf(".") + 1;
            if (index > 0 && index < registeredDomain.length()) {
                this.tld = registeredDomain.substring(index);
                this.registeredDomain = registeredDomain;
                int subdomainIndex = domain.lastIndexOf("." + registeredDomain);
                if (subdomainIndex > 0) {
                    this.subdomain = domain.substring(0, subdomainIndex);
                } else {
                    this.subdomain = null;
                }
            } else {
                this.tld = null;
                this.registeredDomain = null;
                this.subdomain = null;
            }
        }

        public String getSubdomain() {
            return subdomain;
        }

        public String getRegisteredDomain() {
            return registeredDomain;
        }

        public String getTLD() {
            return tld;
        }
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public RegisteredDomainProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            String targetETLDField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_etld_field");
            String targetSubdomainField = ConfigurationUtils.readOptionalStringProperty(
                TYPE,
                processorTag,
                config,
                "target_subdomain_field"
            );
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);

            return new RegisteredDomainProcessor(
                processorTag,
                description,
                field,
                targetField,
                targetETLDField,
                targetSubdomainField,
                ignoreMissing
            );
        }
    }
}

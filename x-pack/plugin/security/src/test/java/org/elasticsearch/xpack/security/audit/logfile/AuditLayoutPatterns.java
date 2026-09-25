/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class AuditLayoutPatterns {

    static final String ACTIVE_PATTERN_KEY = "appender.audit_rolling.layout.pattern";
    static final String DEFAULT_FORMAT_PROPERTY = "property.audit_default_format";
    static final String LEGACY_FORMAT_PROPERTY = "property.audit_legacy_format";

    private AuditLayoutPatterns() {}

    static Properties loadAuditConfig() throws IOException {
        final Properties properties = new Properties();
        try (InputStream configStream = LoggingAuditTrail.class.getClassLoader().getResourceAsStream("log4j2.properties")) {
            if (configStream == null) {
                throw new AssertionError("log4j2.properties not found on the test classpath");
            }
            properties.load(configStream);
        }
        return properties;
    }

    static String activePattern(Properties properties) {
        final String pattern = properties.getProperty(ACTIVE_PATTERN_KEY);
        if (pattern == null) {
            throw new AssertionError("the audit log4j2.properties does not define [" + ACTIVE_PATTERN_KEY + "]");
        }
        final String reference = pattern.trim();
        if (reference.startsWith("${") && reference.endsWith("}")) {
            final String formatProperty = "property." + reference.substring(2, reference.length() - 1);
            final String format = properties.getProperty(formatProperty);
            if (format == null) {
                throw new AssertionError(
                    "[" + ACTIVE_PATTERN_KEY + "] references [" + reference + "] but [" + formatProperty + "] is not defined"
                );
            }
            return format;
        }
        return pattern;
    }
}

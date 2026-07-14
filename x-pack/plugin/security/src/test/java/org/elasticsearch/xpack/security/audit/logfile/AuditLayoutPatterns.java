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

/**
 * Test-side access to the audit appender's layout as shipped in the x-pack {@code log4j2.properties} config file (which the build puts
 * on the test classpath). The file defines the available audit entry formats as log4j configuration properties ({@code property.<name>})
 * and assigns one of them to the appender's {@code layout.pattern} via a {@code ${<name>}} reference. {@link java.util.Properties}
 * performs no log4j substitution, so tests reading the active pattern must resolve that one level of indirection themselves — this
 * helper centralizes it. Reassigning the pattern to the other format property in the config file therefore transparently changes which
 * layout the tests (and the profiling harness) exercise, with no test edits.
 */
final class AuditLayoutPatterns {

    /** The key holding the active audit layout pattern: a {@code ${...}} reference to one of the format properties below. */
    static final String ACTIVE_PATTERN_KEY = "appender.audit_rolling.layout.pattern";
    /** The default format: the audit entry rendered as a single prebuilt JSON message via {@code %m}. */
    static final String DEFAULT_FORMAT_PROPERTY = "property.audit_default_format";
    /** The pre-{@code %m} format extracting each field with {@code %map{...}}; kept for upgraded or customized deployments. */
    static final String LEGACY_FORMAT_PROPERTY = "property.audit_legacy_format";

    private AuditLayoutPatterns() {}

    /** Loads the audit {@code log4j2.properties} from the classpath. */
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

    /**
     * Returns the active audit layout pattern with the {@code ${<format property>}} reference resolved, i.e. the pattern that log4j
     * would build the audit appender's layout from.
     */
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

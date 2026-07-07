/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.ACTION_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.CLUSTER_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.EVENT_ACTION_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.EVENT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.INDICES_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.NODE_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PUT_CONFIG_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REQUEST_BODY_FIELD_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Guards the backwards-compatibility contract that {@link FastLogEntryAccumulator} must honor for the audit {@code log4j2.properties}
 * layout.
 *
 * <p>Both formats are read from the <em>shipped</em> config file (via {@link AuditLayoutPatterns}), where they are defined as the
 * {@code audit_default_format} and {@code audit_legacy_format} properties: the same log event is rendered through the two layouts and
 * the resulting audit lines must be byte-for-byte identical. Any edit to
 * either format property that breaks the equivalence fails these tests.
 */
public class FastLogEntryAccumulatorMapLayoutCompatibilityTests extends ESTestCase {

    private static PatternLayout defaultLayout;
    private static PatternLayout legacyLayout;

    @BeforeClass
    public static void buildLayoutsFromShippedConfig() throws Exception {
        final Properties properties = AuditLayoutPatterns.loadAuditConfig();
        defaultLayout = layoutFor(properties, AuditLayoutPatterns.DEFAULT_FORMAT_PROPERTY);
        legacyLayout = layoutFor(properties, AuditLayoutPatterns.LEGACY_FORMAT_PROPERTY);
    }

    @AfterClass
    public static void releaseLayouts() {
        defaultLayout = null;
        legacyLayout = null;
    }

    private static PatternLayout layoutFor(Properties properties, String formatProperty) {
        final String pattern = properties.getProperty(formatProperty);
        assertThat(
            "the shipped audit config must define [" + formatProperty + "]; it is part of the audit layout compatibility contract",
            pattern,
            notNullValue()
        );
        return PatternLayout.newBuilder().setPattern(pattern).setConfiguration(new DefaultConfiguration()).build();
    }

    /**
     * A single event is rendered through both layouts, so the {@code %d} timestamp (taken from the event's fixed instant) and the
     * message render from identical inputs and whole audit lines can be compared.
     */
    private static LogEvent eventFor(FastLogEntryAccumulator accumulator) {
        return Log4jLogEvent.newBuilder().setLoggerName("audit").setLevel(Level.INFO).setMessage(accumulator).build();
    }

    /**
     * A representative accumulator covering every {@link FastLogEntryAccumulator.FieldType}: scalar strings (including one that needs JSON
     * escaping), a string array ({@code user.roles}), and a raw JSON fragment ({@code put}). Fields are set out of slot order to also
     * exercise ordering.
     */
    private static FastLogEntryAccumulator populatedEntry() {
        return new FastLogEntryAccumulator(Map.of())
            // scalar (common-like) fields
            .with(CLUSTER_NAME_FIELD_NAME, "my-cluster")
            .with(NODE_NAME_FIELD_NAME, "node-1")
            .with(EVENT_TYPE_FIELD_NAME, "transport")
            .with(EVENT_ACTION_FIELD_NAME, "access_granted")
            .with(PRINCIPAL_FIELD_NAME, "elastic")
            // scalar needing escaping
            .with(ACTION_FIELD_NAME, "say \"hi\"\\done")
            .with(REQUEST_BODY_FIELD_NAME, "line1\nline2")
            // string array
            .with(PRINCIPAL_ROLES_FIELD_NAME, new Object[] { "superuser", "kibana_admin" })
            .with(INDICES_FIELD_NAME, new Object[] { "index-a", "index-b" })
            // raw JSON fragment
            .with(PUT_CONFIG_FIELD_NAME, "{\"role\":{\"cluster\":[\"all\"]}}");
    }

    public void testLegacyFormatMatchesDefaultFormat() {
        final LogEvent event = eventFor(populatedEntry());
        assertThat(legacyLayout.toSerializable(event), equalTo(defaultLayout.toSerializable(event)));
    }

    public void testLegacyFormatStillPopulatesFields() {
        // Guards against a false pass where both renderings carry no fields: confirm the legacy %map path actually resolves values.
        final String viaLegacyFormat = legacyLayout.toSerializable(eventFor(populatedEntry()));
        assertThat(viaLegacyFormat, containsString("\"cluster.name\":\"my-cluster\""));
        assertThat(viaLegacyFormat, containsString("\"user.roles\":[\"superuser\",\"kibana_admin\"]"));
        assertThat(viaLegacyFormat, containsString("\"put\":{\"role\":{\"cluster\":[\"all\"]}}"));
    }

    public void testEmptyEntryRendersIdenticallyThroughBothFormats() {
        final FastLogEntryAccumulator empty = new FastLogEntryAccumulator(Map.of());
        final LogEvent event = eventFor(empty);
        final String viaLegacyFormat = legacyLayout.toSerializable(event);
        assertThat(viaLegacyFormat, equalTo(defaultLayout.toSerializable(event)));
        // both are just the wrapper: no fields, but still a well-formed audit line
        assertThat(viaLegacyFormat, containsString("\"type\":\"audit\""));
        assertThat(empty.getFormattedMessage(), equalTo(""));
    }
}

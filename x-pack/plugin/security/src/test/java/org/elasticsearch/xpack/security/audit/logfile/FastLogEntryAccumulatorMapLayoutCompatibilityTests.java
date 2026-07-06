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

import java.util.Map;

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
import static org.hamcrest.Matchers.not;

/**
 * Guards the backwards-compatibility contract that {@link FastLogEntryAccumulator} must honour for the audit {@code log4j2.properties}
 * layout. That file is user-owned and preserved across upgrades (and is documented as customizable), so existing and customized
 * deployments keep the pre-change layout that extracts each field with {@code %map{field}} rather than the new {@code %m}. log4j's
 * {@code MapPatternConverter} only serves {@code %map{...}} when the log event's message is a {@code MapMessage}; if the accumulator were
 * not one, every {@code %map{...}} would silently resolve to empty and audit lines would lose all their fields.
 *
 * <p>These tests render a populated accumulator through the <em>actual</em> pre-change {@code %map}/{@code %enc}/{@code %varsNotEmpty}
 * pattern and assert the result is byte-for-byte identical to the new {@code %m} path ({@link FastLogEntryAccumulator#getFormattedMessage}),
 * proving the two layouts remain interchangeable.
 */
public class FastLogEntryAccumulatorMapLayoutCompatibilityTests extends ESTestCase {

    /**
     * The field-emitting portion of the original audit layout pattern (copied verbatim from {@code log4j2.properties} as it existed before
     * this change, minus the {@code {"type":"audit","timestamp":...}} wrapper and the properties-file line continuations). {@code %m} in
     * the new layout replaces exactly this run of converters.
     */
    private static final String LEGACY_MAP_PATTERN = "%varsNotEmpty{, \"cluster.name\":\"%enc{%map{cluster.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"cluster.uuid\":\"%enc{%map{cluster.uuid}}{JSON}\"}"
        + "%varsNotEmpty{, \"node.name\":\"%enc{%map{node.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"node.id\":\"%enc{%map{node.id}}{JSON}\"}"
        + "%varsNotEmpty{, \"host.name\":\"%enc{%map{host.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"host.ip\":\"%enc{%map{host.ip}}{JSON}\"}"
        + "%varsNotEmpty{, \"event.type\":\"%enc{%map{event.type}}{JSON}\"}"
        + "%varsNotEmpty{, \"event.action\":\"%enc{%map{event.action}}{JSON}\"}"
        + "%varsNotEmpty{, \"authentication.type\":\"%enc{%map{authentication.type}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.name\":\"%enc{%map{user.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.run_by.name\":\"%enc{%map{user.run_by.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.run_as.name\":\"%enc{%map{user.run_as.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.realm\":\"%enc{%map{user.realm}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.realm_domain\":\"%enc{%map{user.realm_domain}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.run_by.realm\":\"%enc{%map{user.run_by.realm}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.run_by.realm_domain\":\"%enc{%map{user.run_by.realm_domain}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.run_as.realm\":\"%enc{%map{user.run_as.realm}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.run_as.realm_domain\":\"%enc{%map{user.run_as.realm_domain}}{JSON}\"}"
        + "%varsNotEmpty{, \"user.roles\":%map{user.roles}}"
        + "%varsNotEmpty{, \"apikey.id\":\"%enc{%map{apikey.id}}{JSON}\"}"
        + "%varsNotEmpty{, \"apikey.name\":\"%enc{%map{apikey.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"authentication.token.name\":\"%enc{%map{authentication.token.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"authentication.token.type\":\"%enc{%map{authentication.token.type}}{JSON}\"}"
        + "%varsNotEmpty{, \"cross_cluster_access\":%map{cross_cluster_access}}"
        + "%varsNotEmpty{, \"origin.type\":\"%enc{%map{origin.type}}{JSON}\"}"
        + "%varsNotEmpty{, \"origin.address\":\"%enc{%map{origin.address}}{JSON}\"}"
        + "%varsNotEmpty{, \"realm\":\"%enc{%map{realm}}{JSON}\"}"
        + "%varsNotEmpty{, \"realm_domain\":\"%enc{%map{realm_domain}}{JSON}\"}"
        + "%varsNotEmpty{, \"url.path\":\"%enc{%map{url.path}}{JSON}\"}"
        + "%varsNotEmpty{, \"url.query\":\"%enc{%map{url.query}}{JSON}\"}"
        + "%varsNotEmpty{, \"request.method\":\"%enc{%map{request.method}}{JSON}\"}"
        + "%varsNotEmpty{, \"request.body\":\"%enc{%map{request.body}}{JSON}\"}"
        + "%varsNotEmpty{, \"request.id\":\"%enc{%map{request.id}}{JSON}\"}"
        + "%varsNotEmpty{, \"action\":\"%enc{%map{action}}{JSON}\"}"
        + "%varsNotEmpty{, \"request.name\":\"%enc{%map{request.name}}{JSON}\"}"
        + "%varsNotEmpty{, \"indices\":%map{indices}}"
        + "%varsNotEmpty{, \"opaque_id\":\"%enc{%map{opaque_id}}{JSON}\"}"
        + "%varsNotEmpty{, \"trace.id\":\"%enc{%map{trace.id}}{JSON}\"}"
        + "%varsNotEmpty{, \"x_forwarded_for\":\"%enc{%map{x_forwarded_for}}{JSON}\"}"
        + "%varsNotEmpty{, \"transport.profile\":\"%enc{%map{transport.profile}}{JSON}\"}"
        + "%varsNotEmpty{, \"rule\":\"%enc{%map{rule}}{JSON}\"}"
        + "%varsNotEmpty{, \"put\":%map{put}}"
        + "%varsNotEmpty{, \"delete\":%map{delete}}"
        + "%varsNotEmpty{, \"change\":%map{change}}"
        + "%varsNotEmpty{, \"create\":%map{create}}"
        + "%varsNotEmpty{, \"invalidate\":%map{invalidate}}";

    private static String renderThroughLegacyLayout(FastLogEntryAccumulator accumulator) {
        final PatternLayout layout = PatternLayout.newBuilder()
            .setPattern(LEGACY_MAP_PATTERN)
            .setConfiguration(new DefaultConfiguration())
            .build();
        final LogEvent event = Log4jLogEvent.newBuilder()
            .setLoggerName("audit")
            .setLevel(Level.INFO)
            .setMessage(accumulator)
            .build();
        return layout.toSerializable(event);
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

    public void testLegacyMapLayoutMatchesNewMessageRendering() {
        final FastLogEntryAccumulator accumulator = populatedEntry();
        final String viaNewMessagePath = accumulator.getFormattedMessage();
        final String viaLegacyMapPattern = renderThroughLegacyLayout(accumulator);
        assertThat(viaLegacyMapPattern, equalTo(viaNewMessagePath));
    }

    public void testLegacyMapLayoutStillPopulatesFields() {
        // Guards against a false pass where both renderings are empty: confirm the legacy %map path actually resolves values.
        final String viaLegacyMapPattern = renderThroughLegacyLayout(populatedEntry());
        assertThat(viaLegacyMapPattern, not(equalTo("")));
        assertThat(viaLegacyMapPattern, containsString("\"cluster.name\":\"my-cluster\""));
        assertThat(viaLegacyMapPattern, containsString("\"user.roles\":[\"superuser\",\"kibana_admin\"]"));
        assertThat(viaLegacyMapPattern, containsString("\"put\":{\"role\":{\"cluster\":[\"all\"]}}"));
    }

    public void testEmptyEntryRendersEmptyThroughBothLayouts() {
        final FastLogEntryAccumulator empty = new FastLogEntryAccumulator(Map.of());
        assertThat(renderThroughLegacyLayout(empty), equalTo(""));
        assertThat(empty.getFormattedMessage(), equalTo(""));
    }
}

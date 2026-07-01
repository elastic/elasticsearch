/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit;

/**
 * Extension point for customizing how {@code LoggingAuditTrail} processes events.
 */
public interface AuditLogCustomizer {

    /**
     * Invoked before an audit event is logged. Returning {@code true} drops the event entirely, providing programmatic suppression
     * beyond what the settings-based {@code ignore_filters} can express.
     */
    default boolean suppress(AuditEventContext ctx) {
        return false;
    }

    /**
     * Final hook to add or overwrite fields on an audit entry just before it is logged.
     * <p>
     * Note that the audit output is a fixed, layout-coupled schema: a field set here is only rendered if its key is also enumerated in
     * the active audit {@code PatternLayout} (the {@code log4j2.properties} file(s)). Overwriting an existing audit field works
     * out of the box, but surfacing a brand-new key additionally requires adding it to the layout used by the deployment, otherwise the
     * value is stored on the entry but silently dropped when the entry is rendered.
     */
    default void enrich(AuditEventContext ctx, AuditEntry entry) {
        // no-op
    }

    AuditLogCustomizer NOOP = new AuditLogCustomizer() {};
}

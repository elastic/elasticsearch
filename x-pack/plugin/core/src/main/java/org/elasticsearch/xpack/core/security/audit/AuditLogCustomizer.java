/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit;

import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.Message;

/**
 * Extension point for customizing how {@code LoggingAuditTrail} processes events.
 * <p>
 * An implementation is contributed through
 * {@link org.elasticsearch.xpack.core.security.SecurityExtension#getAuditLogCustomizer}. When no extension provides one, the
 * {@link #NOOP} instance is used and audit output is unchanged.
 */
public interface AuditLogCustomizer {

    /**
     * Decides whether an audit event should be dropped before it is written.
     * <p>
     * Invoked by {@code LoggingAuditTrail} for each event that would otherwise be emitted, after the event type has been checked
     * against the configured include/exclude filters.
     *
     * @param ctx read-only context describing the event
     * @return {@code true} to discard the event, {@code false} to let it be written
     */
    default boolean suppress(AuditEventContext ctx) {
        return false;
    }

    /**
     * Returns the message to write for an audit event that is about to be logged.
     * <p>
     * Invoked by {@code LoggingAuditTrail} as the last step of building an entry, so all standard fields are already populated, and
     * only for events that were not dropped by {@link #suppress(AuditEventContext)}. An implementation may return the entry as-is
     * or build a different message: only what it returns is logged.
     *
     * @param ctx   read-only context describing the event
     * @param entry the entry built for this event
     * @return the message to log
     */
    default Message rewrite(AuditEventContext ctx, MapMessage<?, ?> entry) {
        return entry;
    }

    /**
     * A customizer that suppresses nothing and rewrites nothing, preserving the default audit behavior.
     */
    AuditLogCustomizer NOOP = new AuditLogCustomizer() {};
}

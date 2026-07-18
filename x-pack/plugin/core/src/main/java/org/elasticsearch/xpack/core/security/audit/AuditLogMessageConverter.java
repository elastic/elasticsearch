/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit;

import org.apache.logging.log4j.message.Message;
import org.elasticsearch.xpack.core.security.audit.data.DataObject;

/**
 * Strategy for rendering a fully-built and enriched audit {@link DataObject} into the log4j {@link Message} that
 * {@code LoggingAuditTrail} writes.
 * <p>
 * A custom {@link AuditLogCustomizer} may supply its own converter via {@link AuditLogCustomizer#messageConverter()} to control the
 * emitted message shape; otherwise the audit trail applies its standard, back-compatible rendering.
 */
@FunctionalInterface
public interface AuditLogMessageConverter {

    /**
     * Renders the given entry as a log4j {@link Message}.
     *
     * @param entry the audit entry to render
     * @return the log4j message to write
     */
    Message convert(DataObject entry);
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit;

import org.elasticsearch.core.Nullable;

/**
 * Read-only context describing a single audit event.
 *
 * @param indices the indices the event relates to, or {@code null} if the event is not index-scoped
 * @param realm   the name of the realm associated with the event, or {@code null} if none applies
 */
public record AuditEventContext(@Nullable String[] indices, @Nullable String realm) {

    /**
     * An empty context, used when no event-specific information is available.
     */
    public static final AuditEventContext EMPTY = new AuditEventContext(null, null);
}

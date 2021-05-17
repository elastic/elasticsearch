/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import java.util.Date;

/**
 * {@link AbstractAuditMessageFactory} interface provides means for creating audit messages.
 * @param <T> type of the audit message
 */
public interface AbstractAuditMessageFactory<T extends AbstractAuditMessage> {

    T newMessage(String resourceId, String message, Level level, Date timestamp, String nodeName);
}

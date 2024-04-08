/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Date;
import java.util.Optional;

public class SystemAuditMessage extends AbstractAuditMessage {

    private static final String SYSTEM = "system";

    public static final ConstructingObjectParser<SystemAuditMessage, Void> PARSER = createParser(
        "ml_system_audit_message",
        (resourceId, message, level, timestamp, nodeName) -> new SystemAuditMessage(message, level, timestamp, nodeName),
        Job.ID
    );

    public SystemAuditMessage(String message, Level level, Date timestamp, String nodeName) {
        super(null, message, level, timestamp, nodeName);
    }

    @Override
    public String getJobType() {
        return SYSTEM;
    }

    @Override
    protected Optional<String> getResourceField() {
        return Optional.empty();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.logging.Level;

import java.util.concurrent.TimeUnit;

/**
 * Generic log producer class.
 * Each log producer receives a context and decides whether or not to log, and at which level. Then it extracts logging information
 * from the context and places it into the message. The producer defines which fields are included in the specific log message.
 * @param <Context> Specific logger context
 */
public interface ActionLoggerProducer<Context extends ActionLoggerContext> {
    ESLogMessage produce(Context context, ActionLoggingFields additionalFields);

    default Level logLevel(Context context, Level defaultLevel) {
        return defaultLevel;
    }

    String loggerName();

    /**
     * Produces a {@link ESLogMessage} with common fields.
     */
    default ESLogMessage produceCommon(Context context, ActionLoggingFields additionalFields) {
        var fields = new ESLogMessage();
        fields.withFields(additionalFields.logFields());
        fields.field("x_opaque_id", context.getOpaqueId());
        long tookInNanos = context.getTookInNanos();
        fields.field("took", tookInNanos);
        fields.field("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        fields.field("success", context.isSuccess());
        fields.field("type", context.getType());
        if (context.isSuccess() == false) {
            fields.field("error.type", context.getErrorType());
            fields.field("error.message", context.getErrorMessage());
        }
        return fields;
    }
}

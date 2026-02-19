/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Generic log producer class.
 * Each log producer receives a context and decides whether to log, and at which level. Then it extracts logging information
 * from the context and places it into the message. The producer defines which fields are included in the specific log message.
 *
 * @param <Context> Specific logger context
 */
public interface ActivityLogProducer<Context extends ActivityLoggerContext> {

    String ES_FIELDS_PREFIX = "elasticsearch.activitylog.";
    String X_OPAQUE_ID_FIELD = "http.request.headers.x_opaque_id";
    String EVENT_OUTCOME_FIELD = "event.outcome";
    String EVENT_DURATION_FIELD = "event.duration";

    /**
     * Produces a {@link ESLogMessage} if the producer decides to log, or nothing otherwise.
     */
    Optional<ESLogMessage> produce(Context context, ActionLoggingFields additionalFields);

    String loggerName();

    /**
     * Produces a {@link ESLogMessage} with common fields.
     */
    default ESLogMessage produceCommon(Context context, ActionLoggingFields additionalFields) {
        var fields = new ESLogMessage();
        fields.withFields(additionalFields.logFields());
        fields.field(X_OPAQUE_ID_FIELD, context.getOpaqueId());
        long tookInNanos = context.getTookInNanos();
        fields.field(EVENT_DURATION_FIELD, tookInNanos);
        fields.field(ES_FIELDS_PREFIX + "took", tookInNanos);
        fields.field(ES_FIELDS_PREFIX + "took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        fields.field(EVENT_OUTCOME_FIELD, context.isSuccess() ? "success" : "failure");
        fields.field(ES_FIELDS_PREFIX + "type", context.getType());
        if (context.isSuccess() == false) {
            fields.field("error.type", context.getErrorType());
            fields.field("error.message", context.getErrorMessage());
        }
        return fields;
    }
}

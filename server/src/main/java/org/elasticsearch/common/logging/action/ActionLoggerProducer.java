/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.SlowLogFields;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public interface ActionLoggerProducer<Context extends ActionLoggerContext> {
    ESLogMessage produce(Context context, SlowLogFields additionalFields);

    Level logLevel(Context context);

    default ESLogMessage produceCommon(Context context, SlowLogFields additionalFields) {
        var fields = new HashMap<String, Object>(additionalFields.queryFields());
        fields.put("task_id", context.getTaskId());
        long tookInNanos = context.getTookInNanos();
        fields.put("took", tookInNanos);
        fields.put("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        fields.put("success", Boolean.toString(context.isSuccess()));
        fields.put("type", context.getType());
        if (context.isSuccess() == false) {
            fields.put("error_type", context.getErrorType());
            fields.put("error_message", context.getErrorMessage());
        }
        return new ESLogMessage().withFields(fields);
    }
}

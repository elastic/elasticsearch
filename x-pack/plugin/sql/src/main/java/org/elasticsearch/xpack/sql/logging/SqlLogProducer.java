/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.logging;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Optional;

public class SqlLogProducer implements ActivityLogProducer<SqlLogContext> {

    public static final String LOGGER_NAME = "sql.activitylog";

    @Override
    public Optional<ESLogMessage> produce(SqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        return Optional.of(msg.field(ES_FIELDS_PREFIX + "query", context.getQuery()).field(ES_FIELDS_PREFIX + "rows", context.getRows()));
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }
}

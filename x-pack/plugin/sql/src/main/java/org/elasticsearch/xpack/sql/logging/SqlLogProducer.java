/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.logging;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.logging.Level;

public class SqlLogProducer implements ActionLoggerProducer<SqlLogContext> {

    public static final String LOGGER_NAME = "sql.actionlog";

    @Override
    public ESLogMessage produce(Level level, SqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(level, context, additionalFields);
        msg.with("query", context.getQuery());
        msg.with("rows", context.getRows());
        return msg;
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }
}

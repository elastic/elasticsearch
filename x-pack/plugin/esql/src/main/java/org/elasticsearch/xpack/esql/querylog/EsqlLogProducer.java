/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.logging.Level;

public class EsqlLogProducer implements ActionLoggerProducer<EsqlLogContext> {

    public static final String LOGGER_NAME = "esql.actionlog";

    @Override
    public ESLogMessage produce(Level level, EsqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(level, context, additionalFields);
        msg.with("query", context.getQuery());
        return msg;
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }
}

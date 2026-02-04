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

public class EsqlLogProducer implements ActionLoggerProducer<EsqlLogContext> {

    public static final String LOGGER_NAME = "esql.actionlog";

    @Override
    public ESLogMessage produce(EsqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        return msg.field("query", context.getQuery());
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.ActionLoggingFields;

public class EqlLogProducer implements ActionLoggerProducer<EqlLogContext> {

    public static final String LOGGER_NAME = "eql.actionlog";

    @Override
    public ESLogMessage produce(EqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        msg.field("query", context.getQuery());
        msg.field("indices", context.getIndices());
        msg.field("hits", context.getHits());
        return msg;
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }
}

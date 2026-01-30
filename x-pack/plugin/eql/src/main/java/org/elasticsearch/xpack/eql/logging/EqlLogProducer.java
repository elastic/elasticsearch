/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.common.logging.action.ActionLogMessage;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.logging.Level;

public class EqlLogProducer implements ActionLoggerProducer<EqlLogContext> {

    public static final String LOGGER_NAME = "eql.actionlog";

    @Override
    public ActionLogMessage produce(Level level, EqlLogContext context, ActionLoggingFields additionalFields) {
        ActionLogMessage msg = produceCommon(level, context, additionalFields);
        msg.put("query", context.getQuery());
        msg.put("indices", context.getIndices());
        msg.put("hits", context.getHits());
        return msg;
    }
}

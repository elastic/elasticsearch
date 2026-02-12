/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Optional;

public class EqlLogProducer implements ActivityLogProducer<EqlLogContext> {

    public static final String LOGGER_NAME = "eql.activitylog";

    @Override
    public Optional<ESLogMessage> produce(EqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        msg.field(ES_FIELDS_PREFIX + "query", context.getQuery());
        msg.field(ES_FIELDS_PREFIX + "indices", context.getIndices());
        msg.field(ES_FIELDS_PREFIX + "hits", context.getHits());
        return Optional.of(msg);
    }

    @Override
    public String loggerName() {
        return LOGGER_NAME;
    }
}

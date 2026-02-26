/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Optional;

public class EsqlLogProducer implements ActivityLogProducer<EsqlLogContext> {

    @Override
    public Optional<ESLogMessage> produce(EsqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        return Optional.of(
            msg.field(QueryLogging.QUERY_FIELD_QUERY, context.getQuery()).field(QueryLogging.QUERY_FIELD_RESULT_COUNT, context.getHits())
        );
    }
}

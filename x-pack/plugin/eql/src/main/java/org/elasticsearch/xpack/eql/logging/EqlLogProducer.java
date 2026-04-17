/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Optional;

public class EqlLogProducer implements ActivityLogProducer<EqlLogContext> {

    @Override
    public Optional<ESLogMessage> produce(EqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, QueryLogging.ES_QUERY_FIELDS_PREFIX, additionalFields);
        msg.field(QueryLogging.QUERY_FIELD_QUERY, context.getQuery());
        msg.field(QueryLogging.QUERY_FIELD_INDICES, context.getIndices());
        msg.field(QueryLogging.QUERY_FIELD_RESULT_COUNT, context.getResultCount());
        var remotes = context.remoteClusterAliases();
        if (remotes.isEmpty() == false) {
            msg.field(QueryLogging.QUERY_FIELD_REMOTE_COUNT, remotes.size());
            msg.field(QueryLogging.QUERY_FIELD_REMOTES, remotes);
            // We do not know statuses & shards for EQL
        }
        return Optional.of(msg);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;

import java.util.Optional;

public class EsqlLogProducer implements ActivityLogProducer<EsqlLogContext> {

    public static final String PROFILE_PREFIX = ES_FIELDS_PREFIX + "esql.profile.";

    @Override
    public Optional<ESLogMessage> produce(EsqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        msg.field(ES_QUERY_FIELDS_PREFIX + "query", context.getQuery());
        context.getQueryProfile().ifPresent(profile -> {
            for (TimeSpanMarker timeSpanMarker : profile.timeSpanMarkers()) {
                TimeValue timeTook = timeSpanMarker.timeTook();
                if (timeTook != null) {
                    String namePrefix = PROFILE_PREFIX + timeSpanMarker.name();
                    msg.field(namePrefix + ".took", timeTook.nanos());
                }
            }
        });
        return Optional.of(msg);
    }
}

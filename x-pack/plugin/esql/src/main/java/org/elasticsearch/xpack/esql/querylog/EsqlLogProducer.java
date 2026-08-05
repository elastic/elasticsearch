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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;

import java.util.Map;
import java.util.Optional;

public class EsqlLogProducer implements ActivityLogProducer<EsqlLogContext> {

    public static final String PROFILE_PREFIX = QueryLogging.ES_QUERY_FIELDS_PREFIX + "esql.profile.";

    @Override
    public Optional<ESLogMessage> produce(EsqlLogContext context, ActionLoggingFields additionalFields) {
        ESLogMessage msg = produceCommon(context, QueryLogging.ES_QUERY_FIELDS_PREFIX, additionalFields);
        msg.field(QueryLogging.QUERY_FIELD_QUERY, context.getQuery())
            .field(QueryLogging.QUERY_FIELD_RESULT_COUNT, context.getResultCount());
        msg.field(QueryLogging.QUERY_FIELD_INDICES, context.getIndices());
        context.getQueryProfile().ifPresent(profile -> {
            for (TimeSpanMarker timeSpanMarker : profile.timeSpanMarkers()) {
                TimeValue timeTook = timeSpanMarker.timeTook();
                if (timeTook != null) {
                    String namePrefix = PROFILE_PREFIX + timeSpanMarker.name();
                    msg.field(namePrefix + ".took", timeTook.nanos());
                }
            }
        });
        context.getFilter().ifPresent(filter -> msg.field(QueryLogging.QUERY_FIELD_FILTER, filter));

        var namedParams = context.namedParams();
        if (namedParams.isEmpty()) {
            var params = context.params();
            if (params.isEmpty() == false) {
                msg.field(QueryLogging.QUERY_FIELD_PARAMS, Map.of(QueryLogging.QUERY_FIELD_PARAM_POSITIONAL, params));
            }
        } else {
            msg.field(QueryLogging.QUERY_FIELD_PARAMS, namedParams);
        }

        // Query-level rollup counters from the response root, surfaced unconditionally so the slow
        // log carries the same I/O / row / CPU cost signal that {@code profile=true} would show
        // under the top-level {@code profile.*} keys. Field names mirror the JSON profile so log
        // and profile readers see the same vocabulary.
        context.getRollupCounters().ifPresent(rollup -> {
            msg.field(PROFILE_PREFIX + "documents_found", rollup.documentsFound());
            msg.field(PROFILE_PREFIX + "values_loaded", rollup.valuesLoaded());
            msg.field(PROFILE_PREFIX + "rows_emitted", rollup.rowsEmitted());
            msg.field(PROFILE_PREFIX + "bytes_read", rollup.bytesRead());
            msg.field(PROFILE_PREFIX + "read_nanos", rollup.readNanos());
            msg.field(PROFILE_PREFIX + "cpu_nanos", rollup.cpuNanos());
        });
        var clusters = context.getClusters();
        var remotes = context.getRemoteClusterAliases(clusters);
        if (remotes.isEmpty() == false) {
            msg.field(QueryLogging.QUERY_FIELD_REMOTES, remotes);
            msg.field(QueryLogging.QUERY_FIELD_REMOTE_COUNT, remotes.size());
            // Count statuses
            msg.field(QueryLogging.QUERY_FIELD_REMOTE_STATUS + "total", clusters.size());
            context.getCountsByStatus(clusters)
                .forEach((status, count) -> msg.field(QueryLogging.QUERY_FIELD_REMOTE_STATUS + status, count));
        }
        return Optional.of(msg);
    }
}

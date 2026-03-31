/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.activity.ActivityLogProducer;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Arrays;
import java.util.Optional;

import static org.elasticsearch.common.logging.activity.QueryLogging.ES_QUERY_FIELDS_PREFIX;

public class SearchLogProducer implements ActivityLogProducer<SearchLogContext> {

    public static final String[] NEVER_MATCH = new String[] { "*", "-*" };
    public static final String QUERY_FIELD_HAS_AGGREGATIONS = ES_QUERY_FIELDS_PREFIX + "has_aggregations";
    public static final String QUERY_FIELD_SEARCH_HITS = ES_QUERY_FIELDS_PREFIX + "search.total_count";
    public static final String QUERY_FIELD_SEARCH_HITS_GTE = ES_QUERY_FIELDS_PREFIX + "search.total_count_partial";

    @Override
    public Optional<ESLogMessage> produce(SearchLogContext context, ActionLoggingFields additionalFields) {
        if (Arrays.equals(NEVER_MATCH, context.getIndexNames())) {
            // Exclude no-match pattern searches, there's not much use in them
            return Optional.empty();
        }
        ESLogMessage msg = produceCommon(context, ES_QUERY_FIELDS_PREFIX, additionalFields);
        msg.field(QueryLogging.QUERY_FIELD_QUERY, context.getQuery());
        msg.field(QueryLogging.QUERY_FIELD_INDICES, context.getIndices());
        msg.field(QueryLogging.QUERY_FIELD_RESULT_COUNT, context.getResultCount());
        if (context.hasAggregations()) {
            msg.field(QUERY_FIELD_HAS_AGGREGATIONS, true);
        }
        var totalHits = context.getTotalHits();
        if (totalHits != null) {
            msg.field(QUERY_FIELD_SEARCH_HITS, totalHits.value());
            if (totalHits.relation() != TotalHits.Relation.EQUAL_TO) {
                msg.field(QUERY_FIELD_SEARCH_HITS_GTE, true);
            }
        }

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
        if (context.isFromRemote()) {
            msg.field(QueryLogging.QUERY_FIELD_IS_REMOTE, true);
        }
        return Optional.of(msg);
    }

}

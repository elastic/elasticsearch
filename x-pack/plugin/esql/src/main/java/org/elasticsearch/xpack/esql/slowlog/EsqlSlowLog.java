/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.slowlog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.esql.session.Result;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING;

public final class EsqlSlowLog {

    private final ClusterSettings clusterSettings;

    public static final String SLOWLOG_PREFIX = "esql.slowlog";

    private static final Logger queryLogger = LogManager.getLogger(SLOWLOG_PREFIX + ".query");

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public EsqlSlowLog(ClusterSettings settings) {
        this.clusterSettings = settings;
    }

    public void onQueryPhase(Result esqlResult, String query) {
        if (esqlResult == null) {
            return; // TODO review, it happens in some tests, not sure if it's a thing also in prod
        }
        long tookInNanos = esqlResult.executionInfo().overallTook().nanos();
        var queryWarnThreshold = clusterSettings.get(ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING).nanos();
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn(Message.of(esqlResult, query));
            return;
        }
        var queryInfoThreshold = clusterSettings.get(ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING).nanos();
        if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info(Message.of(esqlResult, query));
            return;
        }
        var queryDebugThreshold = clusterSettings.get(ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING).nanos();
        if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug(Message.of(esqlResult, query));
            return;
        }
        var queryTraceThreshold = clusterSettings.get(ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING).nanos();
        if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace(Message.of(esqlResult, query));
            return;
        }
    }

    static final class Message {

        private static String escapeJson(String text) {
            byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
            return new String(sourceEscaped, StandardCharsets.UTF_8);
        }

        public static ESLogMessage of(Result esqlResult, String query) {
            Map<String, Object> jsonFields = prepareMap(esqlResult, query);

            return new ESLogMessage().withFields(jsonFields);
        }

        private static Map<String, Object> prepareMap(Result esqlResult, String query) {
            Map<String, Object> messageFields = new HashMap<>();
            messageFields.put("elasticsearch.slowlog.message", esqlResult.executionInfo().clusterAliases());
            messageFields.put("elasticsearch.slowlog.took", esqlResult.executionInfo().overallTook().nanos());
            messageFields.put("elasticsearch.slowlog.took_millis", esqlResult.executionInfo().overallTook().millis());
            messageFields.put("elasticsearch.slowlog.planning.took", esqlResult.executionInfo().planningTookTime().nanos());
            messageFields.put("elasticsearch.slowlog.planning.took_millis", esqlResult.executionInfo().planningTookTime().millis());
            messageFields.put("elasticsearch.slowlog.search_type", "ESQL");
            String source = escapeJson(query);
            messageFields.put("elasticsearch.slowlog.source", source);
            return messageFields;
        }
    }
}

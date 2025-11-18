/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.esql.session.Result;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING;

public final class EsqlQueryLog {

    public static final String ELASTICSEARCH_QUERYLOG_PREFIX = "elasticsearch.querylog";
    public static final String ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE = ELASTICSEARCH_QUERYLOG_PREFIX + ".error.message";
    public static final String ELASTICSEARCH_QUERYLOG_ERROR_TYPE = ELASTICSEARCH_QUERYLOG_PREFIX + ".error.type";
    public static final String ELASTICSEARCH_QUERYLOG_TOOK = ELASTICSEARCH_QUERYLOG_PREFIX + ".took";
    public static final String ELASTICSEARCH_QUERYLOG_TOOK_MILLIS = ELASTICSEARCH_QUERYLOG_PREFIX + ".took_millis";
    public static final String ELASTICSEARCH_QUERYLOG_PLANNING_TOOK = ELASTICSEARCH_QUERYLOG_PREFIX + ".planning.took";
    public static final String ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS = ELASTICSEARCH_QUERYLOG_PREFIX + ".planning.took_millis";
    public static final String ELASTICSEARCH_QUERYLOG_SUCCESS = ELASTICSEARCH_QUERYLOG_PREFIX + ".success";
    public static final String ELASTICSEARCH_QUERYLOG_SEARCH_TYPE = ELASTICSEARCH_QUERYLOG_PREFIX + ".search_type";
    public static final String ELASTICSEARCH_QUERYLOG_QUERY = ELASTICSEARCH_QUERYLOG_PREFIX + ".query";

    public static final String LOGGER_NAME = "esql.querylog";
    private static final Logger queryLogger = LogManager.getLogger(LOGGER_NAME);
    private final SlowLogFields additionalFields;

    private volatile long queryWarnThreshold;
    private volatile long queryInfoThreshold;
    private volatile long queryDebugThreshold;
    private volatile long queryTraceThreshold;

    private volatile boolean includeUser;

    public EsqlQueryLog(ClusterSettings settings, SlowLogFieldProvider slowLogFieldProvider) {
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_WARN_SETTING, this::setQueryWarnThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_INFO_SETTING, this::setQueryInfoThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING, this::setQueryDebugThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING, this::setQueryTraceThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_INCLUDE_USER_SETTING, this::setIncludeUser);

        this.additionalFields = slowLogFieldProvider.create();
    }

    public void onQueryPhase(Result esqlResult, String query) {
        if (esqlResult == null) {
            return; // TODO review, it happens in some tests, not sure if it's a thing also in prod
        }
        long tookInNanos = esqlResult.executionInfo().overallTook().nanos();
        log(() -> Message.of(esqlResult, query, includeUser ? additionalFields.queryFields() : Map.of()), tookInNanos);
    }

    public void onQueryFailure(String query, Exception ex, long tookInNanos) {
        log(() -> Message.of(query, tookInNanos, ex, includeUser ? additionalFields.queryFields() : Map.of()), tookInNanos);
    }

    private void log(Supplier<ESLogMessage> logProducer, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn(logProducer.get());
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info(logProducer.get());
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug(logProducer.get());
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace(logProducer.get());
        }
    }

    public void setQueryWarnThreshold(TimeValue queryWarnThreshold) {
        this.queryWarnThreshold = queryWarnThreshold.nanos();
    }

    public void setQueryInfoThreshold(TimeValue queryInfoThreshold) {
        this.queryInfoThreshold = queryInfoThreshold.nanos();
    }

    public void setQueryDebugThreshold(TimeValue queryDebugThreshold) {
        this.queryDebugThreshold = queryDebugThreshold.nanos();
    }

    public void setQueryTraceThreshold(TimeValue queryTraceThreshold) {
        this.queryTraceThreshold = queryTraceThreshold.nanos();
    }

    public void setIncludeUser(boolean includeUser) {
        this.includeUser = includeUser;
    }

    static final class Message {

        private static String escapeJson(String text) {
            byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
            return new String(sourceEscaped, StandardCharsets.UTF_8);
        }

        public static ESLogMessage of(Result esqlResult, String query, Map<String, String> additionalFields) {
            Map<String, Object> jsonFields = new HashMap<>();
            jsonFields.putAll(additionalFields);
            addGenericFields(jsonFields, query, true);
            addResultFields(jsonFields, esqlResult);
            return new ESLogMessage().withFields(jsonFields);
        }

        public static ESLogMessage of(String query, long took, Exception exception, Map<String, String> additionalFields) {
            Map<String, Object> jsonFields = new HashMap<>();
            jsonFields.putAll(additionalFields);
            addGenericFields(jsonFields, query, false);
            addErrorFields(jsonFields, took, exception);
            return new ESLogMessage().withFields(jsonFields);
        }

        private static void addGenericFields(Map<String, Object> fieldMap, String query, boolean success) {
            String source = escapeJson(query);
            fieldMap.put(ELASTICSEARCH_QUERYLOG_SUCCESS, success);
            fieldMap.put(ELASTICSEARCH_QUERYLOG_SEARCH_TYPE, "ESQL");
            fieldMap.put(ELASTICSEARCH_QUERYLOG_QUERY, source);
        }

        private static void addResultFields(Map<String, Object> fieldMap, Result esqlResult) {
            fieldMap.put(ELASTICSEARCH_QUERYLOG_TOOK, esqlResult.executionInfo().overallTook().nanos());
            fieldMap.put(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS, esqlResult.executionInfo().overallTook().millis());
            fieldMap.put(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK, esqlResult.executionInfo().planningTookTime().nanos());
            fieldMap.put(ELASTICSEARCH_QUERYLOG_PLANNING_TOOK_MILLIS, esqlResult.executionInfo().planningTookTime().millis());
        }

        private static void addErrorFields(Map<String, Object> jsonFields, long took, Exception exception) {
            jsonFields.put(ELASTICSEARCH_QUERYLOG_TOOK, took);
            jsonFields.put(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS, took / 1_000_000);
            jsonFields.put(ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE, exception.getMessage() == null ? "" : exception.getMessage());
            jsonFields.put(ELASTICSEARCH_QUERYLOG_ERROR_TYPE, exception.getClass().getName());
        }
    }
}

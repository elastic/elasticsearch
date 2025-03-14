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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.esql.session.Result;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_INCLUDE_USER_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING;

public final class EsqlSlowLog {

    public static final String ELASTICSEARCH_SLOWLOG_PREFIX = "elasticsearch.slowlog";
    public static final String ELASTICSEARCH_SLOWLOG_ERROR_MESSAGE = ELASTICSEARCH_SLOWLOG_PREFIX + ".error.message";
    public static final String ELASTICSEARCH_SLOWLOG_ERROR_TYPE = ELASTICSEARCH_SLOWLOG_PREFIX + ".error.type";
    public static final String ELASTICSEARCH_SLOWLOG_TOOK = ELASTICSEARCH_SLOWLOG_PREFIX + ".took";
    public static final String ELASTICSEARCH_SLOWLOG_TOOK_MILLIS = ELASTICSEARCH_SLOWLOG_PREFIX + ".took_millis";
    public static final String ELASTICSEARCH_SLOWLOG_PLANNING_TOOK = ELASTICSEARCH_SLOWLOG_PREFIX + ".planning.took";
    public static final String ELASTICSEARCH_SLOWLOG_PLANNING_TOOK_MILLIS = ELASTICSEARCH_SLOWLOG_PREFIX + ".planning.took_millis";
    public static final String ELASTICSEARCH_SLOWLOG_SUCCESS = ELASTICSEARCH_SLOWLOG_PREFIX + ".success";
    public static final String ELASTICSEARCH_SLOWLOG_SEARCH_TYPE = ELASTICSEARCH_SLOWLOG_PREFIX + ".search_type";
    public static final String ELASTICSEARCH_SLOWLOG_QUERY = ELASTICSEARCH_SLOWLOG_PREFIX + ".query";

    public static final String LOGGER_NAME = "esql.slowlog.query";
    private static final Logger queryLogger = LogManager.getLogger(LOGGER_NAME);
    private final List<SlowLogFields> additionalProviders;

    private volatile long queryWarnThreshold;
    private volatile long queryInfoThreshold;
    private volatile long queryDebugThreshold;
    private volatile long queryTraceThreshold;

    private volatile boolean includeUser;

    public EsqlSlowLog(ClusterSettings settings, List<? extends SlowLogFieldProvider> slowLogFieldProviders) {
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_INCLUDE_USER_SETTING, this::setIncludeUser);

        this.additionalProviders = slowLogFieldProviders.stream().map(SlowLogFieldProvider::create).toList();
    }

    public EsqlSlowLog(ClusterSettings settings) {
        this(settings, null);
    }

    public void onQueryPhase(Result esqlResult, String query) {
        if (esqlResult == null) {
            return; // TODO review, it happens in some tests, not sure if it's a thing also in prod
        }
        long tookInNanos = esqlResult.executionInfo().overallTook().nanos();
        log(() -> Message.of(esqlResult, query, additionalProviders), tookInNanos);
    }

    public void onQueryFailure(String query, Exception ex, long tookInNanos) {
        log(() -> Message.of(query, tookInNanos, ex, additionalProviders), tookInNanos);
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

        public static ESLogMessage of(Result esqlResult, String query, List<SlowLogFields> providers) {
            Map<String, Object> jsonFields = new HashMap<>();
            addFromProviders(providers, jsonFields);
            addGenericFields(jsonFields, query, true);
            addResultFields(jsonFields, esqlResult);
            return new ESLogMessage().withFields(jsonFields);
        }

        public static ESLogMessage of(String query, long took, Exception exception, List<SlowLogFields> providers) {
            Map<String, Object> jsonFields = new HashMap<>();
            addFromProviders(providers, jsonFields);
            addGenericFields(jsonFields, query, false);
            addErrorFields(jsonFields, took, exception);
            return new ESLogMessage().withFields(jsonFields);
        }

        private static void addFromProviders(List<SlowLogFields> providers, Map<String, Object> jsonFields) {
            for (SlowLogFields provider : providers) {
                jsonFields.putAll(provider.queryFields());
            }
        }

        private static void addGenericFields(Map<String, Object> fieldMap, String query, boolean success) {
            String source = escapeJson(query);
            fieldMap.put(ELASTICSEARCH_SLOWLOG_SUCCESS, success);
            fieldMap.put(ELASTICSEARCH_SLOWLOG_SEARCH_TYPE, "ESQL");
            fieldMap.put(ELASTICSEARCH_SLOWLOG_QUERY, source);
        }

        private static void addResultFields(Map<String, Object> fieldMap, Result esqlResult) {
            fieldMap.put(ELASTICSEARCH_SLOWLOG_TOOK, esqlResult.executionInfo().overallTook().nanos());
            fieldMap.put(ELASTICSEARCH_SLOWLOG_TOOK_MILLIS, esqlResult.executionInfo().overallTook().millis());
            fieldMap.put(ELASTICSEARCH_SLOWLOG_PLANNING_TOOK, esqlResult.executionInfo().planningTookTime().nanos());
            fieldMap.put(ELASTICSEARCH_SLOWLOG_PLANNING_TOOK_MILLIS, esqlResult.executionInfo().planningTookTime().millis());
        }

        private static void addErrorFields(Map<String, Object> jsonFields, long took, Exception exception) {
            jsonFields.put(ELASTICSEARCH_SLOWLOG_TOOK, took);
            jsonFields.put(ELASTICSEARCH_SLOWLOG_TOOK_MILLIS, took / 1_000_000);
            jsonFields.put(ELASTICSEARCH_SLOWLOG_ERROR_MESSAGE, exception.getMessage() == null ? "" : exception.getMessage());
            jsonFields.put(ELASTICSEARCH_SLOWLOG_ERROR_TYPE, exception.getClass().getName());
        }
    }
}

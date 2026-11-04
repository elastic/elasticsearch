/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsContext;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING;

public final class EsqlQueryLog {

    private static final Logger logger = LogManager.getLogger(EsqlQueryLog.class);

    public static final String ELASTICSEARCH_QUERYLOG_PREFIX = "elasticsearch.querylog";
    public static final String ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE = ELASTICSEARCH_QUERYLOG_PREFIX + ".error.message";
    public static final String ELASTICSEARCH_QUERYLOG_ERROR_TYPE = ELASTICSEARCH_QUERYLOG_PREFIX + ".error.type";
    public static final String ELASTICSEARCH_QUERYLOG_TOOK = ELASTICSEARCH_QUERYLOG_PREFIX + ".took";
    public static final String ELASTICSEARCH_QUERYLOG_TOOK_MILLIS = ELASTICSEARCH_QUERYLOG_PREFIX + ".took_millis";
    public static final String ELASTICSEARCH_QUERYLOG_TOOK_SUFFIX = ".took";
    public static final String ELASTICSEARCH_QUERYLOG_TOOK_MILLIS_SUFFIX = ".took_millis";
    public static final String ELASTICSEARCH_QUERYLOG_SUCCESS = ELASTICSEARCH_QUERYLOG_PREFIX + ".success";
    public static final String ELASTICSEARCH_QUERYLOG_SEARCH_TYPE = ELASTICSEARCH_QUERYLOG_PREFIX + ".search_type";
    public static final String ELASTICSEARCH_QUERYLOG_QUERY = ELASTICSEARCH_QUERYLOG_PREFIX + ".query";
    public static final String ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX = ELASTICSEARCH_QUERYLOG_PREFIX + ".settings";
    public static final String ELASTICSEARCH_QUERYLOG_VIEW_SETTINGS = ELASTICSEARCH_QUERYLOG_PREFIX + ".view_settings";

    public static final String LOGGER_NAME = "esql.querylog";
    private static final Logger queryLogger = LogManager.getLogger(LOGGER_NAME);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(EsqlQueryLog.class);
    public static final String DEPRECATION_MESSAGE = "ES|QL query logging configured via the [esql.querylog.*] settings is deprecated. "
        + "ES|QL queries can still be logged using the core query logging feature by enabling the [elasticsearch.querylog.enabled] "
        + "cluster setting.";
    private final AtomicBoolean deprecatedLogged = new AtomicBoolean(false);
    private final ActionLoggingFields additionalFields;

    private volatile long queryWarnThreshold;
    private volatile long queryInfoThreshold;
    private volatile long queryDebugThreshold;
    private volatile long queryTraceThreshold;

    public EsqlQueryLog(ClusterSettings settings, ActionLoggingFieldsProvider loggingFieldsProvider) {
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_WARN_SETTING, this::setQueryWarnThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_INFO_SETTING, this::setQueryInfoThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING, this::setQueryDebugThreshold);
        settings.initializeAndWatch(ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING, this::setQueryTraceThreshold);

        ActionLoggingFieldsContext logContext = new ActionLoggingFieldsContext();
        settings.initializeAndWatch(ESQL_QUERYLOG_INCLUDE_USER_SETTING, logContext::setIncludeUserInformation);

        this.additionalFields = loggingFieldsProvider.create(logContext);
    }

    public void onQueryPhase(Versioned<Result> esqlResult, String query, Map<String, ViewQuerySettings> viewSettings) {
        if (esqlResult.inner() == null) {
            return; // TODO review, it happens in some tests, not sure if it's a thing also in prod
        }
        long tookInNanos = esqlResult.inner().executionInfo().overallTook().nanos();
        ResolvedSettings settings = esqlResult.inner().configuration().resolvedSettings();
        log(() -> Message.of(esqlResult.inner(), query, settings, viewSettings, additionalFields.logFields()), tookInNanos);
    }

    public void onQueryFailure(
        String query,
        ResolvedSettings settings,
        Map<String, ViewQuerySettings> viewSettings,
        Exception ex,
        long tookInNanos
    ) {
        log(() -> Message.of(query, settings, viewSettings, tookInNanos, ex, additionalFields.logFields()), tookInNanos);
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

    private void deprecatedIfEnabled(long threshold) {
        if (threshold >= 0 && deprecatedLogged.compareAndSet(false, true)) {
            deprecationLogger.warn(DeprecationCategory.SETTINGS, LOGGER_NAME, DEPRECATION_MESSAGE);
        }
    }

    public void setQueryWarnThreshold(TimeValue queryWarnThreshold) {
        this.queryWarnThreshold = queryWarnThreshold.nanos();
        deprecatedIfEnabled(this.queryWarnThreshold);
    }

    public void setQueryInfoThreshold(TimeValue queryInfoThreshold) {
        this.queryInfoThreshold = queryInfoThreshold.nanos();
        deprecatedIfEnabled(this.queryInfoThreshold);
    }

    public void setQueryDebugThreshold(TimeValue queryDebugThreshold) {
        this.queryDebugThreshold = queryDebugThreshold.nanos();
        deprecatedIfEnabled(this.queryDebugThreshold);
    }

    public void setQueryTraceThreshold(TimeValue queryTraceThreshold) {
        this.queryTraceThreshold = queryTraceThreshold.nanos();
        deprecatedIfEnabled(this.queryTraceThreshold);
    }

    static final class Message {

        private static String escapeJson(String text) {
            byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
            return new String(sourceEscaped, StandardCharsets.UTF_8);
        }

        public static ESLogMessage of(
            Result esqlResult,
            String query,
            ResolvedSettings settings,
            Map<String, ViewQuerySettings> viewSettings,
            Map<String, String> additionalFields
        ) {
            Map<String, Object> jsonFields = new HashMap<>();
            jsonFields.putAll(additionalFields);
            addGenericFields(jsonFields, query, true);
            addSettingsFields(jsonFields, settings, viewSettings);
            addResultFields(jsonFields, esqlResult);
            return new ESLogMessage().withFields(jsonFields);
        }

        public static ESLogMessage of(
            String query,
            ResolvedSettings settings,
            Map<String, ViewQuerySettings> viewSettings,
            long took,
            Exception exception,
            Map<String, String> additionalFields
        ) {
            Map<String, Object> jsonFields = new HashMap<>();
            jsonFields.putAll(additionalFields);
            addGenericFields(jsonFields, query, false);
            addSettingsFields(jsonFields, settings, viewSettings);
            addErrorFields(jsonFields, took, exception);
            return new ESLogMessage().withFields(jsonFields);
        }

        private static void addGenericFields(Map<String, Object> fieldMap, String query, boolean success) {
            String source = escapeJson(query);
            fieldMap.put(ELASTICSEARCH_QUERYLOG_SUCCESS, success);
            fieldMap.put(ELASTICSEARCH_QUERYLOG_SEARCH_TYPE, "ESQL");
            fieldMap.put(ELASTICSEARCH_QUERYLOG_QUERY, source);
        }

        private static void addSettingsFields(
            Map<String, Object> fieldMap,
            ResolvedSettings settings,
            Map<String, ViewQuerySettings> viewSettings
        ) {
            if (settings == null) {
                return;
            }
            for (QuerySettingDef<?> def : QuerySettings.all()) {
                Object value = def.get(settings);
                if (value != null) {
                    fieldMap.put(ELASTICSEARCH_QUERYLOG_SETTINGS_PREFIX + "." + def.name(), value.toString());
                }
            }
            var viewValues = ViewQuerySettings.logValues(viewSettings);
            if (viewValues.isEmpty() == false) {
                fieldMap.put(ELASTICSEARCH_QUERYLOG_VIEW_SETTINGS, viewValues);
            }
        }

        private static void addResultFields(Map<String, Object> fieldMap, Result esqlResult) {
            fieldMap.put(ELASTICSEARCH_QUERYLOG_TOOK, esqlResult.executionInfo().overallTook().nanos());
            fieldMap.put(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS, esqlResult.executionInfo().overallTook().millis());
            EsqlQueryProfile esqlQueryProfile = esqlResult.executionInfo().queryProfile();
            for (TimeSpanMarker timeSpanMarker : esqlQueryProfile.timeSpanMarkers()) {
                TimeValue timeTook = timeSpanMarker.timeTook();
                if (timeTook == null) {
                    assert timeSpanMarker.wasStarted() == false
                        : "TimeSpanMarker [" + timeSpanMarker.name() + "] was started but not stopped before query logging";
                    continue;
                }
                String namePrefix = ELASTICSEARCH_QUERYLOG_PREFIX + timeSpanMarker.name();
                fieldMap.put(namePrefix + ELASTICSEARCH_QUERYLOG_TOOK_SUFFIX, timeTook.nanos());
                fieldMap.put(namePrefix + ELASTICSEARCH_QUERYLOG_TOOK_MILLIS_SUFFIX, timeTook.millis());
            }
        }

        private static void addErrorFields(Map<String, Object> jsonFields, long took, Exception exception) {
            jsonFields.put(ELASTICSEARCH_QUERYLOG_TOOK, took);
            jsonFields.put(ELASTICSEARCH_QUERYLOG_TOOK_MILLIS, took / 1_000_000);
            jsonFields.put(ELASTICSEARCH_QUERYLOG_ERROR_MESSAGE, exception.getMessage() == null ? "" : exception.getMessage());
            jsonFields.put(ELASTICSEARCH_QUERYLOG_ERROR_TYPE, exception.getClass().getName());
        }
    }
}

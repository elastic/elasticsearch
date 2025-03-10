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
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.session.Result;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_INCLUDE_USER_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING;

public final class EsqlSlowLog {

    private final ClusterSettings clusterSettings;

    public static final String SLOWLOG_PREFIX = "esql.slowlog";

    private static final Logger queryLogger = LogManager.getLogger(SLOWLOG_PREFIX + ".query");
    private final SecurityContext security;

    private volatile long queryWarnThreshold;
    private volatile long queryInfoThreshold;
    private volatile long queryDebugThreshold;
    private volatile long queryTraceThreshold;

    private volatile boolean includeUser;

    public EsqlSlowLog(ClusterSettings settings, SecurityContext security) {
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);
        settings.initializeAndWatch(ESQL_SLOWLOG_THRESHOLD_INCLUDE_USER_SETTING, this::setIncludeUser);
        this.clusterSettings = settings;

        this.security = security;
    }

    public EsqlSlowLog(ClusterSettings settings) {
        this(settings, null);
    }

    public void onQueryPhase(Result esqlResult, String query) {
        if (esqlResult == null) {
            return; // TODO review, it happens in some tests, not sure if it's a thing also in prod
        }
        long tookInNanos = esqlResult.executionInfo().overallTook().nanos();
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn(Message.of(esqlResult, query, user()));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info(Message.of(esqlResult, query, user()));

        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug(Message.of(esqlResult, query, user()));

        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace(Message.of(esqlResult, query, user()));

        }
    }

    private User user() {
        User user = null;
        if (includeUser && security != null) {
            user = security.getUser();
        }
        return user;
    }

    public void onQueryFailure(String query, Exception ex, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn(Message.of(query, tookInNanos, ex, user()));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info(Message.of(query, tookInNanos, ex, user()));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug(Message.of(query, tookInNanos, ex, user()));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace(Message.of(query, tookInNanos, ex, user()));
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

        public static ESLogMessage of(Result esqlResult, String query, User user) {
            Map<String, Object> jsonFields = prepareMap(esqlResult, query, true, user);

            return new ESLogMessage().withFields(jsonFields);
        }

        public static ESLogMessage of(String query, long took, Exception exception, User user) {
            Map<String, Object> jsonFields = prepareMap(null, query, false, user);
            jsonFields.put("elasticsearch.slowlog.error.message", exception.getMessage() == null ? "" : exception.getMessage());
            jsonFields.put("elasticsearch.slowlog.error.type", exception.getClass().getName());
            jsonFields.put("elasticsearch.slowlog.took", took);
            jsonFields.put("elasticsearch.slowlog.took_millis", took / 1_000_000);
            return new ESLogMessage().withFields(jsonFields);
        }

        private static Map<String, Object> prepareMap(Result esqlResult, String query, boolean success, User user) {
            Map<String, Object> messageFields = new HashMap<>();
            if (user != null) {
                messageFields.put("user.name", user.principal());
            }
            if (esqlResult != null) {
                messageFields.put("elasticsearch.slowlog.took", esqlResult.executionInfo().overallTook().nanos());
                messageFields.put("elasticsearch.slowlog.took_millis", esqlResult.executionInfo().overallTook().millis());
                messageFields.put("elasticsearch.slowlog.planning.took", esqlResult.executionInfo().planningTookTime().nanos());
                messageFields.put("elasticsearch.slowlog.planning.took_millis", esqlResult.executionInfo().planningTookTime().millis());
            }
            String source = escapeJson(query);
            messageFields.put("elasticsearch.slowlog.success", success);
            messageFields.put("elasticsearch.slowlog.search_type", "ESQL");
            messageFields.put("elasticsearch.slowlog.query", source);
            return messageFields;
        }
    }
}

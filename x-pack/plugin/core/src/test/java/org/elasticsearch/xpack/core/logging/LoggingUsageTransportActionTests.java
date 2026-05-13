/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.activity.QueryLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoggingUsageTransportActionTests extends ESTestCase {

    /**
     * Mirrors {@code EsqlPlugin}'s ES|QL querylog settings to avoid dependency on the ES|QL module.
     */
    private static final Setting<Boolean> ESQL_QUERYLOG_INCLUDE_USER = Setting.boolSetting(
        "esql.querylog.include.user",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Set<Setting<?>> ALL_SETTINGS = new HashSet<>(
        Set.of(
            QueryLogger.QUERY_LOGGER_ENABLED,
            QueryLogger.QUERY_LOGGER_THRESHOLD,
            QueryLogger.QUERY_LOGGER_INCLUDE_USER,
            QueryLogger.QUERY_LOGGER_LOG_SYSTEM,
            ESQL_QUERYLOG_INCLUDE_USER
        )
    );

    private static final Map<String, Setting<TimeValue>> ESQL_SETTINGS = new HashMap<>();

    static {
        for (String level : LoggingUsageTransportAction.LOG_LEVELS) {
            Setting<TimeValue> setting = Setting.timeSetting(
                "esql.querylog.threshold." + level,
                TimeValue.timeValueMillis(-1),
                TimeValue.timeValueMillis(-1),
                TimeValue.timeValueMillis(Integer.MAX_VALUE),
                Setting.Property.NodeScope,
                Setting.Property.Dynamic
            );
            ESQL_SETTINGS.put(level, setting);
            ALL_SETTINGS.add(setting);
        }
    }

    private final TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();

    public void testElasticsearchQueryLogBooleanSettings() throws Exception {
        boolean enabled = randomBoolean(), system = randomBoolean(), user = randomBoolean();

        Settings on = Settings.builder()
            .put(QueryLogger.QUERY_LOGGER_ENABLED.getKey(), enabled)
            .put(QueryLogger.QUERY_LOGGER_INCLUDE_USER.getKey(), user)
            .put(QueryLogger.QUERY_LOGGER_LOG_SYSTEM.getKey(), system)
            .build();
        LoggingFeatureSetUsage usageOn = usage(on);
        assertThat(usageOn.queryConfig().base().enabled(), equalTo(enabled));
        assertThat(usageOn.queryConfig().base().userInfo(), equalTo(user));
        assertThat(usageOn.queryConfig().system(), equalTo(system));
    }

    public void testElasticsearchQueryLogThresholdSetting() throws Exception {
        TimeValue threshold = TimeValue.timeValueMillis(randomIntBetween(1, 100));
        Settings settings = Settings.builder().put(QueryLogger.QUERY_LOGGER_THRESHOLD.getKey(), threshold.getStringRep()).build();

        LoggingFeatureSetUsage usage = usage(settings);
        assertThat(usage.queryConfig().threshold(), equalTo(threshold.getStringRep()));
    }

    public void testElasticsearchQueryLogThresholdAbsentWhenNotPositive() throws Exception {
        Settings minusOne = Settings.builder().put(QueryLogger.QUERY_LOGGER_THRESHOLD.getKey(), TimeValue.MINUS_ONE.getStringRep()).build();
        assertThat(usage(minusOne).queryConfig().threshold(), nullValue());

        Settings zero = Settings.builder().put(QueryLogger.QUERY_LOGGER_THRESHOLD.getKey(), TimeValue.ZERO.getStringRep()).build();
        assertThat(usage(zero).queryConfig().threshold(), nullValue());
    }

    public void testEsqlQueryLogThresholdSettings() throws Exception {
        String level = randomFrom(ESQL_SETTINGS.keySet());
        Setting<TimeValue> setting = ESQL_SETTINGS.get(level);
        // 0 is important since for ESQL threshold=0 enables the query log
        long ms = randomBoolean() ? randomIntBetween(1, 100) : 0;
        Settings settings = Settings.builder().put(setting.getKey(), ms + "ms").build();

        LoggingFeatureSetUsage usage = usage(settings);
        assertThat(usage.esqlConfig().base().enabled(), equalTo(true));
        assertThat(usage.esqlConfig().thresholds().get(level), equalTo(ms + "ms"));
    }

    public void testEsqlQueryLogDisabledWhenNoThresholdAtOrAboveZero() throws Exception {
        String level = randomFrom(ESQL_SETTINGS.keySet());
        Setting<TimeValue> setting = ESQL_SETTINGS.get(level);
        Settings settings = Settings.builder().put(setting.getKey(), TimeValue.MINUS_ONE.getStringRep()).build();
        LoggingFeatureSetUsage usage = usage(settings);
        assertThat(usage.esqlConfig().base().enabled(), equalTo(false));
        assertThat(usage.esqlConfig().thresholds().isEmpty(), equalTo(true));
    }

    public void testEsqlQueryLogIncludeUserSetting() throws Exception {
        Settings off = Settings.builder().put(ESQL_QUERYLOG_INCLUDE_USER.getKey(), false).build();
        assertThat(usage(off).esqlConfig().base().userInfo(), equalTo(false));

        Settings on = Settings.builder().put(ESQL_QUERYLOG_INCLUDE_USER.getKey(), true).build();
        assertThat(usage(on).esqlConfig().base().userInfo(), equalTo(true));
    }

    private LoggingFeatureSetUsage usage(Settings settings) throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(settings, ALL_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        LoggingUsageTransportAction action = new LoggingUsageTransportAction(
            transportService,
            clusterService,
            transportService.getThreadPool(),
            mock(ActionFilters.class)
        );

        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        action.localClusterStateOperation(null, null, null, future);
        return (LoggingFeatureSetUsage) future.get().getUsage();
    }
}

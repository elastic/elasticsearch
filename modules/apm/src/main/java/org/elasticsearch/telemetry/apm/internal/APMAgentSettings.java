/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

import java.util.List;

import static org.elasticsearch.common.settings.Setting.Property.DeprecatedWarning;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;

/**
 * This class is responsible for APM settings.
 * The methods could all be static, however they are not in order to make unit testing easier.
 */
public class APMAgentSettings {

    public void addClusterSettingsListeners(ClusterService clusterService, APMTelemetryProvider apmTelemetryProvider) {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final APMTracer apmTracer = apmTelemetryProvider.getTracer();
        final APMMeterService apmMeterService = apmTelemetryProvider.getMeterService();

        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_ENABLED_SETTING, apmTracer::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_METRICS_ENABLED_SETTING, apmMeterService::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_NAMES_INCLUDE_SETTING, apmTracer::setIncludeNames);
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING, apmTracer::setExcludeNames);
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_SANITIZE_FIELD_NAMES, apmTracer::setLabelFilters);
        clusterSettings.addSettingsUpdateConsumer(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH, apmTracer::setMaxTraceDepth);
        clusterSettings.addSettingsUpdateConsumer(
            OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS,
            apmTracer::setRecordExceptionStacks
        );
        clusterSettings.addSettingsUpdateConsumer(
            OtelSdkSettings.TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED,
            apmMeterService.getMeterRegistry()::setInstrumentTimingEnabled
        );
    }

    private static final String TELEMETRY_SETTING_PREFIX = "telemetry.";

    /**
     * Configuration of the removed Elastic APM Java agent. Accepted but ignored, so that a node carrying agent
     * configuration keeps starting. Some keys are still read as defaults for their replacements in
     * {@link OtelSdkSettings}.
     */
    public static final Setting.AffixSetting<String> APM_AGENT_SETTINGS = Setting.prefixKeySetting(
        TELEMETRY_SETTING_PREFIX + "agent.",
        null, // no fallback
        (namespace, qualifiedKey) -> Setting.simpleString(qualifiedKey, NodeScope, OperatorDynamic, DeprecatedWarning)
    );

    public static final Setting<List<String>> TELEMETRY_TRACING_NAMES_INCLUDE_SETTING = Setting.stringListSetting(
        TELEMETRY_SETTING_PREFIX + "tracing.names.include",
        OperatorDynamic,
        NodeScope
    );

    public static final Setting<List<String>> TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING = Setting.stringListSetting(
        TELEMETRY_SETTING_PREFIX + "tracing.names.exclude",
        OperatorDynamic,
        NodeScope
    );

    public static final Setting<List<String>> TELEMETRY_TRACING_SANITIZE_FIELD_NAMES = Setting.stringListSetting(
        TELEMETRY_SETTING_PREFIX + "tracing.sanitize_field_names",
        List.of(
            "password",
            "passwd",
            "pwd",
            "secret",
            "*key",
            "*token*",
            "*session*",
            "*credit*",
            "*card*",
            "*auth*",
            "*principal*",
            "set-cookie"
        ),
        OperatorDynamic,
        NodeScope
    );

    public static final Setting<Boolean> TELEMETRY_TRACING_ENABLED_SETTING = Setting.boolSetting(
        TELEMETRY_SETTING_PREFIX + "tracing.enabled",
        false,
        OperatorDynamic,
        NodeScope
    );

    public static final Setting<Boolean> TELEMETRY_METRICS_ENABLED_SETTING = Setting.boolSetting(
        TELEMETRY_SETTING_PREFIX + "metrics.enabled",
        false,
        OperatorDynamic,
        NodeScope
    );

    public static final Setting<SecureString> TELEMETRY_SECRET_TOKEN_SETTING = SecureSetting.secureString(
        TELEMETRY_SETTING_PREFIX + "secret_token",
        null
    );

    public static final Setting<SecureString> TELEMETRY_API_KEY_SETTING = SecureSetting.secureString(
        TELEMETRY_SETTING_PREFIX + "api_key",
        null
    );
}

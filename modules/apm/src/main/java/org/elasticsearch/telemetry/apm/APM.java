/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.TelemetryPlugin;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.APMMeterService;
import org.elasticsearch.telemetry.apm.internal.APMTelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

import java.util.Collection;
import java.util.List;

/**
 * This module integrates Elastic's APM product with Elasticsearch. Elasticsearch has
 * a {@link org.elasticsearch.telemetry.tracing.Tracer} interface, which this module implements via
 * {@link APMTracer}. We use the OpenTelemetry API to capture "spans", and attach the
 * Elastic APM Java to ship those spans to an APM server. Although it is possible to
 * programmatically attach the agent, the Security Manager permissions required for this
 * make this approach difficult to the point of impossibility.
 * <p>
 * All settings are found under the <code>telemetry.</code> prefix. Any setting under
 * the <code>telemetry.agent.</code> prefix will be forwarded on to the APM Java agent
 * by setting appropriate system properties. Some settings can only be set once, and must be
 * set when the agent starts. We therefore also create and configure a config file in
 * the {@code APMJvmOptions} class, which we then delete when Elasticsearch starts, so that
 * sensitive settings such as <code>secret_token</code> or <code>api_key</code> are not
 * left on disk.
 * <p>
 * When settings are reconfigured using the settings REST API, the new values will again
 * be passed via system properties to the Java agent, which periodically checks for changes
 * and applies the new settings values, provided those settings can be dynamically updated.
 */
public class APM extends Plugin implements NetworkPlugin, TelemetryPlugin {
    private static final Logger logger = LogManager.getLogger(APM.class);
    private final SetOnce<APMTelemetryProvider> telemetryProvider = new SetOnce<>();
    private final Settings settings;

    public APM(Settings settings) {
        this.settings = settings;
    }

    @Override
    public TelemetryProvider getTelemetryProvider(Settings settings) {
        final APMTelemetryProvider apmTelemetryProvider = new APMTelemetryProvider(settings);
        telemetryProvider.set(apmTelemetryProvider);
        return apmTelemetryProvider;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final APMTracer apmTracer = telemetryProvider.get().getTracer();
        final APMMeterService apmMeter = telemetryProvider.get().getMeterService();

        apmTracer.setClusterName(services.clusterService().getClusterName().value());
        apmTracer.setNodeName(services.clusterService().getNodeName());

        final APMAgentSettings apmAgentSettings = new APMAgentSettings();
        apmAgentSettings.initAgentSystemProperties(settings);
        apmAgentSettings.addClusterSettingsListeners(services.clusterService(), telemetryProvider.get());
        logger.info("Sending apm metrics is {}", APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.get(settings) ? "enabled" : "disabled");
        logger.info("Sending apm tracing is {}", APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.get(settings) ? "enabled" : "disabled");

        return List.of(apmTracer, apmMeter);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            // APM general
            APMAgentSettings.APM_AGENT_SETTINGS,
            APMAgentSettings.TELEMETRY_SECRET_TOKEN_SETTING,
            APMAgentSettings.TELEMETRY_API_KEY_SETTING,
            // Metrics
            APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING,
            // Tracing
            APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING,
            APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING,
            APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING,
            APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES
        );
    }
}

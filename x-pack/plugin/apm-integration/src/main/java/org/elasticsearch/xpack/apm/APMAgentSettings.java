/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;

class APMAgentSettings {

    private static final Logger LOGGER = LogManager.getLogger(APMAgentSettings.class);

    /**
     * Sensible defaults that Elasticsearch configures. This cannot be done via the APM agent
     * config file, as then their values cannot be overridden dynamically via system properties.
     */
    // tag::noformat
    static Map<String, String> APM_AGENT_DEFAULT_SETTINGS = Map.of(
        "transaction_sample_rate", "0.2"
    );
    // end::noformat

    void addClusterSettingsListeners(ClusterService clusterService, APMTracer apmTracer) {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(APM_ENABLED_SETTING, enabled -> {
            apmTracer.setEnabled(enabled);
            // The agent records data other than spans, e.g. JVM metrics, so we toggle this setting in order to
            // minimise its impact to a running Elasticsearch.
            this.setAgentSetting("recording", Boolean.toString(enabled));
        });
        clusterSettings.addSettingsUpdateConsumer(APM_TRACING_NAMES_INCLUDE_SETTING, apmTracer::setIncludeNames);
        clusterSettings.addSettingsUpdateConsumer(APM_TRACING_NAMES_EXCLUDE_SETTING, apmTracer::setExcludeNames);
        clusterSettings.addAffixMapUpdateConsumer(APM_AGENT_SETTINGS, map -> map.forEach(this::setAgentSetting), (x, y) -> {});
    }

    void syncAgentSystemProperties(Settings settings) {
        this.setAgentSetting("recording", Boolean.toString(APM_ENABLED_SETTING.get(settings)));

        // Apply default values for some system properties. Although we configure
        // the settings in APM_AGENT_DEFAULT_SETTINGS to defer to the default values, they won't
        // do anything if those settings are never configured.
        APM_AGENT_DEFAULT_SETTINGS.keySet()
            .forEach(
                key -> this.setAgentSetting(
                    key,
                    APM_AGENT_SETTINGS.getConcreteSetting(APM_AGENT_SETTINGS.getKey() + key).get(settings)
                )
            );

        // Then apply values from the settings in the cluster state
        APM_AGENT_SETTINGS.getAsMap(settings).forEach(this::setAgentSetting);
    }

    @SuppressForbidden(reason = "Need to be able to manipulate APM agent-related properties to set them dynamically")
    void setAgentSetting(String key, String value) {
        final String completeKey = "elastic.apm." + Objects.requireNonNull(key);
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            if (value == null || value.isEmpty()) {
                LOGGER.trace("Clearing system property [{}]", completeKey);
                System.clearProperty(completeKey);
            } else {
                LOGGER.trace("Setting setting property [{}] to [{}]", completeKey, value);
                System.setProperty(completeKey, value);
            }
            return null;
        });
    }

    private static final String APM_SETTING_PREFIX = "xpack.apm.";

    private static final List<String> PROHIBITED_AGENT_KEYS = List.of(
        // ES generates a config file and sets this value
        "config_file",
        // ES controls this via `xpack.apm.enabled`
        "recording"
    );

    static final Setting.AffixSetting<String> APM_AGENT_SETTINGS = Setting.prefixKeySetting(
        APM_SETTING_PREFIX + "agent.",
        (qualifiedKey) -> {
            final String[] parts = qualifiedKey.split("\\.");
            final String key = parts[parts.length - 1];
            final String defaultValue = APM_AGENT_DEFAULT_SETTINGS.getOrDefault(key, "");
            return new Setting<>(qualifiedKey, defaultValue, (value) -> {
                if (PROHIBITED_AGENT_KEYS.contains(key)) {
                    throw new IllegalArgumentException("Explicitly configuring [" + qualifiedKey + "] is prohibited");
                }
                return value;
            }, Setting.Property.NodeScope, Setting.Property.OperatorDynamic);
        }
    );

    static final Setting<List<String>> APM_TRACING_NAMES_INCLUDE_SETTING = Setting.listSetting(
        APM_SETTING_PREFIX + "names.include",
        Collections.emptyList(),
        Function.identity(),
        OperatorDynamic,
        NodeScope
    );

    static final Setting<List<String>> APM_TRACING_NAMES_EXCLUDE_SETTING = Setting.listSetting(
        APM_SETTING_PREFIX + "names.exclude",
        Collections.emptyList(),
        Function.identity(),
        OperatorDynamic,
        NodeScope
    );

    static final Setting<Boolean> APM_ENABLED_SETTING = Setting.boolSetting(
        APM_SETTING_PREFIX + "enabled",
        false,
        OperatorDynamic,
        NodeScope
    );

    static final Setting<SecureString> APM_TOKEN_SETTING = SecureSetting.secureString(
        APM_SETTING_PREFIX + "secret_token",
        null
    );
}

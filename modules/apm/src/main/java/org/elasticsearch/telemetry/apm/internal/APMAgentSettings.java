/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;

/**
 * This class is responsible for APM settings, both for Elasticsearch and the APM Java agent.
 * The methods could all be static, however they are not in order to make unit testing easier.
 */
public class APMAgentSettings {

    private static final Logger LOGGER = LogManager.getLogger(APMAgentSettings.class);

    public void addClusterSettingsListeners(ClusterService clusterService, APMTelemetryProvider apmTelemetryProvider) {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final APMTracer apmTracer = apmTelemetryProvider.getTracer();
        final APMMeterService apmMeterService = apmTelemetryProvider.getMeterService();

        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_ENABLED_SETTING, enabled -> {
            apmTracer.setEnabled(enabled);
            // The agent records data other than spans, e.g. JVM metrics, so we toggle this setting in order to
            // minimise its impact to a running Elasticsearch.
            boolean recording = enabled || clusterSettings.get(TELEMETRY_METRICS_ENABLED_SETTING);
            this.setAgentSetting("recording", Boolean.toString(recording));
        });
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_METRICS_ENABLED_SETTING, enabled -> {
            apmMeterService.setEnabled(enabled);
            // The agent records data other than spans, e.g. JVM metrics, so we toggle this setting in order to
            // minimise its impact to a running Elasticsearch.
            boolean recording = enabled || clusterSettings.get(TELEMETRY_TRACING_ENABLED_SETTING);
            this.setAgentSetting("recording", Boolean.toString(recording));
        });
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_NAMES_INCLUDE_SETTING, apmTracer::setIncludeNames);
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING, apmTracer::setExcludeNames);
        clusterSettings.addSettingsUpdateConsumer(TELEMETRY_TRACING_SANITIZE_FIELD_NAMES, apmTracer::setLabelFilters);
        clusterSettings.addAffixMapUpdateConsumer(APM_AGENT_SETTINGS, map -> map.forEach(this::setAgentSetting), (x, y) -> {});
    }

    /**
     * Initialize APM settings from the provided settings object into the corresponding system properties.
     * Later updates to these settings are synchronized using update consumers.
     * @param settings the settings to apply
     */
    public void initAgentSystemProperties(Settings settings) {
        boolean tracing = TELEMETRY_TRACING_ENABLED_SETTING.get(settings);
        boolean metrics = TELEMETRY_METRICS_ENABLED_SETTING.get(settings);

        this.setAgentSetting("recording", Boolean.toString(tracing || metrics));
        // Apply values from the settings in the cluster state
        APM_AGENT_SETTINGS.getAsMap(settings).forEach(this::setAgentSetting);
    }

    /**
     * Copies a setting to the APM agent's system properties under <code>elastic.apm</code>, either
     * by setting the property if {@code value} has a value, or by deleting the property if it doesn't.
     *
     * All permitted agent properties must be covered by the <code>write_system_properties</code> entitlement,
     * see the entitlement policy of this module!
     *
     * @param key the config key to set, without any prefix
     * @param value the value to set, or <code>null</code>
     */
    @SuppressForbidden(reason = "Need to be able to manipulate APM agent-related properties to set them dynamically")
    public void setAgentSetting(String key, String value) {
        if (key.startsWith("global_labels.")) {
            // Invalid agent setting, leftover from flattening global labels in APMJVMOptions
            // https://github.com/elastic/elasticsearch/issues/120791
            return;
        }
        final String completeKey = "elastic.apm." + Objects.requireNonNull(key);
        if (value == null || value.isEmpty()) {
            LOGGER.trace("Clearing system property [{}]", completeKey);
            System.clearProperty(completeKey);
        } else {
            LOGGER.trace("Setting setting property [{}] to [{}]", completeKey, value);
            System.setProperty(completeKey, value);
        }
    }

    private static final String TELEMETRY_SETTING_PREFIX = "telemetry.";

    /**
     * Allow-list of APM agent config keys users are permitted to configure.
     * <p><b>WARNING</b>: Make sure to update the module entitlements if permitting additional agent keys
     * </p>
     * @see <a href="https://www.elastic.co/guide/en/apm/agent/java/current/configuration.html">APM Java Agent Configuration</a>
     */
    public static final Set<String> PERMITTED_AGENT_KEYS = Set.of(
        // Circuit-Breaker:
        "circuit_breaker_enabled",
        "stress_monitoring_interval",
        "stress_monitor_gc_stress_threshold",
        "stress_monitor_gc_relief_threshold",
        "stress_monitor_cpu_duration_threshold",
        "stress_monitor_system_cpu_stress_threshold",
        "stress_monitor_system_cpu_relief_threshold",

        // Core:
        // forbid 'enabled', must remain enabled to dynamically enable tracing / metrics
        // forbid 'recording', controlled by 'telemetry.metrics.enabled' / 'telemetry.tracing.enabled'
        // forbid 'instrument', automatic instrumentation can cause issues
        "service_name",
        "service_node_name",
        // forbid 'service_version', forced by APMJvmOptions
        "hostname",
        "environment",
        "transaction_sample_rate",
        "transaction_max_spans",
        "long_field_max_length",
        "sanitize_field_names",
        "enable_instrumentations",
        "disable_instrumentations",
        // forbid 'enable_experimental_instrumentations', expected to be always enabled by APMJvmOptions
        "unnest_exceptions",
        "ignore_exceptions",
        "capture_body",
        "capture_headers",
        "global_labels",
        "instrument_ancient_bytecode",
        "context_propagation_only",
        "classes_excluded_from_instrumentation",
        "trace_methods",
        "trace_methods_duration_threshold",
        // forbid 'central_config', may impact usage of config_file, disabled in APMJvmOptions
        // forbid 'config_file', configured by APMJvmOptions
        "breakdown_metrics",
        "plugins_dir",
        "use_elastic_traceparent_header",
        "disable_outgoing_tracecontext_headers",
        "span_min_duration",
        "cloud_provider",
        "enable_public_api_annotation_inheritance",
        "transaction_name_groups",
        "trace_continuation_strategy",
        "baggage_to_attach",

        // Datastore: irrelevant, not whitelisted

        // HTTP:
        "capture_body_content_types",
        "transaction_ignore_urls",
        "transaction_ignore_user_agents",
        "use_path_as_transaction_name",
        // forbid deprecated url_groups

        // Huge Traces:
        "span_compression_enabled",
        "span_compression_exact_match_max_duration",
        "span_compression_same_kind_max_duration",
        "exit_span_min_duration",

        // JAX-RS: irrelevant, not whitelisted

        // JMX:
        "capture_jmx_metrics",

        // Logging:
        "log_level", // allow overriding the default in APMJvmOptions
        // forbid log_file, always set by APMJvmOptions
        "log_ecs_reformatting",
        "log_ecs_reformatting_additional_fields",
        "log_ecs_formatter_allow_list",
        // forbid log_ecs_reformatting_dir, always use logsDir provided in APMJvmOptions
        "log_file_size",
        // forbid log_format_sout, always use file logging
        // forbid log_format_file, expected to be JSON in APMJvmOptions
        "log_sending",

        // Messaging: irrelevant, not whitelisted

        // Metrics:
        "dedot_custom_metrics",
        "custom_metrics_histogram_boundaries",
        "metric_set_limit",
        "agent_reporter_health_metrics",
        "agent_background_overhead_metrics",

        // Profiling:
        "profiling_inferred_spans_enabled",
        "profiling_inferred_spans_logging_enabled",
        "profiling_inferred_spans_sampling_interval",
        "profiling_inferred_spans_min_duration",
        "profiling_inferred_spans_included_classes",
        "profiling_inferred_spans_excluded_classes",
        "profiling_inferred_spans_lib_directory",

        // Reporter:
        // forbid secret_token: use telemetry.secret_token instead
        // forbid api_key: use telemetry.api_key instead
        "server_url",
        "server_urls",
        "disable_send",
        "server_timeout",
        "verify_server_cert",
        "max_queue_size",
        "include_process_args",
        "api_request_time",
        "api_request_size",
        "metrics_interval",
        "disable_metrics",

        // Serverless:
        "aws_lambda_handler",
        "data_flush_timeout",

        // Stacktraces:
        "application_packages",
        "stack_trace_limit",
        "span_stack_trace_min_duration"
    );

    private static Setting<String> concreteAgentSetting(String namespace, String qualifiedKey, Setting.Property... properties) {
        return new Setting<>(qualifiedKey, "", (value) -> {
            if (qualifiedKey.equals("_na_") == false && PERMITTED_AGENT_KEYS.contains(namespace) == false) {
                if (namespace.startsWith("global_labels.")) {
                    // Invalid agent setting, leftover from flattening global labels in APMJVMOptions
                    // https://github.com/elastic/elasticsearch/issues/120791
                    return value;
                }
                throw new IllegalArgumentException("Configuration [" + qualifiedKey + "] is either prohibited or unknown.");
            }
            return value;
        }, properties);
    }

    public static final Setting.AffixSetting<String> APM_AGENT_SETTINGS = Setting.prefixKeySetting(
        TELEMETRY_SETTING_PREFIX + "agent.",
        null, // no fallback
        (namespace, qualifiedKey) -> concreteAgentSetting(namespace, qualifiedKey, NodeScope, OperatorDynamic)
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

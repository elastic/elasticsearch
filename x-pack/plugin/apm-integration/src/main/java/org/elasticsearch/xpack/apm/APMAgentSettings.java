/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.settings.Setting;
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
        "transaction_sample_rate", "0.5"
    );
    // end::noformat

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

    /**
     * Lists all known APM agent configuration keys.
     */
    private static final List<String> AGENT_KEYS = List.of(
        // Circuit-Breaker configuration options
        "circuit_breaker_enabled",
        "stress_monitoring_interval",
        "stress_monitor_gc_stress_threshold",
        "stress_monitor_gc_relief_threshold",
        "stress_monitor_cpu_duration_threshold",
        "stress_monitor_system_cpu_stress_threshold",
        "stress_monitor_system_cpu_relief_threshold",

        // Core configuration options
        "recording",
        "enabled",
        "instrument",
        "service_name",
        "service_node_name",
        "service_version",
        "hostname",
        "environment",
        "transaction_sample_rate",
        "transaction_max_spans",
        "sanitize_field_names",
        "enable_instrumentations",
        "disable_instrumentations",
        "enable_experimental_instrumentations",
        "unnest_exceptions",
        "ignore_exceptions",
        "capture_body",
        "capture_headers",
        "global_labels",
        "classes_excluded_from_instrumentation",
        "trace_methods",
        "trace_methods_duration_threshold",
        "central_config",
        "breakdown_metrics",
        "config_file",
        "plugins_dir",
        "use_elastic_traceparent_header",
        "span_min_duration",
        "cloud_provider",
        "enable_public_api_annotation_inheritance",

        // HTTP configuration options
        "capture_body_content_types",
        "transaction_ignore_urls",
        "transaction_ignore_user_agents",
        "use_path_as_transaction_name",
        "url_groups",

        // Huge Traces configuration options
        "span_compression_enabled",
        "span_compression_exact_match_max_duration",
        "span_compression_same_kind_max_duration",
        "exit_span_min_duration",

        // JAX-RS configuration options
        "enable_jaxrs_annotation_inheritance",
        "use_jaxrs_path_as_transaction_name",

        // JMX configuration options
        "capture_jmx_metrics",

        // Logging configuration options
        "log_level",
        "log_file",
        "log_ecs_reformatting",
        "log_ecs_reformatting_additional_fields",
        "log_ecs_formatter_allow_list",
        "log_ecs_reformatting_dir",
        "log_file_size",
        "log_format_sout",
        "log_format_file",

        // Messaging configuration options
        "ignore_message_queues",

        // Metrics configuration options
        "dedot_custom_metrics",

        // Profiling configuration options
        "profiling_inferred_spans_enabled",
        "profiling_inferred_spans_sampling_interval",
        "profiling_inferred_spans_min_duration",
        "profiling_inferred_spans_included_classes",
        "profiling_inferred_spans_excluded_classes",
        "profiling_inferred_spans_lib_directory",

        // Reporter configuration options
        "secret_token",
        "api_key",
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

        // Serverless configuration options
        "aws_lambda_handler",
        "data_flush_timeout",

        // Stacktrace configuration options
        "application_packages",
        "stack_trace_limit",
        "span_stack_trace_min_duration"
    );

    /**
     * Lists all APM configuration keys that are not dynamic and must be configured via the config file.
     */
    private static final List<String> STATIC_AGENT_KEYS = List.of(
        "enabled",
        "service_name",
        "service_node_name",
        "service_version",
        "hostname",
        "environment",
        "global_labels",
        "trace_methods_duration_threshold",
        "breakdown_metrics",
        "plugins_dir",
        "cloud_provider",
        "stress_monitoring_interval",
        "log_ecs_reformatting_additional_fields",
        "log_ecs_formatter_allow_list",
        "log_ecs_reformatting_dir",
        "log_file_size",
        "log_format_sout",
        "log_format_file",
        "profiling_inferred_spans_lib_directory",
        "secret_token",
        "api_key",
        "verify_server_cert",
        "max_queue_size",
        "include_process_args",
        "metrics_interval",
        "disable_metrics",
        "data_flush_timeout"
    );

    /**
     * Lists APM agent configuration keys that cannot be configured via the cluster settings REST API.
     * This may be because the setting's value must not be changed at runtime, or because it relates
     * to a feature that is not required for tracing with Elasticsearch, but which configuring could
     * impact performance.
     */
    private static final List<String> PROHIBITED_AGENT_KEYS = List.of(
        // ES doesn't use dynamic instrumentation
        "instrument",
        "enable_instrumentations",
        "disable_instrumentations",
        "classes_excluded_from_instrumentation",
        "enable_public_api_annotation_inheritance",

        // We don't use JAX-RS
        "enable_jaxrs_annotation_inheritance",
        "use_jaxrs_path_as_transaction_name",

        // Must be enabled to use OpenTelemetry
        "enable_experimental_instrumentations",

        // For now, we don't use central config
        "central_config",

        // Config file path can't be changed
        "config_file",

        // The use case for capturing traces but not sending them doesn't apply to ES
        "disable_send",

        // We don't run ES in an AWS Lambda
        "aws_lambda_handler"
    );

    static final String APM_SETTING_PREFIX = "xpack.apm.tracing.";

    static final Setting.AffixSetting<String> APM_AGENT_SETTINGS = Setting.prefixKeySetting(
        APM_SETTING_PREFIX + "agent.",
        (qualifiedKey) -> {
            final String[] parts = qualifiedKey.split("\\.");
            final String key = parts[parts.length - 1];
            final String defaultValue = APM_AGENT_DEFAULT_SETTINGS.getOrDefault(key, "");
            return new Setting<>(qualifiedKey, defaultValue, (value) -> {
                // The `Setting` constructor asserts that a setting's parser doesn't return null when called with the default
                // value. This makes less sense for prefix settings, but is particularly problematic here since we validate
                // the setting name and reject unknown keys. Thus, if assertions are enabled, we have to tolerate the "_na_" key,
                // which comes from `Setting#prefixKeySetting()`.
                if (Assertions.ENABLED && qualifiedKey.equals("_na_")) {
                    return value;
                }
                if (AGENT_KEYS.contains(key) == false) {
                    throw new IllegalArgumentException("Unknown APM configuration key: [" + qualifiedKey + "]");
                }
                if (STATIC_AGENT_KEYS.contains(key)) {
                    throw new IllegalArgumentException(
                        "Cannot set ["
                            + qualifiedKey
                            + "] as it is not a dynamic setting - configure it via [config/elasticapm.properties] instead"
                    );
                }
                if (PROHIBITED_AGENT_KEYS.contains(key)) {
                    throw new IllegalArgumentException("Configuring [" + qualifiedKey + "] is prohibited with Elasticsearch");
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

    static final Setting<Boolean> APM_ENABLED_SETTING = Setting.boolSetting(
        APM_SETTING_PREFIX + "enabled",
        false,
        OperatorDynamic,
        NodeScope
    );
}

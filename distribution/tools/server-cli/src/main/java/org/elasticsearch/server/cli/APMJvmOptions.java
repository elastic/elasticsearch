/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class APMJvmOptions {
    private static final Map<String, String> STATIC_CONFIG;
    private static final Map<String, String> CONFIG_DEFAULTS;

    /**
     * Lists all APM configuration keys that are not dynamic and must be configured via the config file.
     */
    private static final List<String> STATIC_AGENT_KEYS = List.of(
        "api_key",
        "aws_lambda_handler",
        "breakdown_metrics",
        "classes_excluded_from_instrumentation",
        "cloud_provider",
        "data_flush_timeout",
        "disable_metrics",
        "disable_send",
        "enabled",
        "enable_public_api_annotation_inheritance",
        "environment",
        "global_labels",
        "hostname",
        "include_process_args",
        "log_ecs_formatter_allow_list",
        "log_ecs_reformatting_additional_fields",
        "log_ecs_reformatting_dir",
        "log_file",
        "log_file_size",
        "log_format_file",
        "log_format_sout",
        "max_queue_size",
        "metrics_interval",
        "plugins_dir",
        "profiling_inferred_spans_lib_directory",
        "secret_token",
        "service_name",
        "service_node_name",
        "service_version",
        "stress_monitoring_interval",
        "trace_methods_duration_threshold",
        "use_jaxrs_path_as_transaction_name",
        "verify_server_cert"
    );

    static {
        STATIC_CONFIG = new HashMap<>();

        // Required for OpenTelemetry support
        STATIC_CONFIG.put("enable_experimental_instrumentations", "true");

        // Identifies the version of Elasticsearch in the captured trace data.
        STATIC_CONFIG.put("service_version", Version.CURRENT.toString());

        // Configures a log file to write to. `_AGENT_HOME_` is a placeholder used
        // by the agent. Don't disable writing to a log file, as the agent will then
        // require extra Security Manager permissions when it tries to do something
        // else, and it's just painful.
        STATIC_CONFIG.put("log_file", "_AGENT_HOME_/../../logs/apm.log");

        // ES does not use auto-instrumentation.
        STATIC_CONFIG.put("instrument", "false");

        CONFIG_DEFAULTS = new HashMap<>();

        // This is used to keep all the errors and transactions of a service
        // together and is the primary filter in the Elastic APM user interface.
        //
        // You can optionally also set `service_node_name`, which is used to
        // distinguish between different nodes of a service, therefore it should
        // be unique for each JVM within a service. If not set, data
        // aggregations will be done based on a container ID (where valid) or on
        // the reported hostname (automatically discovered or manually
        // configured through hostname). However, if this node's `node.name` is
        // set, then that value is used for the `service_node_name`.
        CONFIG_DEFAULTS.put("service_name", "elasticsearch");

        // An arbitrary string that identifies this deployment environment. For
        // example, "dev", "staging" or "prod". Can be anything you like, but must
        // have the same value across different systems in the same deployment
        // environment.
        CONFIG_DEFAULTS.put("environment", "dev");

        // Logging configuration. Unless you need detailed logs about what the APM
        // is doing, leave this value alone.
        CONFIG_DEFAULTS.put("log_level", "error");
        CONFIG_DEFAULTS.put("application_packages", "org.elasticsearch,org.apache.lucene");
        CONFIG_DEFAULTS.put("metrics_interval", "120s");
        CONFIG_DEFAULTS.put("breakdown_metrics", "false");
        CONFIG_DEFAULTS.put("central_config", "false");
    }

    public static List<String> apmJvmOptions(Settings settings, KeyStoreWrapper keystore, Path tmpdir) throws UserException, IOException {
        final boolean enabled = settings.getAsBoolean("xpack.apm.enabled", false);

        if (enabled == false) {
            return List.of();
        }

        final Optional<Path> agentJar = findAgentJar();

        if (agentJar.isEmpty()) {
            return List.of();
        }

        final Map<String, String> propertiesMap = extractApmSettings(settings);

        if (propertiesMap.containsKey("service_node_name") == false) {
            final String nodeName = settings.get("node.name");
            if (nodeName != null) {
                propertiesMap.put("service_node_name", nodeName);
            }
        }

        if (keystore != null && keystore.getSettingNames().contains("xpack.apm.secret_token")) {
            try (SecureString token = keystore.getString("xpack.apm.secret_token")) {
                propertiesMap.put("secret_token", token.toString());
            }
        }

        final Map<String, String> dynamicSettings = extractDynamicSettings(propertiesMap);

        final File tempFile = writeApmProperties(tmpdir, propertiesMap);

        final List<String> options = new ArrayList<>();

        // Use an agent argument to specify the config file instead of e.g. `-Delastic.apm.config_file=...`
        // because then the agent won't try to reload the file, and we can remove it after startup.
        options.add("-javaagent:" + agentJar.get() + "=c=" + tempFile);

        dynamicSettings.forEach((key, value) -> options.add("-Delastic.apm." + key + "=" + value));

        return options;
    }

    private static Map<String, String> extractDynamicSettings(Map<String, String> propertiesMap) {
        final Map<String, String> cliOptionsMap = new HashMap<>();

        final Iterator<Map.Entry<String, String>> propertiesIterator = propertiesMap.entrySet().iterator();
        while (propertiesIterator.hasNext()) {
            final Map.Entry<String, String> entry = propertiesIterator.next();
            if (STATIC_AGENT_KEYS.contains(entry.getKey()) == false) {
                propertiesIterator.remove();
                cliOptionsMap.put(entry.getKey(), entry.getValue());
            }
        }

        return cliOptionsMap;
    }

    private static Map<String, String> extractApmSettings(Settings settings) throws UserException {
        final Map<String, String> propertiesMap = new HashMap<>();

        final Settings agentSettings = settings.getByPrefix("xpack.apm.agent.");
        agentSettings.keySet().forEach(key -> propertiesMap.put(key, String.valueOf(agentSettings.get(key))));

        // These settings must not be changed
        for (String key : STATIC_CONFIG.keySet()) {
            if (propertiesMap.containsKey(key)) {
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Do not set a value for [xpack.apm.agent." + key + "], as this is configured automatically by Elasticsearch"
                );
            }
        }

        CONFIG_DEFAULTS.forEach(propertiesMap::putIfAbsent);

        propertiesMap.putAll(STATIC_CONFIG);
        return propertiesMap;
    }

    private static File writeApmProperties(Path tmpdir, Map<String, String> propertiesMap) throws IOException {
        final Properties p = new Properties();
        p.putAll(propertiesMap);

        File tempFile = File.createTempFile(".elstcapm.", ".tmp", tmpdir.toFile());
        try (OutputStream os = new FileOutputStream(tempFile)) {
            p.store(os, " Automatically generated by Elasticsearch, do not edit!");
        }
        return tempFile;
    }

    private static Optional<Path> findAgentJar() throws IOException {
        final Path apmModule = Path.of("modules/x-pack-apm-integration");

        if (Files.isDirectory(apmModule) == false) {
            return Optional.empty();
        }

        try (var apmStream = Files.list(apmModule)) {
            return apmStream.filter(path -> path.getFileName().toString().matches("elastic-apm-agent-\\d+\\.\\d+\\.\\d+\\.jar"))
                .findFirst();
        }
    }
}

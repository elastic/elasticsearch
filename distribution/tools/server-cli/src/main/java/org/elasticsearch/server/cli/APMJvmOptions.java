/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * This class is responsible for working out if APM tracing is configured and if so, preparing
 * a temporary config file for the APM Java agent and CLI options to the JVM to configure APM.
 * APM doesn't need to be enabled, as that can be toggled at runtime, but some configuration e.g.
 * server URL and secret key can only be provided when Elasticsearch starts.
 */
class APMJvmOptions {
    /**
     * Contains agent configuration that must always be applied, and cannot be overridden.
     */
    // tag::noformat
    private static final Map<String, String> STATIC_CONFIG = Map.of(
        // Identifies the version of Elasticsearch in the captured trace data.
        "service_version", Version.CURRENT.toString(),

        // Configures a log file to write to. `_AGENT_HOME_` is a placeholder used
        // by the agent. Don't disable writing to a log file, as the agent will then
        // require extra Security Manager permissions when it tries to do something
        // else, and it's just painful.
        "log_file", "_AGENT_HOME_/../../logs/apm.log",

        // ES does not use auto-instrumentation.
        "instrument", "false"
        );

    /**
     * Contains default configuration that will be used unless overridden by explicit configuration.
     */
    private static final Map<String, String> CONFIG_DEFAULTS = Map.of(
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
        "service_name", "elasticsearch",

        // An arbitrary string that identifies this deployment environment. For
        // example, "dev", "staging" or "prod". Can be anything you like, but must
        // have the same value across different systems in the same deployment
        // environment.
        "environment", "dev",

        // Logging configuration. Unless you need detailed logs about what the APM
        // is doing, leave this value alone.
        "log_level", "error",
        "application_packages", "org.elasticsearch,org.apache.lucene",
        "metrics_interval", "120s",
        "breakdown_metrics", "false",
        "central_config", "false"
        );
    // end::noformat

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

    /**
     * This method works out if APM tracing is enabled, and if so, prepares a temporary config file
     * for the APM Java agent and CLI options to the JVM to configure APM. The config file is temporary
     * because it will be deleted once Elasticsearch starts.
     *
     * @param settings the Elasticsearch settings to consider
     * @param keystore a wrapper to access the keystore, or null if there is no keystore
     * @param tmpdir Elasticsearch's temporary directory, where the config file will be written
     */
    static List<String> apmJvmOptions(Settings settings, @Nullable KeyStoreWrapper keystore, Path tmpdir) throws UserException,
        IOException {
        final Path agentJar = findAgentJar();

        if (agentJar == null) {
            return List.of();
        }

        final Map<String, String> propertiesMap = extractApmSettings(settings);

        // No point doing anything if we don't have a destination for the trace data, and it can't be configured dynamically
        if (propertiesMap.containsKey("server_url") == false && propertiesMap.containsKey("server_urls") == false) {
            return List.of();
        }

        if (propertiesMap.containsKey("service_node_name") == false) {
            final String nodeName = settings.get("node.name");
            if (nodeName != null) {
                propertiesMap.put("service_node_name", nodeName);
            }
        }

        if (keystore != null) {
            extractSecureSettings(keystore, propertiesMap);
        }
        final Map<String, String> dynamicSettings = extractDynamicSettings(propertiesMap);

        final Path tmpProperties = writeApmProperties(tmpdir, propertiesMap);

        final List<String> options = new ArrayList<>();
        // Use an agent argument to specify the config file instead of e.g. `-Delastic.apm.config_file=...`
        // because then the agent won't try to reload the file, and we can remove it after startup.
        options.add("-javaagent:" + agentJar + "=c=" + tmpProperties);

        dynamicSettings.forEach((key, value) -> options.add("-Delastic.apm." + key + "=" + value));

        return options;
    }

    private static void extractSecureSettings(KeyStoreWrapper keystore, Map<String, String> propertiesMap) {
        final Set<String> settingNames = keystore.getSettingNames();
        for (String key : List.of("api_key", "secret_token")) {
            if (settingNames.contains("tracing.apm." + key)) {
                try (SecureString token = keystore.getString("tracing.apm." + key)) {
                    propertiesMap.put(key, token.toString());
                }
            }
        }
    }

    /**
     * Removes settings that can be changed dynamically at runtime from the supplied map, and returns
     * those settings in a new map.
     */
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

        final Settings agentSettings = settings.getByPrefix("tracing.apm.agent.");
        agentSettings.keySet().forEach(key -> propertiesMap.put(key, String.valueOf(agentSettings.get(key))));

        // These settings must not be changed
        for (String key : STATIC_CONFIG.keySet()) {
            if (propertiesMap.containsKey(key)) {
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Do not set a value for [tracing.apm.agent." + key + "], as this is configured automatically by Elasticsearch"
                );
            }
        }

        CONFIG_DEFAULTS.forEach(propertiesMap::putIfAbsent);

        propertiesMap.putAll(STATIC_CONFIG);
        return propertiesMap;
    }

    /**
     * Writes a Java properties file with data from supplied map to a temporary config, and returns
     * the file that was created.
     *
     * @param tmpdir        the directory for the file
     * @param propertiesMap the data to write
     * @return the file that was created
     * @throws IOException if writing the file fails
     */
    private static Path writeApmProperties(Path tmpdir, Map<String, String> propertiesMap) throws IOException {
        final Properties p = new Properties();
        p.putAll(propertiesMap);

        final Path tmpFile = Files.createTempFile(tmpdir, ".elstcapm.", ".tmp");
        try (OutputStream os = Files.newOutputStream(tmpFile)) {
            p.store(os, " Automatically generated by Elasticsearch, do not edit!");
        }
        return tmpFile;
    }

    /**
     * The JVM argument that configure the APM agent needs to specify the agent jar path, so this method
     * finds the jar by inspecting the filesystem.
     * @return the agent jar file
     * @throws IOException if a problem occurs reading the filesystem
     */
    @Nullable
    private static Path findAgentJar() throws IOException, UserException {
        final Path apmModule = Path.of(System.getProperty("user.dir")).resolve("modules/apm");

        if (Files.notExists(apmModule)) {
            if (Build.CURRENT.isProductionRelease()) {
                throw new UserException(
                    ExitCodes.CODE_ERROR,
                    "Expected to find [apm] module in [" + apmModule + "]! Installation is corrupt"
                );
            }
            return null;
        }

        try (var apmStream = Files.list(apmModule)) {
            final List<Path> paths = apmStream.filter(
                path -> path.getFileName().toString().matches("elastic-apm-agent-\\d+\\.\\d+\\.\\d+\\.jar")
            ).toList();

            if (paths.size() > 1) {
                throw new UserException(
                    ExitCodes.CODE_ERROR,
                    "Found multiple [elastic-apm-agent] jars under [" + apmModule + "]! Installation is corrupt."
                );
            }

            if (paths.isEmpty()) {
                throw new UserException(
                    ExitCodes.CODE_ERROR,
                    "Found no [elastic-apm-agent] jar under [" + apmModule + "]! Installation is corrupt."
                );
            }

            return paths.get(0);
        }
    }
}

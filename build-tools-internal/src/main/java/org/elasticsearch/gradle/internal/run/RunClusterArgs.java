/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Renders the command line for the {@code :test:test-clusters-cli} cluster runner from a {@link RunTask}'s configuration.
 * <p>
 * This also applies the {@link RunTask} convenience options ({@code --https}, {@code --transport-tls}, telemetry defaults), which
 * are expressed to the runner as plain settings and config files.
 */
public final class RunClusterArgs {

    static final String TLS_CERTIFICATE_AUTHORITY = "public-ca.pem";
    static final String HTTPS_CERTIFICATE = "private-cert1.p12";
    static final String TRANSPORT_CERTIFICATE = "private-cert2.p12";
    static final String LICENSE_SETTING = "xpack.license.self_generated.type";
    static final List<String> LICENSE_TYPES = List.of("basic", "trial");

    private String clusterName;
    private String distribution;
    private String license;
    private int numberOfNodes = 1;
    private File workingDir;
    private boolean preserveData;
    private File dataDir;
    private int httpPort;
    private int transportPort;
    private Integer remoteClusterPort;
    private final Map<String, String> settings = new LinkedHashMap<>();
    private final List<String> nodeSettings = new ArrayList<>();
    private final Map<String, String> systemProperties = new LinkedHashMap<>();
    private final List<String> jvmArgs = new ArrayList<>();
    private final Map<String, String> keystoreSettings = new LinkedHashMap<>();
    private final Map<String, File> keystoreFiles = new LinkedHashMap<>();
    private String keystorePassword;
    private final List<RunUser> users = new ArrayList<>();
    private final List<String> plugins = new ArrayList<>();
    private final List<String> modules = new ArrayList<>();
    private final Map<String, File> configFiles = new LinkedHashMap<>();
    private boolean https;
    private boolean transportTls;
    private File tlsResourcesDir;
    private boolean telemetryEnabled;
    private boolean debugServer;
    private int debugServerPort;
    private boolean debugCli;
    private int debugCliPort;
    private File pidFile;

    public RunClusterArgs clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public RunClusterArgs distribution(String distribution) {
        this.distribution = distribution;
        return this;
    }

    /**
     * The self-generated license type, {@code basic} or {@code trial}. Only supported by the default distribution.
     */
    public RunClusterArgs license(String license) {
        this.license = license;
        return this;
    }

    public RunClusterArgs numberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        return this;
    }

    public RunClusterArgs workingDir(File workingDir) {
        this.workingDir = workingDir;
        return this;
    }

    public RunClusterArgs preserveData(boolean preserveData) {
        this.preserveData = preserveData;
        return this;
    }

    public RunClusterArgs dataDir(File dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public RunClusterArgs ports(int httpPort, int transportPort, Integer remoteClusterPort) {
        this.httpPort = httpPort;
        this.transportPort = transportPort;
        this.remoteClusterPort = remoteClusterPort;
        return this;
    }

    public RunClusterArgs settings(Map<String, String> settings) {
        this.settings.putAll(settings);
        return this;
    }

    /**
     * Per-node settings in the runner's {@code <index>:<key>=<value>} format.
     */
    public RunClusterArgs nodeSettings(List<String> nodeSettings) {
        this.nodeSettings.addAll(nodeSettings);
        return this;
    }

    public RunClusterArgs systemProperties(Map<String, String> systemProperties) {
        this.systemProperties.putAll(systemProperties);
        return this;
    }

    public RunClusterArgs jvmArgs(List<String> jvmArgs) {
        this.jvmArgs.addAll(jvmArgs);
        return this;
    }

    public RunClusterArgs keystore(Map<String, String> keystoreSettings, Map<String, File> keystoreFiles, String keystorePassword) {
        this.keystoreSettings.putAll(keystoreSettings);
        this.keystoreFiles.putAll(keystoreFiles);
        this.keystorePassword = keystorePassword;
        return this;
    }

    public RunClusterArgs users(List<RunUser> users) {
        this.users.addAll(users);
        return this;
    }

    public RunClusterArgs plugins(List<String> plugins) {
        this.plugins.addAll(plugins);
        return this;
    }

    public RunClusterArgs modules(List<String> modules) {
        this.modules.addAll(modules);
        return this;
    }

    public RunClusterArgs configFiles(Map<String, File> configFiles) {
        this.configFiles.putAll(configFiles);
        return this;
    }

    public RunClusterArgs tls(boolean https, boolean transportTls, File tlsResourcesDir) {
        this.https = https;
        this.transportTls = transportTls;
        this.tlsResourcesDir = tlsResourcesDir;
        return this;
    }

    /**
     * Whether telemetry export is configured by the caller (e.g. for the mock APM server). When it isn't, metrics and tracing
     * are disabled for the default distribution unless explicitly enabled through settings, since they are enabled by default
     * in serverless.
     */
    public RunClusterArgs telemetryEnabled(boolean telemetryEnabled) {
        this.telemetryEnabled = telemetryEnabled;
        return this;
    }

    public RunClusterArgs debugServer(boolean enabled, int startingPort) {
        this.debugServer = enabled;
        this.debugServerPort = startingPort;
        return this;
    }

    public RunClusterArgs debugCli(boolean enabled, int startingPort) {
        this.debugCli = enabled;
        this.debugCliPort = startingPort;
        return this;
    }

    public RunClusterArgs pidFile(File pidFile) {
        this.pidFile = pidFile;
        return this;
    }

    /**
     * Builds the runner arguments.
     *
     * @throws IllegalArgumentException if a convenience option conflicts with explicitly configured settings
     */
    public List<String> build() {
        Map<String, String> finalSettings = new LinkedHashMap<>(settings);
        Map<String, File> finalConfigFiles = new LinkedHashMap<>(configFiles);

        if (license != null) {
            if (LICENSE_TYPES.contains(license) == false) {
                throw new IllegalArgumentException("Unsupported license type [" + license + "], expected one of " + LICENSE_TYPES);
            }
            if ("DEFAULT".equalsIgnoreCase(distribution) == false) {
                throw new IllegalArgumentException("License type [" + license + "] requires the default distribution");
            }
            finalSettings.put(LICENSE_SETTING, license);
        }
        if (https) {
            validateHelperOption("--https", "xpack.security.http.ssl", finalSettings);
            finalSettings.put("xpack.security.http.ssl.enabled", "true");
            finalSettings.put("xpack.security.http.ssl.keystore.path", "https.keystore");
            finalSettings.put("xpack.security.http.ssl.certificate_authorities", "https.ca");
            finalConfigFiles.put("https.keystore", new File(tlsResourcesDir, HTTPS_CERTIFICATE));
            finalConfigFiles.put("https.ca", new File(tlsResourcesDir, TLS_CERTIFICATE_AUTHORITY));
        }
        if (transportTls) {
            validateHelperOption("--transport-tls", "xpack.security.transport.ssl", finalSettings);
            finalSettings.put("xpack.security.transport.ssl.enabled", "true");
            finalSettings.put("xpack.security.transport.ssl.client_authentication", "required");
            finalSettings.put("xpack.security.transport.ssl.keystore.path", "transport.keystore");
            finalSettings.put("xpack.security.transport.ssl.certificate_authorities", "transport.ca");
            finalConfigFiles.put("transport.keystore", new File(tlsResourcesDir, TRANSPORT_CERTIFICATE));
            finalConfigFiles.put("transport.ca", new File(tlsResourcesDir, TLS_CERTIFICATE_AUTHORITY));
        }
        // Without an explicit value, the default distribution auto-configures security with TLS on the HTTP layer
        if ("DEFAULT".equalsIgnoreCase(distribution)) {
            finalSettings.putIfAbsent("xpack.security.enabled", "true");
        }
        // Telemetry settings are provided by the APM module, which only ships with the default distribution
        if (telemetryEnabled == false && "DEFAULT".equalsIgnoreCase(distribution)) {
            finalSettings.putIfAbsent("telemetry.metrics.enabled", "false");
            finalSettings.putIfAbsent("telemetry.tracing.enabled", "false");
        }

        List<String> args = new ArrayList<>();
        add(args, "--name", clusterName);
        add(args, "--distribution", distribution);
        add(args, "--nodes", String.valueOf(numberOfNodes));
        if (workingDir != null) {
            add(args, "--working-dir", workingDir.getAbsolutePath());
        }
        if (preserveData) {
            args.add("--preserve-data");
        }
        if (dataDir != null) {
            add(args, "--data-dir", dataDir.getAbsolutePath());
        }
        add(args, "--http-port", String.valueOf(httpPort));
        add(args, "--transport-port", String.valueOf(transportPort));
        if (remoteClusterPort != null) {
            add(args, "--remote-cluster-port", String.valueOf(remoteClusterPort));
        }
        finalSettings.forEach((k, v) -> add(args, "--setting", k + "=" + v));
        nodeSettings.forEach(s -> add(args, "--node-setting", s));
        systemProperties.forEach((k, v) -> add(args, "--system-property", k + "=" + v));
        jvmArgs.forEach(arg -> add(args, "--jvm-arg", arg));
        keystoreSettings.forEach((k, v) -> add(args, "--keystore", k + "=" + v));
        keystoreFiles.forEach((k, f) -> add(args, "--keystore-file", k + "=" + f.getAbsolutePath()));
        if (keystorePassword != null && keystorePassword.isEmpty() == false) {
            add(args, "--keystore-password", keystorePassword);
        }
        users.forEach(u -> add(args, "--user", u.username() + ":" + u.password() + (u.role() == null ? "" : ":" + u.role())));
        plugins.forEach(p -> add(args, "--plugin", p));
        modules.forEach(m -> add(args, "--module", m));
        finalConfigFiles.forEach((name, f) -> add(args, "--config-file", name + "=" + f.getAbsolutePath()));
        if (debugServer) {
            args.add("--debug-server-jvm");
            add(args, "--debug-server-port", String.valueOf(debugServerPort));
        }
        if (debugCli) {
            args.add("--debug-cli-jvm");
            add(args, "--debug-cli-port", String.valueOf(debugCliPort));
        }
        if (pidFile != null) {
            add(args, "--pid-file", pidFile.getAbsolutePath());
        }
        return args;
    }

    /**
     * Parses repeatable {@code <key>=<value>} option values, splitting on the first {@code =}. Later values win.
     */
    public static Map<String, String> parseKeyValues(String option, List<String> values) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String value : values) {
            int separator = value.indexOf('=');
            if (separator < 1) {
                throw new IllegalArgumentException("Invalid value for --" + option + " [" + value + "], expected <key>=<value>");
            }
            result.put(value.substring(0, separator), value.substring(separator + 1));
        }
        return result;
    }

    /**
     * Disallow overlap between helper options and explicit configuration.
     */
    private static void validateHelperOption(String option, String prefix, Map<String, String> settings) {
        Set<String> preConfigured = new TreeSet<>();
        settings.keySet().stream().filter(key -> key.startsWith(prefix)).forEach(preConfigured::add);
        if (preConfigured.isEmpty() == false) {
            throw new IllegalArgumentException("Can not use " + option + " with " + String.join(",", preConfigured));
        }
    }

    private static void add(List<String> args, String option, String value) {
        args.add(option);
        args.add(value);
    }
}

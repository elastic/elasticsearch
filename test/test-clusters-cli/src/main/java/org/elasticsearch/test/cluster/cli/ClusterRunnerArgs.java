/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.cli;

import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Command line arguments accepted by {@link ClusterRunner}. Each invocation describes a single cluster.
 * <p>
 * All {@code --*-port} options are <em>starting</em> ports: node {@code i} of a multi-node cluster uses {@code port + i}.
 */
public final class ClusterRunnerArgs {

    public static final String DEFAULT_NAME = "runTask";
    public static final int DEFAULT_HTTP_PORT = 9200;
    public static final int DEFAULT_TRANSPORT_PORT = 9300;
    public static final int DEFAULT_DEBUG_SERVER_PORT = 5007;
    public static final int DEFAULT_DEBUG_CLI_PORT = 5107;

    static final String USAGE = """
        Usage: ClusterRunner [options]

        Starts a local Elasticsearch cluster and blocks until the process is terminated.

        Cluster:
          --name <name>                     Cluster name; nodes are named <name>-0..N (default: runTask)
          --distribution <type>             DEFAULT or INTEG_TEST (default: DEFAULT)
          --nodes <count>                   Number of nodes (default: 1)
          --working-dir <path>              Base directory for node working directories (default: a new temp directory)
          --preserve-data                   Keep each node's data directory between runs
          --data-dir <path>                 Custom data path; <path>/<node name> when running more than one node

        Ports (all ports are starting ports, node i uses <port> + i):
          --http-port <port>                Starting HTTP port (default: 9200)
          --transport-port <port>           Starting transport port (default: 9300)
          --remote-cluster-port <port>      Starting remote cluster server port (default: not set)

        Configuration (repeatable):
          --setting <key>=<value>           Setting written to elasticsearch.yml of every node
          --node-setting <i>:<key>=<value>  Setting written to elasticsearch.yml of node <i> only (0-based)
          --system-property <key>=<value>   JVM system property for every node
          --jvm-arg <arg>                   JVM argument for every node
          --keystore <key>=<value>          Secure string setting added to the keystore
          --keystore-file <key>=<path>      Secure file setting added to the keystore
          --keystore-password <password>    Password protecting the keystore
          --user <name>:<password>[:<role>] File realm user (default role: _es_test_root)
          --plugin <name>                   Plugin to install, located via the tests.cluster.plugins.path system property
          --module <name>                   Module to install, located via the tests.cluster.modules.path system property
          --config-file <name>=<path>       Extra file copied into each node's config directory

        Debugging (debug ports are starting ports, node i uses <port> + i):
          --debug-server-jvm                Connect each server JVM to a listening debugger (default port: 5007)
          --debug-server-port <port>        Starting port for --debug-server-jvm
          --debug-cli-jvm                   Connect each CLI launcher JVM to a listening debugger (default port: 5107)
          --debug-cli-port <port>           Starting port for --debug-cli-jvm

        Other:
          --pid-file <path>                 Write the runner and node process ids to this file once the cluster is started
          --help                            Print this message
        """;

    private String name = DEFAULT_NAME;
    private DistributionType distribution = DistributionType.DEFAULT;
    private int nodes = 1;
    private Path workingDir;
    private boolean preserveData;
    private Path dataDir;
    private int httpPort = DEFAULT_HTTP_PORT;
    private int transportPort = DEFAULT_TRANSPORT_PORT;
    private Integer remoteClusterPort;
    private final Map<String, String> settings = new LinkedHashMap<>();
    private final Map<Integer, Map<String, String>> nodeSettings = new LinkedHashMap<>();
    private final Map<String, String> systemProperties = new LinkedHashMap<>();
    private final List<String> jvmArgs = new ArrayList<>();
    private final Map<String, String> keystoreSettings = new LinkedHashMap<>();
    private final Map<String, Path> keystoreFiles = new LinkedHashMap<>();
    private String keystorePassword;
    private final List<User> users = new ArrayList<>();
    private final List<String> plugins = new ArrayList<>();
    private final List<String> modules = new ArrayList<>();
    private final Map<String, Path> configFiles = new LinkedHashMap<>();
    private boolean debugServer;
    private int debugServerPort = DEFAULT_DEBUG_SERVER_PORT;
    private boolean debugCli;
    private int debugCliPort = DEFAULT_DEBUG_CLI_PORT;
    private Path pidFile;
    private boolean help;

    private ClusterRunnerArgs() {}

    /**
     * Parses the given command line arguments.
     *
     * @throws IllegalArgumentException if the arguments are malformed or an option is unknown
     */
    public static ClusterRunnerArgs parse(String... args) {
        ClusterRunnerArgs result = new ClusterRunnerArgs();
        Cursor cursor = new Cursor(args);
        while (cursor.hasNext()) {
            String option = cursor.next();
            switch (option) {
                case "--name" -> result.name = cursor.value(option);
                case "--distribution" -> result.distribution = parseDistribution(cursor.value(option));
                case "--nodes" -> result.nodes = parsePositiveInt(option, cursor.value(option));
                case "--working-dir" -> result.workingDir = Path.of(cursor.value(option));
                case "--preserve-data" -> result.preserveData = true;
                case "--data-dir" -> result.dataDir = Path.of(cursor.value(option));
                case "--http-port" -> result.httpPort = parsePort(option, cursor.value(option));
                case "--transport-port" -> result.transportPort = parsePort(option, cursor.value(option));
                case "--remote-cluster-port" -> result.remoteClusterPort = parsePort(option, cursor.value(option));
                case "--setting" -> putKeyValue(result.settings, option, cursor.value(option));
                case "--node-setting" -> putNodeSetting(result.nodeSettings, cursor.value(option));
                case "--system-property" -> putKeyValue(result.systemProperties, option, cursor.value(option));
                case "--jvm-arg" -> result.jvmArgs.add(cursor.value(option));
                case "--keystore" -> putKeyValue(result.keystoreSettings, option, cursor.value(option));
                case "--keystore-file" -> {
                    String[] entry = splitKeyValue(option, cursor.value(option));
                    result.keystoreFiles.put(entry[0], Path.of(entry[1]));
                }
                case "--keystore-password" -> result.keystorePassword = cursor.value(option);
                case "--user" -> result.users.add(parseUser(cursor.value(option)));
                case "--plugin" -> result.plugins.add(cursor.value(option));
                case "--module" -> result.modules.add(cursor.value(option));
                case "--config-file" -> {
                    String[] entry = splitKeyValue(option, cursor.value(option));
                    result.configFiles.put(entry[0], Path.of(entry[1]));
                }
                case "--debug-server-jvm" -> result.debugServer = true;
                case "--debug-server-port" -> result.debugServerPort = parsePort(option, cursor.value(option));
                case "--debug-cli-jvm" -> result.debugCli = true;
                case "--debug-cli-port" -> result.debugCliPort = parsePort(option, cursor.value(option));
                case "--pid-file" -> result.pidFile = Path.of(cursor.value(option));
                case "--help", "-h" -> result.help = true;
                default -> throw new IllegalArgumentException("Unknown option [" + option + "]");
            }
        }
        // validated once all options are parsed since --nodes may come after --node-setting
        for (Integer index : result.nodeSettings.keySet()) {
            if (index >= result.nodes) {
                throw new IllegalArgumentException(
                    "Invalid node index [" + index + "] for --node-setting, the cluster has " + result.nodes + " node(s)"
                );
            }
        }
        return result;
    }

    private static DistributionType parseDistribution(String value) {
        try {
            return DistributionType.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value for --distribution [" + value + "], expected DEFAULT or INTEG_TEST");
        }
    }

    private static int parsePositiveInt(String option, String value) {
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for " + option + " [" + value + "], expected a number");
        }
        if (parsed < 1) {
            throw new IllegalArgumentException("Invalid value for " + option + " [" + value + "], must be at least 1");
        }
        return parsed;
    }

    private static int parsePort(String option, String value) {
        int port = parsePositiveInt(option, value);
        if (port > 65535) {
            throw new IllegalArgumentException("Invalid value for " + option + " [" + value + "], must be a valid port");
        }
        return port;
    }

    private static void putNodeSetting(Map<Integer, Map<String, String>> target, String value) {
        int separator = value.indexOf(':');
        if (separator < 1) {
            throw new IllegalArgumentException("Invalid value for --node-setting [" + value + "], expected <index>:<key>=<value>");
        }
        int index;
        try {
            index = Integer.parseInt(value.substring(0, separator));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for --node-setting [" + value + "], expected <index>:<key>=<value>");
        }
        if (index < 0) {
            throw new IllegalArgumentException("Invalid value for --node-setting [" + value + "], node index must not be negative");
        }
        putKeyValue(target.computeIfAbsent(index, i -> new LinkedHashMap<>()), "--node-setting", value.substring(separator + 1));
    }

    private static void putKeyValue(Map<String, String> target, String option, String value) {
        String[] entry = splitKeyValue(option, value);
        target.put(entry[0], entry[1]);
    }

    /**
     * Splits {@code key=value} on the first {@code =}, so values may themselves contain {@code =}.
     */
    private static String[] splitKeyValue(String option, String value) {
        int separator = value.indexOf('=');
        if (separator < 1) {
            throw new IllegalArgumentException("Invalid value for " + option + " [" + value + "], expected <key>=<value>");
        }
        return new String[] { value.substring(0, separator), value.substring(separator + 1) };
    }

    private static User parseUser(String value) {
        String[] parts = value.split(":", -1);
        if (parts.length < 2 || parts.length > 3 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException("Invalid value for --user [" + value + "], expected <name>:<password>[:<role>]");
        }
        String role = parts.length == 3 && parts[2].isEmpty() == false ? parts[2] : User.ROOT_USER_ROLE;
        return new User(parts[0], parts[1], role, true);
    }

    public static String usage() {
        return USAGE;
    }

    public String name() {
        return name;
    }

    public DistributionType distribution() {
        return distribution;
    }

    public int nodes() {
        return nodes;
    }

    public Path workingDir() {
        return workingDir;
    }

    public boolean preserveData() {
        return preserveData;
    }

    public Path dataDir() {
        return dataDir;
    }

    public int httpPort() {
        return httpPort;
    }

    public int transportPort() {
        return transportPort;
    }

    public Integer remoteClusterPort() {
        return remoteClusterPort;
    }

    public Map<String, String> settings() {
        return Collections.unmodifiableMap(settings);
    }

    /**
     * Settings for individual nodes, keyed by node index.
     */
    public Map<Integer, Map<String, String>> nodeSettings() {
        return Collections.unmodifiableMap(nodeSettings);
    }

    public Map<String, String> systemProperties() {
        return Collections.unmodifiableMap(systemProperties);
    }

    public List<String> jvmArgs() {
        return Collections.unmodifiableList(jvmArgs);
    }

    public Map<String, String> keystoreSettings() {
        return Collections.unmodifiableMap(keystoreSettings);
    }

    public Map<String, Path> keystoreFiles() {
        return Collections.unmodifiableMap(keystoreFiles);
    }

    public String keystorePassword() {
        return keystorePassword;
    }

    public List<User> users() {
        return Collections.unmodifiableList(users);
    }

    public List<String> plugins() {
        return Collections.unmodifiableList(plugins);
    }

    public List<String> modules() {
        return Collections.unmodifiableList(modules);
    }

    public Map<String, Path> configFiles() {
        return Collections.unmodifiableMap(configFiles);
    }

    public boolean debugServer() {
        return debugServer;
    }

    public int debugServerPort() {
        return debugServerPort;
    }

    public boolean debugCli() {
        return debugCli;
    }

    public int debugCliPort() {
        return debugCliPort;
    }

    public Path pidFile() {
        return pidFile;
    }

    public boolean help() {
        return help;
    }

    /**
     * Name of the node at the given index, matching the default naming of the test-clusters library.
     */
    public String nodeName(int index) {
        return name + "-" + index;
    }

    private static final class Cursor {
        private final String[] args;
        private int position = 0;

        Cursor(String[] args) {
            this.args = args;
        }

        boolean hasNext() {
            return position < args.length;
        }

        String next() {
            return args[position++];
        }

        String value(String option) {
            if (hasNext() == false) {
                throw new IllegalArgumentException("Missing value for option [" + option + "]");
            }
            return next();
        }
    }
}

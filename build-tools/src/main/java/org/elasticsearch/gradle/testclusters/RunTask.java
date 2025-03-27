/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.testclusters;

import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class RunTask extends DefaultTestClustersTask {

    public static final String CUSTOM_SETTINGS_PREFIX = "tests.es.";
    private static final Logger logger = Logging.getLogger(RunTask.class);
    private static final String tlsCertificateAuthority = "public-ca.pem";
    private static final String httpsCertificate = "private-cert1.p12";
    private static final String transportCertificate = "private-cert2.p12";

    private Boolean debug = false;
    private Boolean cliDebug = false;

    private Boolean apmServerEnabled = false;

    private List<String> plugins = List.of();

    private Boolean preserveData = false;

    private Path dataDir = null;

    private String keystorePassword = "";

    private Boolean useHttps = false;

    private Boolean useTransportTls = false;

    private final Path tlsBasePath = Path.of(
        new File(getProject().getRootDir(), "build-tools-internal/src/main/resources/run.ssl").toURI()
    );
    private MockApmServer mockServer;

    @Option(option = "debug-jvm", description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch.")
    public void setDebug(boolean enabled) {
        this.debug = enabled;
    }

    @Option(option = "debug-cli-jvm", description = "Enable debugging configuration, to allow attaching a debugger to the cli launcher.")
    public void setCliDebug(boolean enabled) {
        this.cliDebug = enabled;
    }

    @Option(
        option = "entitlements",
        description = "Use the Entitlements agent system in place of SecurityManager to enforce sandbox policies."
    )
    public void setEntitlementsEnabled(boolean enabled) {}

    @Input
    public Boolean getDebug() {
        return debug;
    }

    @Input
    public Boolean getCliDebug() {
        return cliDebug;
    }

    @Input
    public Boolean getEntitlementsEnabled() {
        return true;
    }

    @Input
    public Boolean getApmServerEnabled() {
        return apmServerEnabled;
    }

    @Option(option = "with-apm-server", description = "Run simple logging http server to accept apm requests")
    public void setApmServerEnabled(Boolean apmServerEnabled) {
        this.apmServerEnabled = apmServerEnabled;
    }

    @Option(option = "with-plugins", description = "Run distribution with plugins installed")
    public void setPlugins(String plugins) {
        this.plugins = Arrays.asList(plugins.split(","));
        for (var cluster : getClusters()) {
            for (String plugin : this.plugins) {
                cluster.plugin(":plugins:" + plugin);
            }
            dependsOn(cluster.getPluginAndModuleConfigurations());
        }
    }

    @Input
    public List<String> getPlugins() {
        return plugins;
    }

    @Option(option = "data-dir", description = "Override the base data directory used by the testcluster")
    public void setDataDir(String dataDirStr) {
        dataDir = Paths.get(dataDirStr).toAbsolutePath();
    }

    @Input
    public Boolean getPreserveData() {
        return preserveData;
    }

    @Option(option = "preserve-data", description = "Preserves data directory contents (path provided to --data-dir is always preserved)")
    public void setPreserveData(Boolean preserveData) {
        this.preserveData = preserveData;
    }

    @Option(option = "keystore-password", description = "Set the elasticsearch keystore password")
    public void setKeystorePassword(String password) {
        keystorePassword = password;
    }

    @Input
    @Optional
    public String getKeystorePassword() {
        return keystorePassword;
    }

    @Input
    @Optional
    public String getDataDir() {
        if (dataDir == null) {
            return null;
        }
        return dataDir.toString();
    }

    @Option(option = "https", description = "Helper option to enable HTTPS")
    public void setUseHttps(boolean useHttps) {
        this.useHttps = useHttps;
    }

    @Input
    @Optional
    public Boolean getUseHttps() {
        return useHttps;
    }

    @Option(option = "transport-tls", description = "Helper option to enable TLS on transport port")
    public void setUseTransportTls(boolean useTransportTls) {
        this.useTransportTls = useTransportTls;
    }

    @Input
    @Optional
    public Boolean getUseTransportTls() {
        return useTransportTls;
    }

    @Override
    public void beforeStart() {
        int httpPort = 9200;
        int transportPort = 9300;
        Map<String, String> additionalSettings = System.getProperties()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().toString().startsWith(CUSTOM_SETTINGS_PREFIX))
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().toString().substring(CUSTOM_SETTINGS_PREFIX.length()),
                    entry -> entry.getValue().toString()
                )
            );
        boolean singleNode = getClusters().stream().mapToLong(c -> c.getNodes().size()).sum() == 1;
        final Function<ElasticsearchNode, Path> getDataPath;
        if (singleNode) {
            getDataPath = n -> dataDir;
        } else {
            getDataPath = n -> dataDir.resolve(n.getName());
        }

        for (ElasticsearchCluster cluster : getClusters()) {
            cluster.setPreserveDataDir(preserveData);
            for (ElasticsearchNode node : cluster.getNodes()) {
                node.setHttpPort(String.valueOf(httpPort++));
                node.setTransportPort(String.valueOf(transportPort++));
                additionalSettings.forEach(node::setting);
                if (dataDir != null) {
                    node.setDataPath(getDataPath.apply(node));
                }
                if (keystorePassword.length() > 0) {
                    node.keystorePassword(keystorePassword);
                }
                if (useHttps) {
                    validateHelperOption("--https", "xpack.security.http.ssl", node);
                    node.setting("xpack.security.http.ssl.enabled", "true");
                    node.extraConfigFile("https.keystore", tlsBasePath.resolve(httpsCertificate).toFile());
                    node.extraConfigFile("https.ca", tlsBasePath.resolve(tlsCertificateAuthority).toFile());
                    node.setting("xpack.security.http.ssl.keystore.path", "https.keystore");
                    node.setting("xpack.security.http.ssl.certificate_authorities", "https.ca");
                }
                if (useTransportTls) {
                    node.setting("xpack.security.transport.ssl.enabled", "true");
                    node.setting("xpack.security.transport.ssl.client_authentication", "required");
                    node.extraConfigFile("transport.keystore", tlsBasePath.resolve(transportCertificate).toFile());
                    node.extraConfigFile("transport.ca", tlsBasePath.resolve(tlsCertificateAuthority).toFile());
                    node.setting("xpack.security.transport.ssl.keystore.path", "transport.keystore");
                    node.setting("xpack.security.transport.ssl.certificate_authorities", "transport.ca");
                }

                if (apmServerEnabled) {
                    mockServer = new MockApmServer(9999);
                    try {
                        mockServer.start();
                        node.setting("telemetry.metrics.enabled", "true");
                        node.setting("telemetry.tracing.enabled", "true");
                        node.setting("telemetry.agent.transaction_sample_rate", "0.10");
                        node.setting("telemetry.agent.metrics_interval", "10s");
                        node.setting("telemetry.agent.server_url", "http://127.0.0.1:" + mockServer.getPort());
                    } catch (IOException e) {
                        logger.warn("Unable to start APM server", e);
                    }
                }
                // in serverless metrics are enabled by default
                // if metrics were not enabled explicitly for gradlew run we should disable them
                else if (node.getSettingKeys().contains("telemetry.metrics.enabled") == false) { // metrics
                    node.setting("telemetry.metrics.enabled", "false");
                } else if (node.getSettingKeys().contains("telemetry.tracing.enabled") == false) { // tracing
                    node.setting("telemetry.tracing.enabled", "false");
                }

            }
        }
        if (debug) {
            enableDebug();
        }
        if (cliDebug) {
            enableCliDebug();
        }
        enableEntitlements();
    }

    @TaskAction
    public void runAndWait() throws IOException {
        List<BufferedReader> toRead = new ArrayList<>();
        List<BooleanSupplier> aliveChecks = new ArrayList<>();

        if (getClusters().isEmpty()) {
            throw new GradleException("Task " + getPath() + " is not configured to use any clusters. Be sure to call useCluster().");
        }

        try {
            for (ElasticsearchCluster cluster : getClusters()) {
                cluster.writeUnicastHostsFiles();
                for (ElasticsearchNode node : cluster.getNodes()) {
                    BufferedReader reader = Files.newBufferedReader(node.getEsOutputFile());
                    toRead.add(reader);
                    aliveChecks.add(node::isProcessAlive);
                }
            }

            while (Thread.currentThread().isInterrupted() == false) {
                boolean readData = false;
                for (BufferedReader bufferedReader : toRead) {
                    if (bufferedReader.ready()) {
                        readData = true;
                        logger.lifecycle(bufferedReader.readLine());
                    }
                }

                if (aliveChecks.stream().allMatch(BooleanSupplier::getAsBoolean) == false) {
                    throw new GradleException("Elasticsearch cluster died");
                }

                if (readData == false) {
                    // no data was ready to be consumed and rather than continuously spinning, pause
                    // for some time to avoid excessive CPU usage. Ideally we would use the JDK
                    // WatchService to receive change notifications but the WatchService does not have
                    // a native MacOS implementation and instead relies upon polling with possible
                    // delays up to 10s before a notification is received. See JDK-7133447.
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } finally {
            Exception thrown = null;
            for (Closeable closeable : toRead) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (thrown == null) {
                        thrown = e;
                    } else {
                        thrown.addSuppressed(e);
                    }
                }
            }

            if (thrown != null) {
                logger.debug("exception occurred during close of stdout file readers", thrown);
            }

            if (apmServerEnabled && mockServer != null) {
                mockServer.stop();
            }
        }
    }

    /**
     * Disallow overlap between helper options and explicit configuration
     */
    private void validateHelperOption(String option, String prefix, ElasticsearchNode node) {
        Set<String> preConfigured = findConfiguredSettingsByPrefix(prefix, node);
        if (preConfigured.isEmpty() == false) {
            throw new IllegalArgumentException("Can not use " + option + " with " + String.join(",", preConfigured));
        }
    }

    /**
     * Find any settings configured with a given prefix
     */
    private Set<String> findConfiguredSettingsByPrefix(String prefix, ElasticsearchNode node) {
        Set<String> preConfigured = new HashSet<>();
        node.getSettingKeys().stream().filter(key -> key.startsWith(prefix)).forEach(k -> preConfigured.add(prefix));
        return preConfigured;
    }
}

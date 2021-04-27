/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RunTask extends DefaultTestClustersTask {

    private static final Logger logger = Logging.getLogger(RunTask.class);
    public static final String CUSTOM_SETTINGS_PREFIX = "tests.es.";

    private Boolean debug = false;

    private Boolean preserveData = false;

    private Path dataDir = null;

    private String keystorePassword = "";

    @Option(option = "debug-jvm", description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch.")
    public void setDebug(boolean enabled) {
        this.debug = enabled;
    }

    @Input
    public Boolean getDebug() {
        return debug;
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

    @Override
    public void beforeStart() {
        int debugPort = 5005;
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
        boolean singleNode = getClusters().stream().flatMap(c -> c.getNodes().stream()).count() == 1;
        final Function<ElasticsearchNode, Path> getDataPath;
        if (singleNode) {
            getDataPath = n -> dataDir;
        } else {
            getDataPath = n -> dataDir.resolve(n.getName());
        }

        for (ElasticsearchCluster cluster : getClusters()) {
            cluster.getFirstNode().setHttpPort(String.valueOf(httpPort));
            httpPort++;
            cluster.getFirstNode().setTransportPort(String.valueOf(transportPort));
            transportPort++;
            cluster.setPreserveDataDir(preserveData);
            for (ElasticsearchNode node : cluster.getNodes()) {
                additionalSettings.forEach(node::setting);
                if (dataDir != null) {
                    node.setDataPath(getDataPath.apply(node));
                }
                if (debug) {
                    logger.lifecycle("Running elasticsearch in debug mode, {} expecting running debug server on port {}", node, debugPort);
                    node.jvmArgs("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + debugPort);
                    debugPort += 1;
                }
                if (keystorePassword.length() > 0) {
                    node.keystorePassword(keystorePassword);
                }
            }
        }
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
                for (ElasticsearchNode node : cluster.getNodes()) {
                    BufferedReader reader = Files.newBufferedReader(node.getEsLogFile());
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
        }
    }
}

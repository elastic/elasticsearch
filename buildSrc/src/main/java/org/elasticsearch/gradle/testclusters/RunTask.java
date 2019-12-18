package org.elasticsearch.gradle.testclusters;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
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
import java.util.stream.Collectors;

public class RunTask extends DefaultTestClustersTask {

    private static final Logger logger = Logging.getLogger(RunTask.class);
    public static final String CUSTOM_SETTINGS_PREFIX = "tests.es.";

    public static final String DATA_DIR_SETTING = "run.datadir";

    private Boolean debug = false;

    @Option(
        option = "debug-jvm",
        description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch."
    )
    public void setDebug(boolean enabled) {
        this.debug = enabled;
    }

    @Input
    public Boolean getDebug() {
        return debug;
    }

    @Override
    public void beforeStart() {
        int debugPort = 5005;
        int httpPort = 9200;
        int transportPort = 9300;
        Map<String, String> additionalSettings = System.getProperties().entrySet().stream()
            .filter(entry -> entry.getKey().toString().startsWith(CUSTOM_SETTINGS_PREFIX))
            .collect(Collectors.toMap(
                entry -> entry.getKey().toString().substring(CUSTOM_SETTINGS_PREFIX.length()),
                entry -> entry.getValue().toString()
            ));
        Path baseDataPath = null;
        String dataPathSetting = System.getProperty(DATA_DIR_SETTING);
        if (dataPathSetting != null) {
            baseDataPath = Paths.get(dataPathSetting).toAbsolutePath();
        }
        for (ElasticsearchCluster cluster : getClusters()) {
            cluster.getFirstNode().setHttpPort(String.valueOf(httpPort));
            httpPort++;
            cluster.getFirstNode().setTransportPort(String.valueOf(transportPort));
            transportPort++;
            Path clusterDataPath = null;
            if (baseDataPath != null) {
                clusterDataPath = baseDataPath.resolve(cluster.getName());
            }
            for (ElasticsearchNode node : cluster.getNodes()) {
                additionalSettings.forEach(node::setting);
                if (clusterDataPath != null) {
                    node.setDataPath(clusterDataPath.resolve(node.getName()));
                }
                if (debug) {
                    logger.lifecycle(
                        "Running elasticsearch in debug mode, {} suspending until connected on debugPort {}",
                        node, debugPort
                    );
                    node.jvmArgs("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + debugPort);
                    debugPort += 1;
                }
            }
        }
    }

    @TaskAction
    public void runAndWait() throws IOException {
        List<BufferedReader> toRead = new ArrayList<>();
        try {
            for (ElasticsearchCluster cluster : getClusters()) {
                for (ElasticsearchNode node : cluster.getNodes()) {
                    BufferedReader reader = Files.newBufferedReader(node.getEsStdoutFile());
                    toRead.add(reader);
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

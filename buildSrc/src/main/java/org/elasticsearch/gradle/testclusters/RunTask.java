package org.elasticsearch.gradle.testclusters;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

public class RunTask extends DefaultTestClustersTask {

    private static final Logger logger = Logging.getLogger(RunTask.class);

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
        int debugPort = 8000;
        int httpPort = 9200;
        int transportPort = 9300;
        for (ElasticsearchCluster cluster : getClusters()) {
            cluster.getFirstNode().setHttpPort(String.valueOf(httpPort));
            httpPort++;
            cluster.getFirstNode().setTransportPort(String.valueOf(transportPort));
            transportPort++;
            for (ElasticsearchNode node : cluster.getNodes()) {
                if (debug) {
                    logger.lifecycle(
                        "Running elasticsearch in debug mode, {} suspending until connected on debugPort {}",
                        node, debugPort
                    );
                    node.jvmArgs("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + debugPort);
                    debugPort += 1;
                }
            }
        }
    }

    @TaskAction
    public void runAndWait() throws IOException {
        Set<BufferedReader> toRead = new HashSet<>();
        for (ElasticsearchCluster cluster : getClusters()) {
            for (ElasticsearchNode node : cluster.getNodes()) {
                toRead.add(Files.newBufferedReader(node.getEsStdoutFile()));
            }
        }
        while (Thread.currentThread().isInterrupted() == false) {
            for (BufferedReader bufferedReader : toRead) {
                if (bufferedReader.ready()) {
                    logger.lifecycle(bufferedReader.readLine());
                }
            }
        }
    }
}

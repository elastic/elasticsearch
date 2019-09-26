package org.elasticsearch.gradle.testclusters;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

public class RunTask extends DefaultTask {

    private Boolean debug = false;

    private ElasticsearchCluster cluster;

    @Option(
        option = "debug-jvm",
        description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch."
    )
    public void setDebug(boolean enabled) {
        this.debug = debug;
    }

    @Input
    public Boolean getDebug() {
        return debug;
    }

    @Input
    public ElasticsearchCluster getCluster() {
        return cluster;
    }

    public void setCluster(ElasticsearchCluster cluster) {
        TestClustersAware.prepareClusterForUse(getProject(), this, cluster);
        this.cluster = cluster;
    }

    @TaskAction
    public void runAndWait() {
        cluster.freeze();
        cluster.run(debug);
        while (Thread.currentThread().isInterrupted() == false) {
            try {
                Thread.sleep(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

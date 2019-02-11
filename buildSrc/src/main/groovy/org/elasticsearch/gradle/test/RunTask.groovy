package org.elasticsearch.gradle.test

import org.gradle.api.DefaultTask
import org.gradle.api.Task
import org.gradle.api.tasks.options.Option
import org.gradle.util.ConfigureUtil

public class RunTask extends DefaultTask {

    ClusterConfiguration clusterConfig

    public RunTask() {
        description = "Runs elasticsearch with '${project.path}'"
        group = 'Verification'
        clusterConfig = new ClusterConfiguration(project)
        clusterConfig.httpPort = 9200
        clusterConfig.transportPort = 9300
        clusterConfig.daemonize = false
        clusterConfig.distribution = 'default'
        project.afterEvaluate {
            ClusterFormationTasks.setup(project, name, this, clusterConfig)
        }
    }

    @Option(
        option = "debug-jvm",
        description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch."
    )
    public void setDebug(boolean enabled) {
        clusterConfig.debug = enabled;
    }

    /** Configure the cluster that will be run. */
    @Override
    public Task configure(Closure closure) {
        ConfigureUtil.configure(closure, clusterConfig)
        return this
    }
}

package org.elasticsearch.gradle.test

import org.gradle.api.DefaultTask
import org.gradle.api.Task
import org.gradle.api.internal.tasks.options.Option
import org.gradle.util.ConfigureUtil

public class RunTask extends DefaultTask {

    ClusterConfiguration clusterConfig = new ClusterConfiguration(httpPort: 9200, transportPort: 9300, daemonize: false)

    public RunTask() {
        description = "Runs elasticsearch with '${project.path}'"
        group = 'Verification'
        project.afterEvaluate {
            ClusterFormationTasks.setup(project, this, clusterConfig)
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
